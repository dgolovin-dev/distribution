package storage

import (
	"context"
	"fmt"
	"path"
	"time"

	"github.com/docker/distribution"
	"github.com/docker/distribution/reference"
	"github.com/docker/distribution/registry/storage/driver"
	"github.com/opencontainers/go-digest"
)

func emit(format string, a ...interface{}) {
	fmt.Printf(format+"\n", a...)
}

// GCOpts contains options for garbage collector
type GCOpts struct {
	DryRun              bool
	RemoveUntagged      bool
	RemoveRepositories  bool
	ModificationTimeout float64
}

// ManifestDel contains manifest structure which will be deleted
type ManifestDel struct {
	Name   string
	Digest digest.Digest
	Tags   []string
}

// MarkAndSweep performs a mark and sweep of registry data
func MarkAndSweep(ctx context.Context, storageDriver driver.StorageDriver, registry distribution.Namespace, opts GCOpts) error {

	deleteBlobSet, deleteManifestArr, deleteRepositoryArr, err := mark(ctx, storageDriver, registry, opts.RemoveUntagged, opts.ModificationTimeout)
	if err != nil {
		return fmt.Errorf("failed to mark blobs and manifests: %v", err)
	}

	emit("\n%d blobs, %d manifests, %d repositories eligible for deletion", len(deleteBlobSet), len(deleteManifestArr), len(deleteRepositoryArr))

	vacuum := NewVacuum(ctx, storageDriver)

	if opts.RemoveUntagged {
		err = sweepManifests(vacuum, deleteManifestArr, opts.DryRun)
		if err != nil {
			return fmt.Errorf("failed to sweep manifests: %v", err)
		}
	}

	if opts.RemoveRepositories {
		err = sweepRepositories(vacuum, deleteRepositoryArr, opts.DryRun)
		if err != nil {
			return fmt.Errorf("failed to sweep repositories: %v", err)
		}
	}

	err = sweepBlobs(vacuum, deleteBlobSet, opts.DryRun)
	if err != nil {
		return fmt.Errorf("failed to sweep blobs: %v", err)
	}

	return err
}

func sweepRepositories(vacuum Vacuum, deleteRepositoryArr []string, dryRun bool) error {
	emit("sweep repositories")
	for _, name := range deleteRepositoryArr {
		emit("repository eligible for deletion: %s", name)
		if dryRun {
			continue
		}
		err := vacuum.RemoveRepository(name)
		if err != nil {
			return fmt.Errorf("failed to delete repository %s: %v", name, err)
		}
	}
	return nil
}

func sweepBlobs(vacuum Vacuum, deleteBlobSet map[digest.Digest]struct{}, dryRun bool) error {
	emit("sweep blobs")
	for dgst := range deleteBlobSet {
		emit("blob eligible for deletion: %s", dgst)
		if dryRun {
			continue
		}
		err := vacuum.RemoveBlob(string(dgst))
		if err != nil {
			return fmt.Errorf("failed to delete blob %s: %v", dgst, err)
		}
	}
	return nil
}

func sweepManifests(vacuum Vacuum, deleteManifestArr []ManifestDel, dryRun bool) error {
	emit("sweep manifests")
	for _, obj := range deleteManifestArr {
		emit("manifest eligible for deletion: %s %s", obj.Name, obj.Digest)
		if dryRun {
			continue
		}
		err := vacuum.RemoveManifest(obj.Name, obj.Digest, obj.Tags)
		if err != nil {
			return fmt.Errorf("failed to delete manifest %s: %v", obj.Digest, err)
		}
	}
	return nil
}

func mark(
	ctx context.Context,
	storageDriver driver.StorageDriver,
	registry distribution.Namespace,
	removeUntagged bool,
	modificationTimeout float64,
) (map[digest.Digest]struct{}, []ManifestDel, []string, error) {
	repositoryEnumerator, ok := registry.(distribution.RepositoryEnumerator)
	if !ok {
		return nil, nil, nil, fmt.Errorf("unable to convert Namespace to RepositoryEnumerator")
	}

	// mark
	markBlobSet := make(map[digest.Digest]struct{})
	deleteManifestArr := make([]ManifestDel, 0)
	deleteRepositoryArr := make([]string, 0)

	err := repositoryEnumerator.Enumerate(ctx, func(repoName string) error {
		emit(repoName)

		repoHasManifest := false
		var err error
		named, err := reference.WithName(repoName)
		if err != nil {
			return fmt.Errorf("failed to parse repo name %s: %v", repoName, err)
		}
		repository, err := registry.Repository(ctx, named)
		if err != nil {
			return fmt.Errorf("failed to construct repository: %v", err)
		}

		manifestService, err := repository.Manifests(ctx)
		if err != nil {
			return fmt.Errorf("failed to construct manifest service: %v", err)
		}

		manifestEnumerator, ok := manifestService.(distribution.ManifestEnumerator)
		if !ok {
			return fmt.Errorf("unable to convert ManifestService into ManifestEnumerator")
		}

		err = manifestEnumerator.Enumerate(ctx, func(dgst digest.Digest) error {
			if removeUntagged {
				// fetch all tags where this manifest is the latest one
				tags, err := repository.Tags(ctx).Lookup(ctx, distribution.Descriptor{Digest: dgst})
				if err != nil {
					return fmt.Errorf("failed to retrieve tags for digest %v: %v", dgst, err)
				}
				if len(tags) == 0 {
					emit("manifest eligible for deletion: %s", dgst)
					// fetch all tags from repository
					// all of these tags could contain manifest in history
					// which means that we need check (and delete) those references when deleting manifest
					allTags, err := repository.Tags(ctx).All(ctx)
					if err != nil {
						return fmt.Errorf("failed to retrieve tags %v", err)
					}

					// check modification
					modifiedEarlier, err := manifestModifiedEarlierThan(ctx, storageDriver, repoName, dgst, modificationTimeout)
					if err != nil {
						return fmt.Errorf("failed to get modification time %v", err)
					}

					if modifiedEarlier {
						deleteManifestArr = append(deleteManifestArr, ManifestDel{Name: repoName, Digest: dgst, Tags: allTags})
						return nil
					}
				}
			}

			// Mark the manifest's blob
			emit("%s: marking manifest %s ", repoName, dgst)
			markBlobSet[dgst] = struct{}{}

			manifest, err := manifestService.Get(ctx, dgst)
			if err != nil {
				return fmt.Errorf("failed to retrieve manifest for digest %v: %v", dgst, err)
			}

			descriptors := manifest.References()
			for _, descriptor := range descriptors {
				markBlobSet[descriptor.Digest] = struct{}{}
				emit("%s: marking blob %s", repoName, descriptor.Digest)
			}

			repoHasManifest = true

			return nil
		})

		// In certain situations such as unfinished uploads, deleting all
		// tags in S3 or removing the _manifests folder manually, this
		// error may be of type PathNotFound.
		//
		// In these cases we can continue marking other manifests safely.
		if _, ok := err.(driver.PathNotFoundError); ok {
			return nil
		}

		if !repoHasManifest {
			modifiedEarlier, err := repositoryModifiedEarlierThan(ctx, storageDriver, repoName, modificationTimeout)
			if err != nil {
				return fmt.Errorf("failed to get modification time %v", err)
			}
			if modifiedEarlier {
				deleteRepositoryArr = append(deleteRepositoryArr, repoName)
			}
		}

		return err
	})

	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to mark: %v", err)
	}

	blobService := registry.Blobs()
	deleteBlobSet := make(map[digest.Digest]struct{})
	err = blobService.Enumerate(ctx, func(dgst digest.Digest) error {
		// check if digest is in markSet. If not, delete it!
		if _, ok := markBlobSet[dgst]; !ok {
			// check modification
			modifiedEarlier, err := blobModifiedEarlierThan(ctx, storageDriver, dgst, modificationTimeout)
			if err != nil {
				return fmt.Errorf("failed to get modification time: %v", err)
			}

			if modifiedEarlier {
				deleteBlobSet[dgst] = struct{}{}
			}
		}
		return nil
	})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("error enumerating blobs: %v", err)
	}

	return deleteBlobSet, deleteManifestArr, deleteRepositoryArr, nil
}

// RepositoryModifiedEarlierThan returns repository was modified more than timeout seconds ago
func repositoryModifiedEarlierThan(
	ctx context.Context,
	strorageDriver driver.StorageDriver,
	repoName string,
	timeout float64,
) (bool, error) {
	rootForRepository, err := pathFor(repositoriesRootPathSpec{})
	if err != nil {
		return false, err
	}
	repoDir := path.Join(rootForRepository, repoName)
	return pathModifiedEarlierThan(ctx, strorageDriver, repoDir, timeout)
}

// manifestModifiedEarlierThan returns manifest was modified more than timeout seconds ago
func manifestModifiedEarlierThan(
	ctx context.Context,
	strorageDriver driver.StorageDriver,
	repoName string,
	revision digest.Digest,
	timeout float64,
) (bool, error) {
	return pathSpecModifiedEarlierThan(ctx, strorageDriver, manifestRevisionPathSpec{name: repoName, revision: revision}, timeout)
}

// blobModifiedEarlierThan returns blob was modified more than timeout seconds ago
func blobModifiedEarlierThan(
	ctx context.Context,
	strorageDriver driver.StorageDriver,
	digest digest.Digest,
	timeout float64,
) (bool, error) {
	return pathSpecModifiedEarlierThan(ctx, strorageDriver, blobPathSpec{digest: digest}, timeout)
}

// pathSpecModifiedEarlierThan returns pathSpec file was modified more than timeout seconds ago
func pathSpecModifiedEarlierThan(
	ctx context.Context,
	strorageDriver driver.StorageDriver,
	spec pathSpec,
	timeout float64,
) (bool, error) {
	path, err := pathFor(spec)
	if err != nil {
		return false, err
	}
	return pathModifiedEarlierThan(ctx, strorageDriver, path, timeout)
}

// pathModifiedEarlierThan returns path was modified more than timeout seconds ago
func pathModifiedEarlierThan(ctx context.Context, strorageDriver driver.StorageDriver, path string, timeout float64) (bool, error) {
	if timeout <= 0 {
		return true, nil
	}
	stat, err := strorageDriver.Stat(ctx, path)
	if err != nil {
		return false, err
	}
	mtime := stat.ModTime()
	since := time.Since(mtime)
	seconds := since.Seconds()
	return seconds > timeout, nil
}
