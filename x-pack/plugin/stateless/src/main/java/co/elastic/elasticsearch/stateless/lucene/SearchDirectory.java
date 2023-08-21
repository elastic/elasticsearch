/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.lucene;

import co.elastic.elasticsearch.stateless.commits.BlobLocation;
import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.SingleInstanceLockFactory;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.ByteSizeDirectory;
import org.elasticsearch.index.store.ImmutableDirectoryException;
import org.elasticsearch.index.store.Store;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongFunction;
import java.util.stream.Stream;

public class SearchDirectory extends ByteSizeDirectory {

    private final ShardId shardId;

    private final SharedBlobCacheService<FileCacheKey> cacheService;

    private final SetOnce<LongFunction<BlobContainer>> blobContainer = new SetOnce<>();
    private final AtomicReference<String> corruptionMarker = new AtomicReference<>();
    private final LockFactory lockFactory = new SingleInstanceLockFactory();

    private final AtomicReference<Thread> updatingCommitThread = Assertions.ENABLED ? new AtomicReference<>() : null;// only used in asserts
    private final AtomicReference<StatelessCompoundCommit> currentCommit = new AtomicReference<>(null);
    private volatile Map<String, BlobLocation> currentMetadata = Map.of();
    private volatile long currentDataSetSizeInBytes = 0L;

    public SearchDirectory(SharedBlobCacheService<FileCacheKey> cacheService, ShardId shardId) {
        super(EmptyDirectory.INSTANCE);
        this.cacheService = cacheService;
        this.shardId = shardId;
    }

    @Override
    public Lock obtainLock(String name) throws IOException {
        return lockFactory.obtainLock(this, name);
    }

    public void setBlobContainer(LongFunction<BlobContainer> blobContainer) {
        this.blobContainer.set(blobContainer);
    }

    public boolean containsFile(String name) {
        if (currentMetadata.isEmpty()) {
            try {
                for (String s : super.listAll()) {
                    if (name.equals(s)) {
                        return true;
                    }
                }
            } catch (IOException e) {
                throw new AssertionError("never throws");
            }
        }
        return currentMetadata.containsKey(name);
    }

    public boolean isMarkedAsCorrupted() {
        return corruptionMarker.get() != null;
    }

    /**
     * Moves the directory to a new commit by setting the newly valid map of files and their metadata.
     *
     * @param newCommit map of file name to store metadata
     */
    public void updateCommit(StatelessCompoundCommit newCommit) {
        assert blobContainer.get() != null : shardId + " must have the blob container set before any commit update";
        assert assertCompareAndSetUpdatingCommitThread(null, Thread.currentThread());
        try {
            // TODO: we only accumulate files as we see new commits, we need to start cleaning this map once we add deletes
            final Map<String, BlobLocation> updated = new HashMap<>(currentMetadata);
            long commitSize = 0L;
            for (var entry : newCommit.commitFiles().entrySet()) {
                updated.put(entry.getKey(), entry.getValue());
                commitSize += entry.getValue().fileLength();
            }
            currentMetadata = Map.copyOf(updated);
            currentDataSetSizeInBytes = commitSize;
            // TODO: Commits may not arrive in order. However, the maximum commit we have received is the commit of this directory since the
            // TODO: files always accumulate
            currentCommit.getAndAccumulate(newCommit, (current, contender) -> {
                if (current == null) {
                    return contender;
                } else if (current.generation() > contender.generation()) {
                    return current;
                } else {
                    return contender;
                }
            });
        } finally {
            assert assertCompareAndSetUpdatingCommitThread(Thread.currentThread(), null);
        }
    }

    /**
     * For test usage only.
     */
    void setMetadata(Map<String, BlobLocation> blobLocations) {
        assert currentMetadata.isEmpty();
        currentMetadata = Map.copyOf(blobLocations);
    }

    // TODO this method works because we never prune old commits files
    public OptionalLong getPrimaryTerm(String segmentsFileName) throws FileNotFoundException {
        final BlobLocation location = currentMetadata.get(segmentsFileName);
        if (location != null) {
            return OptionalLong.of(location.primaryTerm());
        }
        if (segmentsFileName.equals(EmptyDirectory.INSTANCE.getSegmentsFileName())) {
            return OptionalLong.empty();
        }
        var exception = new FileNotFoundException(segmentsFileName);
        assert false : exception;
        throw exception;
    }

    public Optional<String> getCurrentMetadataNodeEphemeralId() {
        StatelessCompoundCommit compoundCommit = currentCommit.get();
        return compoundCommit != null ? Optional.of(compoundCommit.nodeEphemeralId()) : Optional.empty();
    }

    public long getTranslogRecoveryStartFile() {
        StatelessCompoundCommit compoundCommit = currentCommit.get();
        return compoundCommit != null ? compoundCommit.translogRecoveryStartFile() : 0;
    }

    private boolean assertCompareAndSetUpdatingCommitThread(Thread current, Thread updated) {
        final Thread witness = updatingCommitThread.compareAndExchange(current, updated);
        assert witness == current
            : "Unable to set updating commit thread to ["
                + updated
                + "]: expected thread ["
                + current
                + "] to be the updating commit thread, but thread "
                + witness
                + " is already updating the commit of "
                + shardId;
        return true;
    }

    @Override
    public String[] listAll() throws IOException {
        final var current = currentMetadata;
        final String[] list;
        if (current.isEmpty()) {
            list = super.listAll();
        } else {
            list = current.keySet().stream().sorted(String::compareTo).toArray(String[]::new);
        }
        if (isMarkedAsCorrupted()) {
            return Stream.concat(Stream.of(list), Stream.of(corruptionMarker.get())).sorted(String::compareTo).toArray(String[]::new);
        }
        return list;
    }

    @Override
    public void deleteFile(String name) {
        throw unsupportedException();
    }

    @Override
    public long fileLength(String name) throws IOException {
        final var current = currentMetadata;
        if (current.isEmpty()) {
            return super.fileLength(name);
        }
        BlobLocation location = current.get(name);
        if (location == null) {
            throw new FileNotFoundException(name);
        }
        return location.fileLength();
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) {
        if (name.startsWith(Store.CORRUPTED_MARKER_NAME_PREFIX)) {
            if (corruptionMarker.compareAndSet(null, name)) {
                throw new ImmutableDirectoryException(name);
            }
        }
        throw unsupportedException();
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) {
        throw unsupportedException();
    }

    @Override
    public void sync(Collection<String> names) {
        if (isMarkedAsCorrupted()) {
            return; // allows to sync after the corruption marker has been written
        }
        throw unsupportedException();
    }

    @Override
    public void syncMetaData() {
        throw unsupportedException();
    }

    @Override
    public void rename(String source, String dest) {
        throw unsupportedException();
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        final var current = currentMetadata;
        if (current.isEmpty()) {
            return super.openInput(name, context);
        }
        final BlobLocation location = current.get(name);
        if (location == null) {
            throw new FileNotFoundException(name);
        }
        return new SearchIndexInput(
            name,
            cacheService.getCacheFile(new FileCacheKey(shardId, location.blobName()), location.blobLength()),
            context,
            blobContainer.get().apply(location.primaryTerm()),
            cacheService,
            location.fileLength(),
            location.offset()
        );
    }

    @Override
    public void close() throws IOException {
        // do not close EmptyDirectory
    }

    @Override
    public Set<String> getPendingDeletions() {
        throw unsupportedException();
    }

    public BlobContainer getBlobContainer(long primaryTerm) {
        return blobContainer.get().apply(primaryTerm);
    }

    public ShardId getShardId() {
        return shardId;
    }

    @Override
    public long estimateSizeInBytes() {
        // size is 0 bytes since search directory has no files on disk
        return 0L;
    }

    @Override
    public long estimateDataSetSizeInBytes() {
        // data set size is equal to the size of the last commit fetched from the object store
        return currentDataSetSizeInBytes;
    }

    private static UnsupportedOperationException unsupportedException() {
        assert false : "this operation is not supported and should have not be called";
        return new UnsupportedOperationException("stateless directory does not support this operation");
    }

    public static SearchDirectory unwrapDirectory(final Directory directory) {
        Directory dir = directory;
        while (dir != null) {
            if (dir instanceof SearchDirectory searchDirectory) {
                return searchDirectory;
            } else if (dir instanceof IndexDirectory indexDirectory) {
                return indexDirectory.getSearchDirectory();
            } else if (dir instanceof FilterDirectory) {
                dir = ((FilterDirectory) dir).getDelegate();
            } else {
                dir = null;
            }
        }
        var e = new IllegalStateException(directory.getClass() + " cannot be unwrapped as " + SearchDirectory.class);
        assert false : e;
        throw e;
    }
}
