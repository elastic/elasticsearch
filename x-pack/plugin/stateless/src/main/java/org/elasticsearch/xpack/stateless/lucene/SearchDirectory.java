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

import co.elastic.elasticsearch.stateless.cache.StatelessSharedBlobCacheService;
import co.elastic.elasticsearch.stateless.commits.BlobLocation;
import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.SingleInstanceLockFactory;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.blobcache.BlobCacheUtils;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.blobcache.shared.SharedBytes;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.ByteSizeDirectory;
import org.elasticsearch.index.store.ImmutableDirectoryException;
import org.elasticsearch.index.store.Store;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongFunction;
import java.util.stream.Stream;

import static org.elasticsearch.blobcache.shared.SharedBytes.MAX_BYTES_PER_WRITE;

public class SearchDirectory extends ByteSizeDirectory {

    private static final Logger logger = LogManager.getLogger(SearchDirectory.class);

    private final ShardId shardId;

    private final StatelessSharedBlobCacheService cacheService;

    private final SetOnce<LongFunction<BlobContainer>> blobContainer = new SetOnce<>();
    private final AtomicReference<String> corruptionMarker = new AtomicReference<>();
    private final LockFactory lockFactory = new SingleInstanceLockFactory();

    private final AtomicReference<Thread> updatingCommitThread = Assertions.ENABLED ? new AtomicReference<>() : null;// only used in asserts
    private final AtomicReference<StatelessCompoundCommit> currentCommit = new AtomicReference<>(null);
    private volatile Map<String, BlobLocation> currentMetadata = Map.of();
    private volatile long currentDataSetSizeInBytes = 0L;

    public SearchDirectory(StatelessSharedBlobCacheService cacheService, ShardId shardId) {
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
     * @return true if this update advanced the commit tracked by this directory
     */
    public boolean updateCommit(StatelessCompoundCommit newCommit) {
        assert blobContainer.get() != null : shardId + " must have the blob container set before any commit update";
        assert assertCompareAndSetUpdatingCommitThread(null, Thread.currentThread());
        try {
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
            return currentCommit.accumulateAndGet(newCommit, (current, contender) -> {
                if (current == null) {
                    return contender;
                } else if (current.generation() > contender.generation()) {
                    return current;
                } else {
                    return contender;
                }
            }).generation() == newCommit.generation();
        } finally {
            assert assertCompareAndSetUpdatingCommitThread(Thread.currentThread(), null);
        }
    }

    /**
     * Removes superfluous files
     * @param filesToRetain the files to retain
     */
    public void retainFiles(Set<String> filesToRetain) {
        if (filesToRetain.containsAll(currentMetadata.keySet()) == false) {
            assert assertCompareAndSetUpdatingCommitThread(null, Thread.currentThread());
            try {
                final Map<String, BlobLocation> updated = new HashMap<>(currentMetadata);
                updated.keySet().retainAll(filesToRetain);
                assert updated.keySet().containsAll(filesToRetain)
                    : "missing files [" + Sets.difference(filesToRetain, updated.keySet()) + "]";
                currentMetadata = Map.copyOf(updated);
            } finally {
                assert assertCompareAndSetUpdatingCommitThread(Thread.currentThread(), null);
            }
        }
    }

    /**
     * On indexing shards we cannot accurately calculate the files to retain such that it does not contain files not
     * in the directory. Hence we omit the assertion for indexing.
     * @param filesToRetain the files to retain
     */
    public void retainFilesIndexing(Set<String> filesToRetain) {
        if (filesToRetain.containsAll(currentMetadata.keySet()) == false) {
            assert assertCompareAndSetUpdatingCommitThread(null, Thread.currentThread());
            try {
                final Map<String, BlobLocation> updated = new HashMap<>(currentMetadata);
                updated.keySet().retainAll(filesToRetain);
                currentMetadata = Map.copyOf(updated);
            } finally {
                assert assertCompareAndSetUpdatingCommitThread(Thread.currentThread(), null);
            }
        }
    }

    /**
     * For test usage only.
     */
    void setMetadata(Map<String, BlobLocation> blobLocations) {
        assert currentMetadata.isEmpty();
        currentMetadata = Map.copyOf(blobLocations);
    }

    /**
     * For test usage only.
     */
    BlobLocation getBlobLocation(String fileName) {
        return currentMetadata.get(fileName);
    }

    /**
     * For test usage only.
     */
    StatelessSharedBlobCacheService getCacheService() {
        return cacheService;
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
            cacheService.getCacheFile(
                new FileCacheKey(shardId, location.primaryTerm(), location.blobName()),
                // this length is only used to assert that the cache file does not try to read data beyond the file boundary within the blob
                // since we overload computeCacheFileRegionSize in StatelessSharedBlobCacheService to to fully utilize each region
                location.offset() + location.fileLength()
            ),
            context,
            blobContainer.get().apply(location.primaryTerm()),
            (position, length) -> getCacheRange(location, position, length),
            location.fileLength(),
            location.offset()
        );
    }

    /**
     * Computes the range of bytes to fetch and to write in cache.
     *
     * @param blobLocation the location of the file in the blob
     * @param position the absolute position in the blob
     * @param length the length of bytes to be read from the position
     *
     * @return the range of bytes to fetch and to write in cache.
     */
    protected ByteRange getCacheRange(BlobLocation blobLocation, long position, int length) {
        // TODO request a smaller range if the blob is not yet uploaded

        // Blob is uploaded, we can request complete regions to serve the read operation
        return BlobCacheUtils.computeRange(cacheService.getRangeSize(), position, length);
    }

    @Override
    public void close() throws IOException {
        // do not close EmptyDirectory
        if (isMarkedAsCorrupted()) {
            forceEvict();
        }
    }

    @Override
    public Set<String> getPendingDeletions() {
        throw unsupportedException();
    }

    public void forceEvict() {
        final int n = cacheService.forceEvict(fileCacheKey -> shardId.equals(fileCacheKey.shardId()));
        logger.warn("[{}] force evicted [{}] blob cache entries for ShardId {}", this, n, shardId);
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

    public void downloadCommit(StatelessCompoundCommit commit, ActionListener<Void> listener) {
        try (RefCountingListener refCountingListener = new RefCountingListener(listener)) {
            final String latestCompoundCommitLocation = StatelessCompoundCommit.blobNameFromGeneration(commit.generation());
            commit.commitFiles().forEach((file, blobLocation) -> {
                if (blobLocation.blobName().equals(latestCompoundCommitLocation) == false) {
                    // only pre-fetch files that are part of the latest commit generation
                    return;
                }
                FileCacheKey key = new FileCacheKey(shardId, blobLocation.primaryTerm(), blobLocation.blobName());
                final var blobLength = blobLocation.blobLength();
                final var container = blobContainer.get().apply(blobLocation.primaryTerm());
                cacheService.maybeFetchFullEntry(key, blobLength, (channel, channelPos, relativePos, length, progressUpdater) -> {
                    final ByteRange rangeToWrite = BlobCacheUtils.computeRange(
                        cacheService.getRangeSize(),
                        relativePos,
                        length,
                        blobLength
                    );
                    final long streamStartPosition = rangeToWrite.start() + relativePos;
                    try (InputStream in = container.readBlob(OperationPurpose.INDICES, key.fileName(), streamStartPosition, length)) {
                        // assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.GENERIC);
                        logger.trace(
                            "{}: writing channel {} pos {} length {} (details: {})",
                            key.fileName(),
                            channelPos,
                            relativePos,
                            length,
                            key
                        );
                        SharedBytes.copyToCacheFileAligned(
                            channel,
                            in,
                            channelPos,
                            relativePos,
                            length,
                            progressUpdater,
                            writeBuffer.get().clear()
                        );
                    }
                }, refCountingListener.acquire());
            });
        }
    }

    private static final ThreadLocal<ByteBuffer> writeBuffer = ThreadLocal.withInitial(
        () -> ByteBuffer.allocateDirect(MAX_BYTES_PER_WRITE)
    );
}
