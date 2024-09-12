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
import co.elastic.elasticsearch.stateless.cache.reader.CacheBlobReader;
import co.elastic.elasticsearch.stateless.commits.BlobLocation;

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
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.ByteSizeDirectory;
import org.elasticsearch.index.store.ImmutableDirectoryException;
import org.elasticsearch.index.store.Store;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongFunction;
import java.util.stream.Stream;

/**
 * Lucene directory implementation that uses a block based cache for reading files stored in an object store
 */
public abstract class BlobStoreCacheDirectory extends ByteSizeDirectory {

    private static final Logger logger = LogManager.getLogger(BlobStoreCacheDirectory.class);

    protected final LongAdder totalBytesReadFromObjectStore = new LongAdder();
    protected final LongAdder totalBytesWarmedFromObjectStore = new LongAdder();

    protected final ShardId shardId;
    protected final StatelessSharedBlobCacheService cacheService;
    protected final SetOnce<LongFunction<BlobContainer>> blobContainer = new SetOnce<>();

    private final AtomicReference<String> corruptionMarker = new AtomicReference<>();
    private final LockFactory lockFactory = new SingleInstanceLockFactory();

    private final AtomicReference<Thread> updatingCommitThread = Assertions.ENABLED ? new AtomicReference<>() : null;// only used in asserts
    protected volatile Map<String, BlobLocation> currentMetadata = Map.of();
    protected volatile long currentDataSetSizeInBytes = 0L;

    BlobStoreCacheDirectory(StatelessSharedBlobCacheService cacheService, ShardId shardId) {
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
     * For test usage only.
     */
    BlobLocation getBlobLocation(String fileName) {
        return currentMetadata.get(fileName);
    }

    StatelessSharedBlobCacheService getCacheService() {
        return cacheService;
    }

    public long totalBytesReadFromObjectStore() {
        return totalBytesReadFromObjectStore.sum();
    }

    public long totalBytesWarmedFromObjectStore() {
        return totalBytesWarmedFromObjectStore.sum();
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

    protected boolean assertCompareAndSetUpdatingCommitThread(Thread current, Thread updated) {
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
        return doOpenInput(name, context, location);
    }

    protected IndexInput doOpenInput(String name, IOContext context, BlobLocation blobLocation) {
        return doOpenInput(name, context, blobLocation, null);
    }

    /**
     * Opens an {@link IndexInput} for reading an existing file.
     *
     * @param name the name of an existing file
     * @param context the IO context
     * @param blobLocation the {@link BlobLocation} of the file
     * @param releasable a {@link Releasable} to be released when the {@link IndexInput} is closed
     * @return an {@link IndexInput}
     */
    protected final IndexInput doOpenInput(String name, IOContext context, BlobLocation blobLocation, @Nullable Releasable releasable) {
        return new BlobCacheIndexInput(
            name,
            cacheService.getCacheFile(
                new FileCacheKey(shardId, blobLocation.primaryTerm(), blobLocation.blobName()),
                // this length is a lower bound on the length of the blob, used to assert that the cache file does not try to read
                // data beyond the file boundary within the blob since we overload computeCacheFileRegionSize in
                // StatelessSharedBlobCacheService to fully utilize each region
                // it is also used for bounding the reads we do against indexing shard to ensure that we never read beyond the
                // blob length (with padding added).
                blobLocation.offset() + blobLocation.fileLength()
            ),
            context,
            getCacheBlobReader(blobLocation),
            releasable,
            blobLocation.fileLength(),
            blobLocation.offset()
        );
    }

    protected abstract CacheBlobReader getCacheBlobReader(BlobLocation blobLocation);

    public abstract CacheBlobReader getCacheBlobReaderForWarming(BlobLocation blobLocation);

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

    public static BlobStoreCacheDirectory unwrapDirectory(final Directory directory) {
        Directory dir = directory;
        while (dir != null) {
            if (dir instanceof BlobStoreCacheDirectory blobStoreCacheDirectory) {
                return blobStoreCacheDirectory;
            } else if (dir instanceof IndexDirectory indexDirectory) {
                return indexDirectory.getBlobStoreCacheDirectory();
            } else if (dir instanceof FilterDirectory) {
                dir = ((FilterDirectory) dir).getDelegate();
            } else {
                dir = null;
            }
        }
        var e = new IllegalStateException(directory.getClass() + " cannot be unwrapped as " + BlobStoreCacheDirectory.class);
        assert false : e;
        throw e;
    }
}
