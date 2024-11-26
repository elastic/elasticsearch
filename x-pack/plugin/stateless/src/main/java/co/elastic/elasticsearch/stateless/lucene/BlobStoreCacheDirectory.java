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
import co.elastic.elasticsearch.stateless.cache.reader.CacheFileReader;
import co.elastic.elasticsearch.stateless.commits.BlobFileRanges;
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
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
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

    protected final LongAdder totalBytesReadFromObjectStore;
    protected final LongAdder totalBytesWarmedFromObjectStore;

    protected final ShardId shardId;
    protected final StatelessSharedBlobCacheService cacheService;
    protected final AtomicReference<LongFunction<BlobContainer>> blobContainer = new AtomicReference<>();

    private final AtomicReference<String> corruptionMarker = new AtomicReference<>();
    private final LockFactory lockFactory = new SingleInstanceLockFactory();

    private final AtomicReference<Thread> updatingCommitThread = Assertions.ENABLED ? new AtomicReference<>() : null;// only used in asserts
    protected volatile Map<String, BlobFileRanges> currentMetadata = Map.of();
    protected volatile long currentDataSetSizeInBytes = 0L;

    BlobStoreCacheDirectory(StatelessSharedBlobCacheService cacheService, ShardId shardId) {
        this(cacheService, shardId, new LongAdder(), new LongAdder());
    }

    protected BlobStoreCacheDirectory(
        StatelessSharedBlobCacheService cacheService,
        ShardId shardId,
        LongAdder totalBytesRead,
        LongAdder totalBytesWarmed
    ) {
        super(EmptyDirectory.INSTANCE);
        this.cacheService = cacheService;
        this.shardId = shardId;
        this.totalBytesReadFromObjectStore = totalBytesRead;
        this.totalBytesWarmedFromObjectStore = totalBytesWarmed;
    }

    @Override
    public Lock obtainLock(String name) throws IOException {
        return lockFactory.obtainLock(this, name);
    }

    public final void setBlobContainer(LongFunction<BlobContainer> blobContainer) {
        this.blobContainer.compareAndSet(null, blobContainer); // set once
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
    @Nullable
    BlobLocation getBlobLocation(String fileName) {
        var blobFileRanges = currentMetadata.get(fileName);
        return blobFileRanges != null ? blobFileRanges.blobLocation() : null;
    }

    /**
     * Returns position of the file in the surrounding blob
     */
    public long getPosition(String fileName, long pos, int length) {
        var blobFileRanges = currentMetadata.get(fileName);
        return blobFileRanges != null ? blobFileRanges.getPosition(pos, length) : pos;
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
        final var blobFileRanges = currentMetadata.get(segmentsFileName);
        if (blobFileRanges != null) {
            return OptionalLong.of(blobFileRanges.primaryTerm());
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
        var blobFileRanges = current.get(name);
        if (blobFileRanges == null) {
            throw new FileNotFoundException(name);
        }
        return blobFileRanges.fileLength();
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
        final var blobFileRanges = current.get(name);
        if (blobFileRanges == null) {
            throw new FileNotFoundException(name);
        }
        return doOpenInput(name, context, blobFileRanges);
    }

    protected IndexInput doOpenInput(String name, IOContext context, BlobFileRanges blobFileRanges) {
        return doOpenInput(name, context, blobFileRanges, null);
    }

    /**
     * Opens an {@link IndexInput} for reading an existing file.
     *
     * @param name the name of an existing file
     * @param context the IO context
     * @param blobFileRanges the {@link BlobFileRanges} associated to the file
     * @param releasable a {@link Releasable} to be released when the {@link IndexInput} is closed
     * @return an {@link IndexInput}
     */
    protected final IndexInput doOpenInput(String name, IOContext context, BlobFileRanges blobFileRanges, @Nullable Releasable releasable) {
        var reader = new CacheFileReader(getCacheFile(blobFileRanges), getCacheBlobReader(blobFileRanges.blobLocation()), blobFileRanges);
        return new BlobCacheIndexInput(name, context, reader, releasable, blobFileRanges.fileLength(), blobFileRanges.fileOffset());
    }

    private SharedBlobCacheService<FileCacheKey>.CacheFile getCacheFile(BlobFileRanges blobFileRanges) {
        return cacheService.getCacheFile(
            new FileCacheKey(shardId, blobFileRanges.primaryTerm(), blobFileRanges.blobName()),
            // this length is a lower bound on the length of the blob, used to assert that the cache file does not try to read
            // data beyond the file boundary within the blob since we overload computeCacheFileRegionSize in
            // StatelessSharedBlobCacheService to fully utilize each region
            // it is also used for bounding the reads we do against indexing shard to ensure that we never read beyond the
            // blob length (with padding added).
            blobFileRanges.fileOffset() + blobFileRanges.fileLength()
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

    public final BlobContainer getBlobContainer(long primaryTerm) {
        var applier = blobContainer.get();
        assert applier != null : "blob container not set";
        return applier.apply(primaryTerm);
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

    /**
     * @return the {@link BlobStoreCacheDirectory} to use for pre-warming, warming or reading BCC purpose.
     * Note: the bytes read using this instance are always added to {@link #totalBytesWarmedFromObjectStore}.
     */
    public BlobStoreCacheDirectory createNewInstance() {
        return this;
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
