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

import co.elastic.elasticsearch.stateless.commits.BlobFileRanges;
import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.blobcache.common.BlobCacheBufferedIndexInput;
import org.elasticsearch.common.lucene.store.FilterIndexOutput;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.ByteSizeDirectory;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.IntConsumer;

import static co.elastic.elasticsearch.stateless.commits.StatelessCommitService.isGenerationalFile;
import static org.elasticsearch.blobcache.BlobCacheUtils.ensureSeek;
import static org.elasticsearch.blobcache.BlobCacheUtils.ensureSlice;
import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.index.store.Store.CORRUPTED_MARKER_NAME_PREFIX;

public class IndexDirectory extends ByteSizeDirectory {

    private static final Logger logger = LogManager.getLogger(IndexDirectory.class);

    /**
     * Directory used to access files stored in the object store through the shared cache. Once a commit is uploaded to the object store,
     * its files should be accessed using this cache directory.
     */
    private final IndexBlobStoreCacheDirectory cacheDirectory;
    /**
     * A callback to invoke when a generational file is deleted (by Lucene). It is used for
     * ref-counting their associated BCC blobs.
     */
    @Nullable
    private final BiConsumer<ShardId, String> onGenerationalFileDeletion;

    /**
     * Map of files created on disk by the indexing shard. A reference {@link LocalFileRef} to this file is kept in the map until the file
     * is deleted by Lucene. The reference to the file can be used to know if the file can be read locally and if so allows to incRef/decRef
     * the reference to hold the file for the time to execute a read operation.
     */
    private final Map<String, LocalFileRef> localFiles = new HashMap<>();

    /**
     * RW locks used to update files and commits
     */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final ReleasableLock writeLock = new ReleasableLock(lock.writeLock());
    private final ReleasableLock readLock = new ReleasableLock(lock.readLock());

    /**
     * An estimation of local non-uploaded files on disk. It does not include deleted files.
     */
    private final AtomicLong estimatedSize = new AtomicLong();

    private final SetOnce<String> recoveryCommitMetadataNodeEphemeralId = new SetOnce<>();
    private final SetOnce<Long> recoveryCommitTranslogRecoveryStartFile = new SetOnce<>();

    private long lastGeneration = -1;

    public IndexDirectory(
        Directory in,
        IndexBlobStoreCacheDirectory cacheDirectory,
        @Nullable BiConsumer<ShardId, String> onGenerationalFileDeletion
    ) {
        super(in);
        this.cacheDirectory = Objects.requireNonNull(cacheDirectory);
        this.onGenerationalFileDeletion = onGenerationalFileDeletion;
    }

    @Override
    public String[] listAll() throws IOException {
        final String[] localFiles;
        try (var ignored = readLock.acquire()) {
            if (this.localFiles.isEmpty()) {
                // we haven't written anything yet, fallback on the empty directory
                return EmptyDirectory.INSTANCE.listAll();
            }
            // under lock since rename() is a two-step operation
            localFiles = this.localFiles.keySet().toArray(String[]::new);
        }
        Arrays.sort(localFiles);
        return localFiles;
    }

    @Override
    public long fileLength(String name) throws IOException {
        if (cacheDirectory.containsFile(name) == false) {
            LocalFileRef localFile;
            try (var ignored = readLock.acquire()) {
                localFile = localFiles.get(name);
            }
            if (localFile != null && localFile.tryIncRefNotUploaded()) {
                try {
                    return super.fileLength(name);
                } finally {
                    localFile.decRef();
                }
            }
        }
        return cacheDirectory.fileLength(name);
    }

    @Override
    public long estimateSizeInBytes() {
        // size is equal to the sum of local non-uploaded files on disk
        return estimatedSize.get();
    }

    @Override
    public long estimateDataSetSizeInBytes() {
        // data set size is equal to the size of the last commit uploaded to the object store
        return cacheDirectory.estimateDataSetSizeInBytes();
    }

    @Override
    public IndexOutput createOutput(final String name, final IOContext context) throws IOException {
        return localFile(super.createOutput(name, context));
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
        return localFile(super.createTempOutput(prefix, suffix, context));
    }

    private IndexOutput localFile(IndexOutput output) throws IOException {
        boolean success = false;
        try {
            final String name = output.getName();
            try (var ignored = writeLock.acquire()) {
                if (localFiles.putIfAbsent(name, new LocalFileRef(name)) != null) {
                    assert false : "directory did not check for file already existing";
                    throw new FileAlreadyExistsException("Local file [" + name + "] already exists");
                }
            }
            logger.trace("{} local file [{}] created", cacheDirectory.getShardId(), name);
            output = new BytesSizeIndexOutput(output, estimatedSize::addAndGet);
            success = true;
            return output;
        } finally {
            if (success == false) {
                IOUtils.close(output);
            }
        }
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        if (cacheDirectory.containsFile(name) == false) {
            LocalFileRef localFile;
            try (var ignored = readLock.acquire()) {
                localFile = localFiles.get(name);
            }
            if (localFile != null && localFile.tryIncRefNotUploaded()) {
                try {
                    return new ReopeningIndexInput(name, context, super.openInput(name, context), localFile);
                } finally {
                    localFile.decRef();
                }
            }
        }
        return cacheDirectory.openInput(name, context);
    }

    @Override
    public void rename(String source, String dest) throws IOException {
        try (var ignored = writeLock.acquire()) {
            var localFile = localFiles.get(source);
            // Lucene only renames pending_segments_N files before finishing the commit so the file should exist on disk
            if (localFile != null && localFile.tryIncRef()) {
                try {
                    if (localFiles.containsKey(dest)) {
                        throw new FileAlreadyExistsException("Local file [" + dest + "] already exists");
                    }
                    super.rename(source, dest);
                    localFiles.put(dest, localFile);
                    localFile.renameTo(dest);
                    localFiles.remove(source);
                } finally {
                    localFile.decRef();
                }
            } else {
                throw new NoSuchFileException(source);
            }
        }
    }

    @Override
    public void deleteFile(String name) throws IOException {
        LocalFileRef localFile;
        try (var ignored = writeLock.acquire()) {
            localFile = localFiles.remove(name);
        }
        if (localFile == null) {
            if (EmptyDirectory.INSTANCE.getSegmentsFileName().equals(name) == false) {
                throw new FileNotFoundException(name);
            }
            return;
        }
        localFile.markAsDeleted();
        if (onGenerationalFileDeletion != null && isGenerationalFile(name)) {
            onGenerationalFileDeletion.accept(cacheDirectory.getShardId(), name);
        }
    }

    public void sync(Collection<String> names) {
        // noop, data on local drive need not be safely persisted
    }

    @Override
    public void syncMetaData() {
        // noop, data on local drive need not be safely persisted
    }

    @Override
    public void close() throws IOException {
        try {
            final String[] files = listAll();
            for (String file : files) {
                if (file.startsWith(CORRUPTED_MARKER_NAME_PREFIX)) {
                    cacheDirectory.forceEvict();
                    break;
                }
            }
        } finally {
            IOUtils.close(cacheDirectory, super::close);
        }
    }

    public IndexBlobStoreCacheDirectory getBlobStoreCacheDirectory() {
        return cacheDirectory;
    }

    public void updateCommit(
        long lastUploadedGeneration,
        long dataSizeInBytes,
        Set<String> uploadedFiles,
        Map<String, BlobFileRanges> blobFileRanges
    ) {
        try (var ignored = writeLock.acquire()) {
            // retaining files will not work if we receive files out of order.
            // StatelessCommitService however promises to only call this in commit generation order.
            assert lastUploadedGeneration >= lastGeneration : "out of order generation " + lastUploadedGeneration + " < " + lastGeneration;
            lastGeneration = lastUploadedGeneration;
            assert blobFileRanges.keySet().containsAll(uploadedFiles);

            cacheDirectory.updateMetadata(blobFileRanges, dataSizeInBytes);
            uploadedFiles.forEach(file -> {
                var localFile = localFiles.get(file);
                if (localFile != null) {
                    localFile.markAsUploaded();
                }
            });
        }
    }

    /**
     * Updates the metadata required for recovery operations. This method is invoked prior to the recovery process
     * to ensure that all necessary information is available for replaying operations from the transaction log (translog) if needed.
     *
     * @param recoveryCommit the base commit point from which recovery will proceed.
     */
    @Deprecated(forRemoval = true)
    public void updateRecoveryCommit(StatelessCompoundCommit recoveryCommit) {
        updateRecoveryCommit(
            recoveryCommit.generation(),
            recoveryCommit.nodeEphemeralId(),
            recoveryCommit.translogRecoveryStartFile(),
            recoveryCommit.sizeInBytes(),
            Maps.transformValues(recoveryCommit.commitFiles(), BlobFileRanges::new)
        );
    }

    /**
     * Updates the metadata required for recovery operations. This method is invoked prior to the recovery process
     * to ensure that all necessary information is available for replaying operations from the transaction log (translog) if needed.
     */
    public void updateRecoveryCommit(
        long generation,
        String nodeEphemeralId,
        long translogRecoveryStartFile,
        long dataSetSizeInBytes,
        Map<String, BlobFileRanges> blobFileRanges
    ) {
        recoveryCommitMetadataNodeEphemeralId.set(nodeEphemeralId);
        recoveryCommitTranslogRecoveryStartFile.set(translogRecoveryStartFile);
        try (var ignored = writeLock.acquire()) {
            assert localFiles.isEmpty();
            blobFileRanges.keySet().forEach(file -> localFiles.putIfAbsent(file, new LocalFileRef(file) {
                @Override
                protected void closeInternal() {
                    // skipping deletion, the file was not created locally
                }
            }));
            updateCommit(generation, dataSetSizeInBytes, blobFileRanges.keySet(), blobFileRanges);
        }
    }

    public Optional<String> getRecoveryCommitMetadataNodeEphemeralId() {
        String nodeEphemeralId = recoveryCommitMetadataNodeEphemeralId.get();
        return nodeEphemeralId != null ? Optional.of(nodeEphemeralId) : Optional.empty();
    }

    public long getTranslogRecoveryStartFile() {
        Long translogRecoveryStartFile = recoveryCommitTranslogRecoveryStartFile.get();
        return translogRecoveryStartFile != null ? translogRecoveryStartFile : 0;
    }

    public static IndexDirectory unwrapDirectory(final Directory directory) {
        Directory dir = directory;
        while (dir != null) {
            if (dir instanceof IndexDirectory indexDirectory) {
                return indexDirectory;
            } else if (dir instanceof FilterDirectory) {
                dir = ((FilterDirectory) dir).getDelegate();
            } else {
                dir = null;
            }
        }
        var e = new IllegalStateException(directory.getClass() + " cannot be unwrapped as " + IndexDirectory.class);
        assert false : e;
        throw e;
    }

    class LocalFileRef extends AbstractRefCounted {

        /**
         * Flag to indicate if the file has been deleted by Lucene
         */
        private final AtomicBoolean deleted = new AtomicBoolean();

        /**
         * Flag to indicate if the file has been uploaded to the object store
         */
        private final AtomicBoolean uploaded = new AtomicBoolean();

        /**
         * Used to decRef() the local file reference only once, either when Lucene deletes a file or when the file is uploaded
         */
        private final AtomicBoolean removeOnce = new AtomicBoolean();

        /**
         * Listeners to be completed once the file is uploaded to the object store. This is used to force the reopening of inputs.
         */
        private Set<ReopeningIndexInput> listeners = null;

        /**
         * Flag to indicate if listeners have been completed
         */
        private boolean completed = false;

        /**
         * File name to use when deleting the file on filesystem (pending_segments_N files are renamed before commit)
         */
        private volatile String name;

        LocalFileRef(String name) {
            this.name = Objects.requireNonNull(name);
        }

        private void renameTo(String name) {
            if (deleted.get() || removeOnce.get()) {
                throw new IllegalStateException(
                    "Cannot rename file ["
                        + this.name
                        + "] to ["
                        + name
                        + "]: "
                        + (deleted.get() ? " file is deleted" : "file is marked as uploaded")
                );
            }
            this.name = name;
        }

        private void decRefOnce() {
            if (removeOnce.compareAndSet(false, true)) {
                decRef();
            }
        }

        public void markAsDeleted() throws FileNotFoundException {
            if (deleted.compareAndSet(false, true) == false) {
                throw new FileNotFoundException(name);
            }
            logger.trace("{} {} local file is marked as deleted", cacheDirectory.getShardId(), name);
            decRefOnce();
        }

        public void markAsUploaded() {
            if (uploaded.compareAndSet(false, true)) {
                logger.trace("{} {} local file is marked as uploaded", cacheDirectory.getShardId(), name);
                completeListeners();
                decRefOnce();
            }
        }

        /**
         * Adds a {@link ReopeningIndexInput} that will be reopened once the local file has been uploaded to the object store
         */
        private void addListener(ReopeningIndexInput input) {
            boolean completeNow = false;
            synchronized (this) {
                if (completed == false) {
                    if (listeners == null) {
                        listeners = new HashSet<>();
                    }
                    var added = listeners.add(input);
                    assert added : "listener already exists: " + input;
                } else {
                    assert listeners == null;
                    completeNow = true;
                }
            }
            if (completeNow) {
                assert assertEmptyListeners();
                reopenIndexInput(input);
            }
        }

        /**
         * Removes a {@link ReopeningIndexInput} from the set of upload listeners
         */
        private void removeListener(ReopeningIndexInput input) {
            synchronized (this) {
                assert listeners != null || completed;
                if (completed == false) {
                    var removed = listeners.remove(input);
                    assert removed : "listener not found: " + input;
                    if (listeners.isEmpty()) {
                        listeners = null;
                    }
                }
            }
        }

        private void completeListeners() {
            Set<ReopeningIndexInput> listenersToComplete = Set.of();
            synchronized (this) {
                assert completed == false : "listeners already completed";
                if (listeners != null) {
                    listenersToComplete = listeners;
                    listeners = null;
                }
                completed = true;
            }
            listenersToComplete.forEach(this::reopenIndexInput);
        }

        private void reopenIndexInput(ReopeningIndexInput input) {
            try {
                input.reopenInputFromCache();
            } catch (Exception e) {
                assert false : e;
                logger.warn(() -> format("%s unexpected exception when reopening %s", cacheDirectory.getShardId(), input), e);
            }
        }

        private boolean assertEmptyListeners() {
            synchronized (this) {
                assert listeners == null : listeners;
            }
            return true;
        }

        public boolean isUploaded() {
            return uploaded.get();
        }

        /**
         * Tries to increment the refCount of the local file only if the file is not marked as uploaded yet.
         */
        public boolean tryIncRefNotUploaded() {
            if (isUploaded()) {
                return false;
            }
            return tryIncRef();
        }

        @Override
        protected void closeInternal() {
            assert assertEmptyListeners();
            final String name = this.name;
            long length = -1L;
            try {
                length = in.fileLength(name);
                logger.trace("{} deleting file {} ({} bytes) from disk", cacheDirectory.getShardId(), name, length);
                in.deleteFile(name);

                var size = estimatedSize.addAndGet(-length);
                assert size >= 0L : "directory estimated size cannot be negative: " + size;
            } catch (IOException e) {
                String lengthAsString = (length == -1L ? "unknown" : String.valueOf(length));
                logger.warn(
                    () -> format(
                        "%s unable to delete local file [%s] of %s bytes size from disk (the file may or may not still exist on disk), "
                            + "directory size will probably now diverge from real directory disk usage",
                        cacheDirectory.getShardId(),
                        name,
                        lengthAsString
                    ),
                    e
                );
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public String toString() {
            return "LocalFileRef [name=" + name + ", uploaded=" + uploaded.get() + ", deleted=" + deleted.get() + ']';
        }
    }

    /**
     * IndexInput that reads bytes from another IndexInput by incRef/decRef a {@link RefCounted} before every read/clone/slice operation. If
     * the {@link RefCounted} does not allow incrementing the ref, then {@link ReopeningIndexInput} will reopen the IndexInput from the
     * cache directory. When it reopens an IndexInput it takes care of restoring the position in the file as well as the clone or slice
     * state.
     */
    class ReopeningIndexInput extends BlobCacheBufferedIndexInput {

        private final String name;
        private final IOContext context;
        private final LocalFileRef localFile;

        private final String sliceDescription;
        private final long sliceOffset;
        private final long sliceLength;

        private volatile Delegate delegate;
        private boolean closed;
        private boolean clone;
        private long position;

        ReopeningIndexInput(String name, IOContext context, IndexInput delegate, LocalFileRef localFile) {
            this(name, delegate.length(), context, localFile, new LocalDelegate(name, delegate, localFile), null, 0L, 0L);
            localFile.addListener(this);
        }

        private ReopeningIndexInput(
            String name,
            long length,
            IOContext context,
            LocalFileRef localFile,
            Delegate delegate,
            String sliceDescription,
            long sliceOffset,
            long sliceLength
        ) {
            super("reopening(" + name + ')', BlobCacheBufferedIndexInput.BUFFER_SIZE, length);
            this.name = name;
            this.context = context;
            this.localFile = localFile;
            this.sliceDescription = sliceDescription;
            this.sliceOffset = sliceOffset;
            this.sliceLength = sliceLength;
            this.delegate = delegate;
        }

        /**
         * Base class for all delegate implementations
         */
        abstract static class Delegate extends FilterIndexInput implements RefCounted {

            protected final AtomicBoolean closed;
            private final boolean cached;

            private Delegate(String name, boolean cached, IndexInput input) {
                super("delegate(" + name + ')', input);
                this.closed = new AtomicBoolean(false);
                this.cached = cached;
            }

            public final boolean isCached() {
                return cached;
            }
        }

        /**
         * A delegate implementation that allows reading from a local file on disk.
         * <p>
         * The delegate is a {@link RefCounted} object that holds a ref on a {@link LocalFileRef} until the delegate is fully released. This
         * is useful to know which files on disk are really opened for reading. It also allows to keep a more accurate estimate of the
         * directory total size, and having the file apparent in the directory folder can be useful for debugging purpose too.
         * </p>
         * <p>
         * For example, during merges Lucene might delete a local file using {@link #deleteFile(String)} and continue to read from it. By
         * holding a ref on the {@link LocalFileRef}, the file remains in the directory folder on disk until the {@link LocalDelegate} is
         * fully released. This specific bug has been fixed in Lucene 9.11.0 (https://github.com/apache/lucene/pull/13017) so we might want
         * to revisit this.
         * </p>
         * <p>
         * When the local file is uploaded to the object store, the {@link LocalFileRef} calls back the {@link ReopeningIndexInput}
         * instances to reopen them from the cache. This will close the {@link LocalDelegate}, which in turn will release the local file
         * and will allow its deletion from the disk.
         * </p>
         * Clones and slices of {@link LocalDelegate} work a bit differently and do not hold a ref on the {@link LocalFileRef}. Instead,
         * they hold a ref on the parent {@link LocalDelegate} itself (ie, the original index input) and only for the time of executing an
         * operation. If the parent has been released, then the clones and slices should be reopened from the cache.
         */
        private static class LocalDelegate extends Delegate {

            private final String name;
            private final LocalFileRef localFile;
            private final AbstractRefCounted refCounted;

            private LocalDelegate(String name, IndexInput input, LocalFileRef localFile) {
                super("local(" + name + ')', false, input);
                this.name = Objects.requireNonNull(name);
                this.localFile = Objects.requireNonNull(localFile);
                this.localFile.incRef(); // Do we need to revisit this? It is fixed in Lucene (https://github.com/apache/lucene/pull/13017)
                this.refCounted = AbstractRefCounted.of(() -> {
                    try {
                        try {
                            super.close();
                        } catch (IOException e) {
                            assert false : e;
                            throw new UncheckedIOException(e);
                        }
                    } finally {
                        localFile.decRef();
                    }
                });
            }

            @Override
            public void incRef() {
                refCounted.incRef();
            }

            @Override
            public boolean tryIncRef() {
                if (localFile.isUploaded()) {
                    // the file is uploaded, use the cached delegate
                    return false;
                }
                return refCounted.tryIncRef();
            }

            @Override
            public boolean decRef() {
                return refCounted.decRef();
            }

            @Override
            public boolean hasReferences() {
                return refCounted.hasReferences();
            }

            @Override
            public void close() {
                if (closed.compareAndSet(false, true)) {
                    refCounted.decRef();
                }
            }

            @Override
            public IndexInput slice(String sliceDescription, long sliceOffset, long sliceLength) throws IOException {
                return new Clone(in.slice(sliceDescription, sliceOffset, sliceLength), this);
            }

            @Override
            public IndexInput clone() {
                return new Clone(in.clone(), this);
            }

            @Override
            public String toString() {
                return "LocalDelegate [input=" + in + ", refCount=" + refCounted.refCount() + ", localFile=" + localFile + ']';
            }

            /**
             * Clone or slice of a {@link LocalDelegate}.
             * <p>
             * Clones and slices try to increment/decrement a reference on the original index input before executing an operation.
             * </p>
             */
            private static class Clone extends Delegate {

                private final LocalDelegate parent;

                private Clone(IndexInput input, LocalDelegate parent) {
                    super("clone(local(" + parent.name + "))", false, input);
                    this.parent = Objects.requireNonNull(parent);
                }

                @Override
                public void incRef() {
                    parent.incRef();
                }

                @Override
                public boolean tryIncRef() {
                    return parent.tryIncRef();
                }

                @Override
                public boolean decRef() {
                    return parent.decRef();
                }

                @Override
                public boolean hasReferences() {
                    return parent.hasReferences();
                }

                @Override
                public void close() throws IOException {
                    // clones are not closed and should not close the index input,
                    // the parent will close the original index input
                }

                @Override
                public IndexInput slice(String sliceDescription, long sliceOffset, long sliceLength) throws IOException {
                    return new Clone(in.slice(sliceDescription, sliceOffset, sliceLength), parent);
                }

                @Override
                public IndexInput clone() {
                    return new Clone(in.clone(), parent);
                }

                @Override
                public String toString() {
                    return "Clone of LocalDelegate [input=" + in + ", parent=" + parent + ']';
                }
            }
        }

        /**
         * A delegate implementation that allows reading from the cache.
         */
        private static class CachedDelegate extends Delegate {

            private final boolean clone;

            private CachedDelegate(String name, IndexInput input, boolean clone) {
                super("cached(" + name + ')', true, input);
                assert FilterIndexInput.unwrap(input) instanceof BlobCacheIndexInput : input;
                this.clone = clone;
            }

            @Override
            public void incRef() {
                throw new UnsupportedOperationException("incRef is not supported");
            }

            @Override
            public boolean tryIncRef() {
                throw new UnsupportedOperationException("tryIncRef is not supported");
            }

            @Override
            public boolean decRef() {
                throw new UnsupportedOperationException("decRef is not supported");
            }

            @Override
            public boolean hasReferences() {
                throw new UnsupportedOperationException("hasReferences is not supported");
            }

            @Override
            public void close() throws IOException {
                if (closed.compareAndSet(false, true)) {
                    if (clone == false) {
                        super.close();
                    }
                }
            }

            @Override
            public IndexInput clone() {
                throw new UnsupportedOperationException("Cloning is not supported, clone the delegate input instead");
            }

            @Override
            public String toString() {
                return "CachedDelegate [input=" + in + ", clone=" + clone + ']';
            }
        }

        /**
         * Executes the checked function using the local file input if the file is available on disk and not yet uploaded. Otherwise,
         * reopens the current {@link ReopeningIndexInput} from cache and executes the checked function using the cached input.
         */
        private <T> T executeLocallyOrReopen(CheckedFunction<Delegate, T, IOException> fn) throws IOException {
            var currentDelegate = delegate;
            if (currentDelegate.isCached()) {
                return fn.apply(currentDelegate);
            }
            synchronized (this) {
                currentDelegate = delegate;
                if (currentDelegate.isCached() == false) {
                    if (currentDelegate.tryIncRef()) {
                        try {
                            return fn.apply(currentDelegate);
                        } finally {
                            currentDelegate.decRef();
                        }
                    }
                }
            }
            return fn.apply(reopenInputFromCache());
        }

        Delegate getDelegate() {
            return delegate;
        }

        @Override
        public synchronized void close() throws IOException {
            if (closed == false && clone == false) {
                localFile.removeListener(this);
                delegate.close();
            }
            closed = true;
        }

        @Override
        protected void seekInternal(long pos) throws IOException {
            executeLocallyOrReopen(current -> {
                ensureSeek(pos, current);
                current.seek(pos);
                position = pos;
                return null;
            });
        }

        @Override
        public IndexInput clone() {
            var bufferClone = tryCloneBuffer();
            if (bufferClone != null) {
                return bufferClone;
            }
            try {
                return executeLocallyOrReopen(current -> {
                    if (current.isCached()) {
                        // We clone the actual delegate input. No need to clone our Delegate wrapper with the "cached" flag.
                        IndexInput inputToClone = current.getDelegate();
                        assert FilterIndexInput.unwrap(inputToClone) instanceof BlobCacheIndexInput : toString();
                        return seekOnClone(inputToClone.clone());
                    } else {
                        final var clone = (ReopeningIndexInput) super.clone();
                        clone.delegate = (Delegate) current.clone();
                        clone.clone = true;
                        return clone;
                    }
                });
            } catch (IOException e) {
                assert false : e;
                throw new UncheckedIOException(e);
            }
        }

        /**
         * Apply the super file pointer (based on buffer position) to the cloned object.
         */
        private IndexInput seekOnClone(IndexInput clone) {
            try {
                clone.seek(getFilePointer());
            } catch (IOException e) {
                assert false : e;
                throw new UncheckedIOException(e);
            }
            return clone;
        }

        @Override
        public IndexInput slice(String sliceDescription, long sliceOffset, long sliceLength) throws IOException {
            var arraySlice = trySliceBuffer(sliceDescription, sliceOffset, length());
            if (arraySlice != null) {
                return arraySlice;
            }
            assert sliceDescription != null;
            return executeLocallyOrReopen(current -> {
                if (current.isCached()) {
                    assert FilterIndexInput.unwrap(current.getDelegate()) instanceof BlobCacheIndexInput : toString();
                    return current.slice(sliceDescription, sliceOffset, sliceLength);
                } else {
                    ensureSlice(sliceDescription, sliceOffset, sliceLength, current);
                    var slice = new ReopeningIndexInput(
                        name,
                        sliceLength,
                        context,
                        null,
                        (Delegate) current.slice(sliceDescription, sliceOffset, sliceLength),
                        sliceDescription,
                        this.sliceOffset + sliceOffset,
                        sliceLength
                    );
                    slice.clone = true;
                    return slice;
                }
            });
        }

        @Override
        protected void readInternal(ByteBuffer b) throws IOException {
            executeLocallyOrReopen(current -> {
                assert assertPositionMatchesFilePointer(current);
                var len = b.remaining();
                var offset = b.position();
                current.readBytes(b.array(), offset, len);
                b.position(offset + len);
                position += len;
                return null;
            });
        }

        private synchronized Delegate reopenInputFromCache() throws IOException {
            if (delegate.isCached() == false) {
                try {
                    if (closed == false) {
                        var next = cacheDirectory.openInput(name, context);
                        assert FilterIndexInput.unwrap(next) instanceof BlobCacheIndexInput : next;
                        if (this.sliceDescription != null) {
                            next = next.slice(this.sliceDescription, this.sliceOffset, this.sliceLength);
                        }
                        if (position > 0L) {
                            next.seek(position);
                        }
                        delegate.close();
                        Delegate nextDelegate = new CachedDelegate(name, next, clone);
                        this.delegate = nextDelegate;
                        return nextDelegate;
                    }
                } catch (IOException e) {
                    logger.error(() -> "Failed to reopen " + this, e);
                    assert false : e;
                    throw e;
                }
            }
            return delegate;
        }

        @Override
        public String toString() {
            return "ReopeningIndexInput {["
                + name
                + "], context="
                + context
                + ", delegate="
                + delegate
                + ", clone="
                + clone
                + ", position="
                + position
                + ", sliceDescription="
                + sliceDescription
                + ", sliceOffset="
                + sliceOffset
                + ", sliceLength="
                + sliceLength
                + '}';
        }

        private boolean assertPositionMatchesFilePointer(IndexInput in) {
            var filePointer = in.getFilePointer();
            assert position == filePointer
                : "Expect position [" + position + "] to be equal to file pointer [" + filePointer + "] of IndexInput " + in;
            return true;
        }
    }

    private static class BytesSizeIndexOutput extends FilterIndexOutput {

        private final IntConsumer consumer;

        BytesSizeIndexOutput(IndexOutput out, IntConsumer consumer) {
            super("BytesSizeIndexOutput(" + out.getName() + ')', out);
            this.consumer = Objects.requireNonNull(consumer);
        }

        @Override
        public void writeByte(byte b) throws IOException {
            super.writeByte(b);
            consumer.accept(1);
        }

        @Override
        public void writeBytes(byte[] b, int offset, int length) throws IOException {
            super.writeBytes(b, offset, length);
            consumer.accept(length);
        }
    }
}
