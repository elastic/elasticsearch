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

import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit;

import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.blobcache.common.BlobCacheBufferedIndexInput;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.common.lucene.store.FilterIndexOutput;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.ByteSizeDirectory;
import org.elasticsearch.index.store.LuceneFilesExtensions;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntConsumer;

import static org.elasticsearch.blobcache.BlobCacheUtils.ensureSeek;
import static org.elasticsearch.blobcache.BlobCacheUtils.ensureSlice;

public class IndexDirectory extends ByteSizeDirectory {

    private static final String PRUNE_LUCENE_PROPERTY = "es.stateless.prune_lucene_files_on_upload";
    private static final boolean PRUNE_LUCENE_FILES_ON_UPLOAD;

    static {
        PRUNE_LUCENE_FILES_ON_UPLOAD = Booleans.parseBoolean(System.getProperty(PRUNE_LUCENE_PROPERTY), true);
    }

    private static final Logger logger = LogManager.getLogger(IndexDirectory.class);

    /**
     * Directory used to access files stored in the object store through the shared cache. Once a commit is uploaded to the object store,
     * its files should be accessed using this cache directory.
     */
    private final SearchDirectory cacheDirectory;

    /**
     * Map of files created on disk by the indexing shard. A reference {@link LocalFileRef} to this file is kept in the map until the file
     * is deleted by Lucene. The reference to the file can be used to know if the file can be read locally and if so allows to incRef/decRef
     * the reference to hold the file for the time to execute a read operation.
     */
    private final Map<String, LocalFileRef> localFiles = new HashMap<>();

    /**
     * An estimation of local non-uploaded files on disk. It does not include deleted files.
     */
    private final AtomicLong estimatedSize = new AtomicLong();

    public IndexDirectory(Directory in, SharedBlobCacheService<FileCacheKey> cacheService, ShardId shardId) {
        super(in);
        this.cacheDirectory = new SearchDirectory(cacheService, shardId);
    }

    @Override
    public String[] listAll() throws IOException {
        final Set<String> localFiles;
        synchronized (this) {
            if (this.localFiles.isEmpty()) {
                // we haven't written anything yet, fallback on the empty directory
                return EmptyDirectory.INSTANCE.listAll();
            }
            // need to be synchronized since rename() is a two-step operation
            localFiles = new HashSet<>(this.localFiles.keySet());
        }

        return localFiles.stream().sorted(String::compareTo).toArray(String[]::new);
    }

    @Override
    public long fileLength(String name) throws IOException {
        if (cacheDirectory.containsFile(name) == false) {
            LocalFileRef localFile;
            synchronized (this) {
                localFile = localFiles.get(name);
            }
            if (localFile != null && localFile.tryIncRef()) {
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
            synchronized (this) {
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
        // Prefer the local file if PRUNE_LUCENE_FILES_ON_UPLOAD is set to False
        if (PRUNE_LUCENE_FILES_ON_UPLOAD == false || cacheDirectory.containsFile(name) == false) {
            LocalFileRef localFile;
            synchronized (this) {
                localFile = localFiles.get(name);
            }
            if (localFile != null && localFile.tryIncRef()) {
                try {
                    var input = super.openInput(name, context);
                    // Lucene can open a generation docs values file to merge soft deletes: it first opens the IndexInput, clones it and
                    // starts merging from the cloned input. If the IndexInput has been opened from a NRT reader and is not part of a Lucene
                    // commit it might be released and deleted from disk by Lucene (due to a subsequent refresh or commit). In that case the
                    // original IndexInput is closed and deleted but the merging thread expects to be able to continue reading from the
                    // cloned instance. Because we still want to delete that file in case it is later committed and uploaded to the object
                    // store, we wrap the underlying index input in a GenerationalDocsValuesIndexInput class that uses the LocalFileRef.
                    if (isGenerationalDocsValuesFile(name)) {
                        return new GenerationalDocsValuesIndexInput(name, input, localFile);
                    } else {
                        return new ReopeningIndexInput(name, context, input, localFile);
                    }
                } finally {
                    localFile.decRef();
                }
            }
        }
        return cacheDirectory.openInput(name, context);
    }

    @Override
    public void rename(String source, String dest) throws IOException {
        synchronized (this) {
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
        synchronized (this) {
            localFile = localFiles.remove(name);
        }
        if (localFile == null) {
            if (EmptyDirectory.INSTANCE.getSegmentsFileName().equals(name) == false) {
                throw new FileNotFoundException(name);
            }
            return;
        }
        localFile.markAsDeleted();
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
        IOUtils.close(cacheDirectory, super::close);
    }

    public SearchDirectory getSearchDirectory() {
        return cacheDirectory;
    }

    public void updateCommit(StatelessCompoundCommit commit) {
        synchronized (this) {
            cacheDirectory.updateCommit(commit);
            if (localFiles.isEmpty()) {
                // create references for first commit files if they are not known
                commit.commitFiles().keySet().forEach(file -> localFiles.putIfAbsent(file, new LocalFileRef(file) {
                    @Override
                    protected void closeInternal() {
                        // skipping deletion, the file was not created locally
                    }
                }));

                // Mark all the files added as uploaded since they came from the object store
                commit.commitFiles().keySet().forEach(file -> localFiles.get(file).markAsUploaded());
            } else if (PRUNE_LUCENE_FILES_ON_UPLOAD) {
                commit.commitFiles().keySet().forEach(file -> {
                    var localFile = localFiles.get(file);
                    if (localFile != null) {
                        localFile.markAsUploaded();
                    }
                });
            }
        }
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
         * Used to decRef() the local file reference only once, either when Lucene deletes a file or when the file is uploaded
         */
        private final AtomicBoolean removeOnce = new AtomicBoolean();

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

        public void markAsDeleted() throws FileNotFoundException {
            if (deleted.compareAndSet(false, true) == false) {
                throw new FileNotFoundException(name);
            }
            if (removeOnce.compareAndSet(false, true)) {
                logger.trace("{} local file is marked as deleted", name);
                decRef();
            }
        }

        public void markAsUploaded() {
            if (removeOnce.compareAndSet(false, true)) {
                logger.trace("{} local file is marked as uploaded", name);
                decRef();
            }
        }

        @Override
        protected void closeInternal() {
            final String name = this.name;
            long length = -1L;
            try {
                length = in.fileLength(name);
                logger.trace("{} deleting file {} ({} bytes) from disk", cacheDirectory.getShardId(), name, length);
                in.deleteFile(name);

                var size = estimatedSize.addAndGet(-length);
                assert size >= 0L : "directory estimated size cannot be negative: " + size;
            } catch (IOException e) {
                logger.warn(
                    "{} unable to delete local file [{}] of {} bytes size from disk (the file may or may not still exist on disk),"
                        + " directory size will probably now diverge from real directory disk usage",
                    cacheDirectory.getShardId(),
                    name,
                    (length == -1L ? "unknown" : length),
                    e
                );
                throw new UncheckedIOException(e);
            }
        }
    }

    /**
     * IndexInput that reads bytes from another IndexInput by incRef/decRef a {@link RefCounted} before every read/clone/slice operation. If
     * the {@link RefCounted} does not allow incrementing the ref, then {@link ReopeningIndexInput} will reopen the IndexInput from the
     * cache directory. When it reopens an IndexInput it takes care of restoring the position in the file as well as the clone or slice
     * state.
     */
    private class ReopeningIndexInput extends BlobCacheBufferedIndexInput {

        private final String name;
        private final long length;
        private final IOContext context;
        private final LocalFileRef localFile;

        private final String sliceDescription;
        private final long sliceOffset;
        private final long sliceLength;

        private volatile Delegate delegate;
        private boolean clone;
        private long position;

        ReopeningIndexInput(String name, IOContext context, IndexInput delegate, LocalFileRef localFile) {
            this(name, delegate.length(), context, new Delegate(name, false, delegate), localFile, null, 0L, 0L);
        }

        private ReopeningIndexInput(
            String name,
            long length,
            IOContext context,
            Delegate delegate,
            LocalFileRef localFile,
            String sliceDescription,
            long sliceOffset,
            long sliceLength
        ) {
            super("reopening(" + name + ')', BlobCacheBufferedIndexInput.BUFFER_SIZE);
            this.name = name;
            this.length = length;
            this.context = context;
            this.localFile = localFile;
            this.sliceDescription = sliceDescription;
            this.sliceOffset = sliceOffset;
            this.sliceLength = sliceLength;
            this.delegate = delegate;
        }

        private static class Delegate extends FilterIndexInput {

            private final boolean isReopened;

            private Delegate(String name, boolean isReopened, IndexInput input) {
                super("delegate(" + name + ')', input);
                this.isReopened = isReopened;
            }

            public boolean isReopened() {
                return isReopened;
            }

            @Override
            public String toString() {
                return "Delegate [isReopened=" + isReopened + ", input=" + in.getClass() + ']';
            }
        }

        @Override
        public void close() throws IOException {
            if (clone == false) {
                delegate.close();
            }
        }

        @Override
        public long length() {
            return length;
        }

        @Override
        protected void seekInternal(long pos) throws IOException {
            var currentDelegate = this.delegate;
            if (currentDelegate.isReopened()) {
                currentDelegate.seek(pos);
                position = pos;
                return;
            }
            if (localFile.tryIncRef()) {
                try {
                    currentDelegate.seek(pos);
                    position = pos;
                    return;
                } finally {
                    localFile.decRef();
                }
            }
            ensureSeek(pos, this);
            reopenInputFromCache().seek(pos);
            position = pos;
        }

        @Override
        public BlobCacheBufferedIndexInput clone() {
            // Note: Lucene can clone a slice or a clone
            var currentDelegate = this.delegate;
            // We clone the actual delegate input. No need to clone our wrapper with the isReopened marker.
            IndexInput inputToClone = currentDelegate.getDelegate();
            if (currentDelegate.isReopened()) {
                assert inputToClone instanceof SearchIndexInput : toString();
                return ((BlobCacheBufferedIndexInput) inputToClone).clone();
            }
            if (localFile.tryIncRef()) {
                try {
                    final var clone = (ReopeningIndexInput) super.clone();
                    clone.delegate = new Delegate(name, false, inputToClone.clone());
                    clone.clone = true;
                    return clone;
                } finally {
                    localFile.decRef();
                }
            }

            try {
                // We clone the actual delegate input. No need to clone our wrapper with the isReopened marker.
                return (BlobCacheBufferedIndexInput) reopenInputFromCache().getDelegate().clone();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public IndexInput slice(String sliceDescription, long sliceOffset, long sliceLength) throws IOException {
            assert sliceDescription != null;
            // Note: Lucene can slice a clone or a slice
            var currentDelegate = this.delegate;
            if (currentDelegate.isReopened()) {
                assert currentDelegate.getDelegate() instanceof SearchIndexInput : toString();
                return currentDelegate.slice(sliceDescription, sliceOffset, sliceLength);
            }
            if (localFile.tryIncRef()) {
                try {
                    ensureSlice(sliceDescription, sliceOffset, sliceLength, this);
                    return new ReopeningIndexInput(
                        name,
                        sliceLength,
                        context,
                        new Delegate(name, false, currentDelegate.slice(sliceDescription, sliceOffset, sliceLength)),
                        localFile,
                        sliceDescription,
                        this.sliceOffset + sliceOffset,
                        sliceLength
                    );
                } finally {
                    localFile.decRef();
                }
            }
            ensureSlice(sliceDescription, sliceOffset, sliceLength, this);
            return reopenInputFromCache().slice(sliceDescription, sliceOffset, sliceLength);
        }

        @Override
        protected void readInternal(ByteBuffer b) throws IOException {
            var currentDelegate = this.delegate;
            if (currentDelegate.isReopened()) {
                readBytes(currentDelegate, b);
                return;
            }
            if (localFile.tryIncRef()) {
                try {
                    readBytes(currentDelegate, b);
                    return;
                } finally {
                    localFile.decRef();
                }
            }
            readBytes(reopenInputFromCache(), b);
        }

        private void readBytes(IndexInput in, ByteBuffer b) throws IOException {
            assert assertPositionMatchesFilePointer(in);
            var len = b.remaining();
            var offset = b.position();
            in.readBytes(b.array(), offset, len);
            b.position(offset + len);
            position += len;
        }

        private synchronized Delegate reopenInputFromCache() throws IOException {
            if (delegate.isReopened() == false) {
                try {
                    var next = cacheDirectory.openInput(name, context);
                    assert next instanceof SearchIndexInput : next;
                    if (this.sliceDescription != null) {
                        next = next.slice(this.sliceDescription, this.sliceOffset, this.sliceLength);
                    }
                    if (position > 0L) {
                        next.seek(position);
                    }
                    delegate.close();
                    Delegate nextDelegate = new Delegate(name, true, next);
                    this.delegate = nextDelegate;
                    return nextDelegate;
                } catch (Exception e) {
                    logger.error(() -> "Failed to reopen " + this, e);
                    throw e;
                }
            } else {
                assert delegate.isReopened() : toString();
                return delegate;
            }
        }

        @Override
        public String toString() {
            return "ReopeningIndexInput ["
                + name
                + "]["
                + ", context="
                + context
                + ", localFile="
                + localFile
                + ", delegate="
                + delegate
                + ", clone="
                + clone
                + ", position="
                + position
                + ", sliceDescription='"
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

    private static class GenerationalDocsValuesIndexInput extends FilterIndexInput {

        private final LocalFileRef localFile;
        private final AtomicBoolean closed;

        GenerationalDocsValuesIndexInput(String name, IndexInput delegate, LocalFileRef localFileRef) {
            super("gendocsvalues(" + name + ')', delegate);
            localFileRef.incRef();
            this.localFile = localFileRef;
            this.closed = new AtomicBoolean(false);
        }

        @Override
        public void close() throws IOException {
            try {
                super.close();
            } finally {
                if (closed.compareAndSet(false, true)) {
                    localFile.decRef();
                }
            }
        }

        @Override
        public IndexInput clone() {
            return in.clone();
        }

        @Override
        public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
            var slice = super.slice(sliceDescription, offset, length);
            if (localFile != null) {
                slice = new GenerationalDocsValuesIndexInput(sliceDescription, slice, localFile);
            }
            return slice;
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

    /**
     * Identify generational docs values files by their name (ex: "_0_1_Lucene90_0.dvd"). Those files are used to update docs values in
     * existing segments and in Elasticsearch we use them for soft deletes. Those files are special since they can be added after the
     * segment is created and potentially outside of a compound segment.
     *
     * @param fileName name of the file
     * @return true if the file name matches a generational docs values files
     */
    public static boolean isGenerationalDocsValuesFile(String fileName) {
        try {
            return fileName.startsWith("_")
                && IndexFileNames.matchesExtension(fileName, LuceneFilesExtensions.DVD.getExtension())
                && IndexFileNames.parseGeneration(fileName) > 0L;
        } catch (NumberFormatException e) {
            // ignore
        }
        return false;
    }
}
