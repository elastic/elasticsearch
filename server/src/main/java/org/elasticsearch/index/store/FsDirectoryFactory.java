/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.store;

import org.apache.lucene.misc.store.DirectIODirectory;
import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FileDataHint;
import org.apache.lucene.store.FileSwitchDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.NativeFSLockFactory;
import org.apache.lucene.store.NoReuseHint;
import org.apache.lucene.store.ReadAdvice;
import org.apache.lucene.store.ReadOnceHint;
import org.apache.lucene.store.SimpleFSLockFactory;
import org.apache.lucene.util.Constants;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.StandardIOBehaviorHint;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.DenseVectorFieldType;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.DenseVectorIndexOptions;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.plugins.IndexStorePlugin;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Supplier;

import static org.apache.lucene.store.MMapDirectory.SHARED_ARENA_MAX_PERMITS_SYSPROP;

public class FsDirectoryFactory implements IndexStorePlugin.DirectoryFactory {

    private static final Logger Log = LogManager.getLogger(FsDirectoryFactory.class);
    private static final int sharedArenaMaxPermits;
    static {
        String prop = System.getProperty(SHARED_ARENA_MAX_PERMITS_SYSPROP);
        int value = 1;
        if (prop != null) {
            try {
                value = Integer.parseInt(prop); // ensure it's a valid integer
            } catch (NumberFormatException e) {
                Log.warn(() -> "unable to parse system property [" + SHARED_ARENA_MAX_PERMITS_SYSPROP + "] with value [" + prop + "]", e);
            }
        }
        sharedArenaMaxPermits = value; // default to 1
    }

    public static final Setting<LockFactory> INDEX_LOCK_FACTOR_SETTING = new Setting<>("index.store.fs.fs_lock", "native", (s) -> {
        return switch (s) {
            case "native" -> NativeFSLockFactory.INSTANCE;
            case "simple" -> SimpleFSLockFactory.INSTANCE;
            default -> throw new IllegalArgumentException("unrecognized [index.store.fs.fs_lock] \"" + s + "\": must be native or simple");
        }; // can we set on both - node and index level, some nodes might be running on NFS so they might need simple rather than native
    }, Property.IndexScope, Property.NodeScope);

    public static final Setting<Integer> ASYNC_PREFETCH_LIMIT = Setting.intSetting(
        "index.store.fs.directio_async_prefetch_limit",
        64,
        // 0 disables async prefetching
        0,
        // creates 256 * 8k buffers, which is 2MB
        256,
        Property.IndexScope,
        Property.NodeScope
    );

    @Override
    public Directory newDirectory(IndexSettings indexSettings, ShardPath path) throws IOException {
        return newDirectory(indexSettings, path, null, () -> MappingLookup.EMPTY);
    }

    @Override
    public Directory newDirectory(
        IndexSettings indexSettings,
        ShardPath path,
        ShardRouting shardRouting,
        Supplier<MappingLookup> mappingLookup
    ) throws IOException {
        final Path location = path.resolveIndex();
        final LockFactory lockFactory = indexSettings.getValue(INDEX_LOCK_FACTOR_SETTING);
        Files.createDirectories(location);
        return newFSDirectory(location, lockFactory, indexSettings, mappingLookup);
    }

    /**
     * Creates the directory of a shard. {@code mappingLookup} gives the shard's current mapping, for a directory deciding how
     * to open a field's files.
     */
    protected Directory newFSDirectory(
        Path location,
        LockFactory lockFactory,
        IndexSettings indexSettings,
        Supplier<MappingLookup> mappingLookup
    ) throws IOException {
        final int asyncPrefetchLimit = indexSettings.getValue(ASYNC_PREFETCH_LIMIT);
        final String storeType = indexSettings.getSettings()
            .get(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), IndexModule.Type.FS.getSettingsKey());
        IndexModule.Type type;
        if (IndexModule.Type.FS.match(storeType)) {
            type = IndexModule.defaultStoreType(IndexModule.NODE_STORE_ALLOW_MMAP.get(indexSettings.getNodeSettings()));
        } else {
            type = IndexModule.Type.fromSettingsKey(storeType);
        }
        Set<String> preLoadExtensions = new HashSet<>(indexSettings.getValue(IndexModule.INDEX_STORE_PRE_LOAD_SETTING));
        switch (type) {
            case HYBRIDFS:
                // Use Lucene defaults
                final FSDirectory primaryDirectory = FSDirectory.open(location, lockFactory);
                if (primaryDirectory instanceof MMapDirectory mMapDirectory) {
                    mMapDirectory = adjustSharedArenaGrouping(mMapDirectory);
                    return new HybridDirectory(
                        lockFactory,
                        setMMapFunctions(mMapDirectory, preLoadExtensions),
                        asyncPrefetchLimit,
                        mappingLookup
                    );
                } else {
                    return primaryDirectory;
                }
            case MMAPFS:
                MMapDirectory mMapDirectory = adjustSharedArenaGrouping(new MMapDirectory(location, lockFactory));
                return setMMapFunctions(mMapDirectory, preLoadExtensions);
            case SIMPLEFS:
            case NIOFS:
                return new NIOFSDirectory(location, lockFactory);
            default:
                throw new AssertionError("unexpected built-in store type [" + type + "]");
        }
    }

    /** Sets the preload, if any, on the given directory based on the extensions. Returns the same directory instance. */
    // visibility and extensibility for testing
    public MMapDirectory setMMapFunctions(MMapDirectory mMapDirectory, Set<String> preLoadExtensions) {
        mMapDirectory.setPreload(getPreloadFunc(preLoadExtensions));
        mMapDirectory.setReadAdvice(getReadAdviceFunc());
        return mMapDirectory;
    }

    public MMapDirectory adjustSharedArenaGrouping(MMapDirectory mMapDirectory) {
        if (sharedArenaMaxPermits <= 1) {
            mMapDirectory.setGroupingFunction(MMapDirectory.NO_GROUPING);
        }
        return mMapDirectory;
    }

    /** Gets a preload function based on the given preLoadExtensions. */
    static BiPredicate<String, IOContext> getPreloadFunc(Set<String> preLoadExtensions) {
        if (preLoadExtensions.isEmpty() == false) {
            if (preLoadExtensions.contains("*")) {
                return MMapDirectory.ALL_FILES;
            } else {
                return (name, context) -> preLoadExtensions.contains(FileSwitchDirectory.getExtension(name));
            }
        }
        return MMapDirectory.NO_FILES;
    }

    /**
     * The advice a mapping is opened with. Random and sequential advice take pages out of the kernel's recency tracking, so
     * only files that are not reused (a no-reuse or read-once hint) get them, following the access hint.
     */
    public static BiFunction<String, IOContext, Optional<ReadAdvice>> getReadAdviceFunc() {
        return (name, context) -> {
            if (context.hints().contains(StandardIOBehaviorHint.INSTANCE)) {
                return Optional.of(ReadAdvice.NORMAL);
            }
            if (context.hints().contains(NoReuseHint.INSTANCE) || context.hints().contains(ReadOnceHint.INSTANCE)) {
                if (context.hints().contains(DataAccessHint.RANDOM)) {
                    return Optional.of(ReadAdvice.RANDOM);
                }
                if (context.hints().contains(DataAccessHint.SEQUENTIAL)) {
                    return Optional.of(ReadAdvice.SEQUENTIAL);
                }
            }
            return Optional.of(Constants.DEFAULT_READADVICE);
        };
    }

    /**
     * Returns true iff the directory is a hybrid fs directory
     */
    public static boolean isHybridFs(Directory directory) {
        Directory unwrap = FilterDirectory.unwrap(directory);
        return unwrap instanceof HybridDirectory;
    }

    @SuppressForbidden(reason = "requires Files.getFileStore for blockSize")
    private static int getBlockSize(Path path) throws IOException {
        return Math.toIntExact(Files.getFileStore(path).getBlockSize());
    }

    public static final class HybridDirectory extends NIOFSDirectory {
        private final MMapDirectory delegate;
        private final DirectIODirectory directIODelegate;
        private final DirectIODirectory mergeDirectIODelegate;
        /** set once a direct I/O create has succeeded in this directory, see {@link #mergeDirectIOCreates(String)} */
        private volatile boolean mergeDirectIOCreatesWork;
        private static final AtomicInteger DIRECT_IO_PROBE_ID = new AtomicInteger();
        private final AtomicLong nextDirectIOTempFile = new AtomicLong();
        private final Supplier<MappingLookup> mappingLookup;

        public HybridDirectory(LockFactory lockFactory, MMapDirectory delegate, int asyncPrefetchLimit) throws IOException {
            this(lockFactory, delegate, asyncPrefetchLimit, () -> MappingLookup.EMPTY);
        }

        /**
         * @param mappingLookup the shard's current mapping, read each time a vectors file is opened so that a mapping update
         *                      applies to the next one
         */
        public HybridDirectory(
            LockFactory lockFactory,
            MMapDirectory delegate,
            int asyncPrefetchLimit,
            Supplier<MappingLookup> mappingLookup
        ) throws IOException {
            super(delegate.getDirectory(), lockFactory);
            this.delegate = delegate;
            this.mappingLookup = mappingLookup;

            DirectIODirectory directIO = null;
            DirectIODirectory mergeDirectIO = null;
            try {
                // rescore reads: small random reads, two-page buffer, async prefetch
                directIO = new AlwaysDirectIODirectory(
                    delegate,
                    AlwaysDirectIODirectory.RANDOM_ACCESS_BUFFER_SIZE,
                    DirectIODirectory.DEFAULT_MIN_BYTES_DIRECT,
                    asyncPrefetchLimit
                );
            } catch (Exception e) {
                // directio not supported
                Log.warn("Could not initialize DirectIO access for rescoring", e);
            }
            // independent of the rescore delegate: the two differ in buffer size and prefetch, and
            // a failure on either side must not take the other down
            try {
                // a merge streams whole files, reading and writing: one delegate, with Lucene's merge buffer
                // size and no async prefetch, does both
                mergeDirectIO = new AlwaysDirectIODirectory(
                    delegate,
                    DirectIODirectory.DEFAULT_MERGE_BUFFER_SIZE,
                    DirectIODirectory.DEFAULT_MIN_BYTES_DIRECT,
                    0
                );
            } catch (Exception e) {
                // directio not supported: merges read and write through the page cache
                Log.warn("Could not initialize DirectIO access for vector merges", e);
            }
            this.directIODelegate = directIO;
            this.mergeDirectIODelegate = mergeDirectIO;
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            Throwable directIOException = null;
            // the buffer follows the access: a merge streams, a rescore reads at random
            DirectIODirectory dio = context.hints().contains(DataAccessHint.SEQUENTIAL) ? mergeDirectIODelegate : directIODelegate;
            if (dio != null && useDirectIO(name, context)) {
                ensureOpen();
                ensureCanRead(name);
                try {
                    Log.debug("Opening {} with direct IO", name);
                    return dio.openInput(name, context);
                } catch (FileSystemException | UnsupportedOperationException e) {
                    Log.debug(() -> Strings.format("Could not open %s with direct IO", name), e);
                    directIOException = e;
                    // and fallthrough to normal opening below
                }
            }

            try {
                if (useDelegate(name, context)) {
                    // we need to do these checks on the outer directory since the inner doesn't know about pending deletes
                    ensureOpen();
                    ensureCanRead(name);
                    // we only use the mmap to open inputs. Everything else is managed by the NIOFSDirectory otherwise
                    // we might run into trouble with files that are pendingDelete in one directory but still
                    // listed in listAll() from the other. We on the other hand don't want to list files from both dirs
                    // and intersect for perf reasons.
                    return delegate.openInput(name, context);
                } else {
                    return super.openInput(name, context);
                }
            } catch (Throwable t) {
                if (directIOException != null) {
                    t.addSuppressed(directIOException);
                }
                throw t;
            }
        }

        @Override
        public IndexOutput createOutput(String name, IOContext context) throws IOException {
            // we need to do these checks on the outer directory since the inner doesn't know about pending deletes
            ensureOpen();
            // a direct I/O output opens the file itself, skipping FSDirectory's pending-delete bookkeeping:
            // a live file created under a name that is still pending delete could be removed by a later
            // retry. getPendingDeletions() retries the pending deletes first; a name still pending after
            // that goes down the buffered path below, which takes it off the pending set before creating.
            // Segment file names are not normally reused.
            if (mergeDirectIODelegate != null
                && context.context() == IOContext.Context.MERGE
                && useDirectIO(name, context)
                && getPendingDeletions().contains(name) == false) {
                if (mergeDirectIOCreates(name)) {
                    Log.debug("Creating {} with direct IO", name);
                    return mergeDirectIODelegate.createOutput(name, context);
                }
            }
            return super.createOutput(name, context);
        }

        /** Like {@link #createOutput}, for a merge's temp copy of the raw vectors. */
        @Override
        public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
            ensureOpen();
            if (mergeDirectIODelegate != null
                && context.context() == IOContext.Context.MERGE
                && useDirectIO(getTempFileName(prefix, suffix, 0), context)
                && mergeDirectIOCreates(prefix)) {
                Set<String> pendingDeletions = getPendingDeletions();
                while (true) {
                    String name = getTempFileName(prefix, suffix, nextDirectIOTempFile.getAndIncrement());
                    if (pendingDeletions.contains(name)) {
                        continue;
                    }
                    try {
                        Log.debug("Creating {} with direct IO", name);
                        return mergeDirectIODelegate.createOutput(name, context);
                    } catch (FileAlreadyExistsException e) {
                        // a buffered temp file took the name: try the next
                    }
                }
            }
            return super.createTempOutput(prefix, suffix, context);
        }

        /**
         * Whether a direct I/O create works in this directory, answered with a probe file of the directory's own: a failed
         * direct open can leave the file behind, since the JDK creates it before setting up direct I/O on the descriptor,
         * and no exception says whether it did, so the merge file itself cannot be the attempt. The probe is written and
         * removed here, named as a temp file so that Lucene skips it when inflating segment generations and sweeps it if it
         * were ever left behind. One success settles the answer for the directory, support being a property of its file
         * system; a failure is not remembered, this file goes down the buffered path and the next one probes again. A probe
         * that fails for any reason must not pass for support, so this catches every I/O failure, not only the shapes
         * {@code openInput} falls back from.
         */
        private boolean mergeDirectIOCreates(String name) {
            if (mergeDirectIOCreatesWork) {
                return true;
            }
            String probe = "_directio_probe_" + DIRECT_IO_PROBE_ID.incrementAndGet() + ".tmp";
            Path probePath = getDirectory().resolve(probe);
            try (IndexOutput out = mergeDirectIODelegate.createOutput(probe, IOContext.DEFAULT)) {
                out.writeInt(0); // the write and the close are where a direct output touches the device
            } catch (IOException | UnsupportedOperationException e) {
                // this and the "Creating" message are matched whole by the DirectIOIT expectations: keep the wording
                Log.debug(() -> Strings.format("Could not create %s with direct IO", name), e);
                return false;
            } finally {
                IOUtils.deleteFilesIgnoringExceptions(probePath);
            }
            mergeDirectIOCreatesWork = true;
            return true;
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(super::close, delegate);
        }

        private static String getExtension(String name) {
            // Unlike FileSwitchDirectory#getExtension, we treat `tmp` as a normal file extension, which can have its own rules for mmaping.
            final int lastDotIndex = name.lastIndexOf('.');
            if (lastDotIndex == -1) {
                return "";
            } else {
                return name.substring(lastDotIndex + 1);
            }
        }

        /** A temporary file holding vector data, which has no extension of its own to go by. */
        private static boolean isTemporaryVectorFile(String name, IOContext context) {
            return LuceneFilesExtensions.TMP.getExtension().equals(getExtension(name))
                && context.hints().contains(FileDataHint.KNN_VECTORS);
        }

        /**
         * Raw vector data, not the metadata written alongside it with the same context, which is far too small for direct I/O.
         * Compares the extension itself: files that are not Lucene's, such as recovery's temporary copies, pass through here.
         */
        static boolean isRawVectorFile(String name) {
            return LuceneFilesExtensions.VEC.getExtension().equals(getExtension(name));
        }

        static boolean useDelegate(String name, IOContext ioContext) {
            if (ioContext.hints().contains(Store.FileFooterOnly.INSTANCE)) {
                // If we're just reading the footer for the checksum then mmap() isn't really necessary, and it's desperately inefficient
                // if pre-loading is enabled on this file.
                return false;
            }

            final LuceneFilesExtensions extension = LuceneFilesExtensions.fromExtension(getExtension(name));
            if (extension == null || extension.shouldMmap() == false || avoidDelegateForFdtTempFiles(name, extension)) {
                // Other files are either less performance-sensitive (e.g. stored field index, norms metadata)
                // or are large and have a random access pattern and mmap leads to page cache trashing
                // (e.g. stored fields and term vectors).
                return false;
            }
            return true;
        }

        /**
         * Force not using mmap if file is a tmp fdt, disi or address-data file.
         * The tmp fdt file only gets created when flushing stored
         * fields to disk and index sorting is active.
         * <p>
         * In Lucene, the <code>SortingStoredFieldsConsumer</code> first
         * flushes stored fields to disk in tmp files in unsorted order and
         * uncompressed format. Then the tmp file gets a full integrity check,
         * then the stored values are read from the tmp in the order of
         * the index sorting in the segment, the order in which this happens
         * from the perspective of tmp fdt file is random. After that,
         * the tmp files are removed.
         * <p>
         * If the machine Elasticsearch runs on has sufficient memory the i/o pattern
         * that <code>SortingStoredFieldsConsumer</code> actually benefits from using mmap.
         * However, in cases when memory scarce, this pattern can cause page faults often.
         * Doing more harm than not using mmap.
         * <p>
         * As part of flushing stored disk when indexing sorting is active,
         * three tmp files are created, fdm (metadata), fdx (index) and
         * fdt (contains stored field data). The first two files are small and
         * mmap-ing that should still be ok even is memory is scarce.
         * The fdt file is large and tends to cause more page faults when memory is scarce.
         *
         * For disi, address-data, block-addresses, and block-doc-ranges files, in es819 tsdb doc values codec,
         * docids and offsets are first written to a tmp file and read and written into new segment.
         *
         * @param name      The name of the file in Lucene index
         * @param extension The extension of the in Lucene index
         * @return whether to avoid using delegate if the file is a tmp fdt file.
         */
        static boolean avoidDelegateForFdtTempFiles(String name, LuceneFilesExtensions extension) {
            return extension == LuceneFilesExtensions.TMP && NO_MMAP_FILE_SUFFIXES.stream().anyMatch(name::contains);
        }

        static final Set<String> NO_MMAP_FILE_SUFFIXES = Set.of("fdt", "disi", "address-data", "block-addresses", "block-doc-ranges");

        /**
         * Whether to read or write this file with direct I/O: only raw vectors of a field whose mapping asks for it. A merge
         * streams them with direct I/O ({@code on_disk_merge}), reading or writing, and refuses it when it reads them at random;
         * a search reads them at random with it ({@code on_disk_rescore}) only if they are not reused; a flush never uses it.
         * A merge's temp files only get it when they are not reused.
         */
        private boolean useDirectIO(String name, IOContext context) {
            // the raw vectors: the field's file, or the copy a merge keeps while it runs
            if (isRawVectorFile(name) == false && isTemporaryVectorFile(name, context) == false) {
                return false;
            }
            var field = context.hints(VectorFieldHint.class).findFirst().orElse(null);
            if (field == null) {
                // the file holds several fields, or its field is not known yet: no one mapping applies
                return false;
            }
            DenseVectorIndexOptions options = vectorIndexOptions(field.field());
            if (options == null) {
                return false;
            }
            boolean notReused = context.hints().contains(NoReuseHint.INSTANCE);
            return switch (context.context()) {
                case MERGE -> context.hints().contains(DataAccessHint.SEQUENTIAL)
                    && (notReused || isRawVectorFile(name))
                    && options.isOnDiskMerge();
                case DEFAULT -> notReused && context.hints().contains(DataAccessHint.RANDOM) && options.isOnDiskRescore();
                case FLUSH -> false;
            };
        }

        /** The index options the mapping currently gives {@code field}, or null if it is not a dense vector field. */
        // visible for testing
        DenseVectorIndexOptions vectorIndexOptions(String field) {
            return mappingLookup.get().getFieldType(field) instanceof DenseVectorFieldType vectorField
                ? vectorField.getIndexOptions()
                : null;
        }

        MMapDirectory getDelegate() {
            return delegate;
        }
    }

    public static final class AlwaysDirectIODirectory extends DirectIODirectory {
        // two pages, guaranteeing a single buffer can load all of an un-page-aligned 1024-dim float vector
        public static final int RANDOM_ACCESS_BUFFER_SIZE = 8192;

        private final int blockSize;
        private final int bufferSize;
        private final int asyncPrefetchLimit;

        public AlwaysDirectIODirectory(FSDirectory delegate, int bufferSize, long minBytesDirect, int asyncPrefetchLimit)
            throws IOException {
            super(delegate, bufferSize, minBytesDirect);
            blockSize = getBlockSize(delegate.getDirectory());
            this.bufferSize = bufferSize;
            this.asyncPrefetchLimit = asyncPrefetchLimit;
        }

        @Override
        protected boolean useDirectIO(String name, IOContext context, OptionalLong fileLength) {
            return true;
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            ensureOpen();
            if (asyncPrefetchLimit > 0) {
                return new AsyncDirectIOIndexInput(getDirectory().resolve(name), blockSize, bufferSize, asyncPrefetchLimit);
            } else {
                // no async prefetching: a plain direct-IO input at this instance's buffer size
                return super.openInput(name, context);
            }
        }
    }
}
