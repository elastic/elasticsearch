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
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FileSwitchDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.NativeFSLockFactory;
import org.apache.lucene.store.ReadAdvice;
import org.apache.lucene.store.SimpleFSLockFactory;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.StandardIOBehaviorHint;
import org.elasticsearch.index.codec.vectors.es818.DirectIOHint;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.plugins.IndexStorePlugin;

import java.io.IOException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;

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
        final Path location = path.resolveIndex();
        final LockFactory lockFactory = indexSettings.getValue(INDEX_LOCK_FACTOR_SETTING);
        Files.createDirectories(location);
        return newFSDirectory(location, lockFactory, indexSettings);
    }

    protected Directory newFSDirectory(Path location, LockFactory lockFactory, IndexSettings indexSettings) throws IOException {
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
                    return new HybridDirectory(lockFactory, setMMapFunctions(mMapDirectory, preLoadExtensions), asyncPrefetchLimit);
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

    private static BiFunction<String, IOContext, Optional<ReadAdvice>> getReadAdviceFunc() {
        return (name, context) -> {
            if (context.hints().contains(StandardIOBehaviorHint.INSTANCE)) {
                return Optional.of(ReadAdvice.NORMAL);
            }
            if (name.endsWith(".cfs")) {
                return Optional.of(ReadAdvice.NORMAL);
            }
            return MMapDirectory.ADVISE_BY_CONTEXT.apply(name, context);
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

        public HybridDirectory(LockFactory lockFactory, MMapDirectory delegate, int asyncPrefetchLimit) throws IOException {
            super(delegate.getDirectory(), lockFactory);
            this.delegate = delegate;

            DirectIODirectory directIO;
            try {
                // use 8kB buffer (two pages) to guarantee it can load all of an un-page-aligned 1024-dim float vector
                directIO = new AlwaysDirectIODirectory(delegate, 8192, DirectIODirectory.DEFAULT_MIN_BYTES_DIRECT, asyncPrefetchLimit);
            } catch (Exception e) {
                // directio not supported
                Log.warn("Could not initialize DirectIO access", e);
                directIO = null;
            }
            this.directIODelegate = directIO;
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            Throwable directIOException = null;
            if (directIODelegate != null && context.hints().contains(DirectIOHint.INSTANCE)) {
                ensureOpen();
                ensureCanRead(name);
                try {
                    Log.debug("Opening {} with direct IO", name);
                    return directIODelegate.openInput(name, context);
                } catch (FileSystemException e) {
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

        MMapDirectory getDelegate() {
            return delegate;
        }
    }

    static final class AlwaysDirectIODirectory extends DirectIODirectory {
        private final int blockSize;
        private final int asyncPrefetchLimit;

        AlwaysDirectIODirectory(FSDirectory delegate, int mergeBufferSize, long minBytesDirect, int asyncPrefetchLimit) throws IOException {
            super(delegate, mergeBufferSize, minBytesDirect);
            blockSize = getBlockSize(delegate.getDirectory());
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
                return new AsyncDirectIOIndexInput(getDirectory().resolve(name), blockSize, 8192, asyncPrefetchLimit);
            } else {
                return super.openInput(name, context);
            }
        }
    }
}
