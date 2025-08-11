/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.store;

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
import org.apache.lucene.store.SimpleFSLockFactory;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.plugins.IndexStorePlugin;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

public class FsDirectoryFactory implements IndexStorePlugin.DirectoryFactory {

    private static final FeatureFlag MADV_RANDOM_FEATURE_FLAG = new FeatureFlag("madv_random");

    public static final Setting<LockFactory> INDEX_LOCK_FACTOR_SETTING = new Setting<>("index.store.fs.fs_lock", "native", (s) -> {
        return switch (s) {
            case "native" -> NativeFSLockFactory.INSTANCE;
            case "simple" -> SimpleFSLockFactory.INSTANCE;
            default -> throw new IllegalArgumentException("unrecognized [index.store.fs.fs_lock] \"" + s + "\": must be native or simple");
        }; // can we set on both - node and index level, some nodes might be running on NFS so they might need simple rather than native
    }, Property.IndexScope, Property.NodeScope);

    @Override
    public Directory newDirectory(IndexSettings indexSettings, ShardPath path) throws IOException {
        final Path location = path.resolveIndex();
        final LockFactory lockFactory = indexSettings.getValue(INDEX_LOCK_FACTOR_SETTING);
        Files.createDirectories(location);
        return newFSDirectory(location, lockFactory, indexSettings);
    }

    protected Directory newFSDirectory(Path location, LockFactory lockFactory, IndexSettings indexSettings) throws IOException {
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
                    Directory dir = new HybridDirectory(lockFactory, setPreload(mMapDirectory, lockFactory, preLoadExtensions));
                    if (MADV_RANDOM_FEATURE_FLAG.isEnabled() == false) {
                        dir = disableRandomAdvice(dir);
                    }
                    return dir;
                } else {
                    return primaryDirectory;
                }
            case MMAPFS:
                Directory dir = setPreload(new MMapDirectory(location, lockFactory), lockFactory, preLoadExtensions);
                if (MADV_RANDOM_FEATURE_FLAG.isEnabled() == false) {
                    dir = disableRandomAdvice(dir);
                }
                return dir;
            case SIMPLEFS:
            case NIOFS:
                return new NIOFSDirectory(location, lockFactory);
            default:
                throw new AssertionError("unexpected built-in store type [" + type + "]");
        }
    }

    public static MMapDirectory setPreload(MMapDirectory mMapDirectory, LockFactory lockFactory, Set<String> preLoadExtensions)
        throws IOException {
        assert mMapDirectory.getPreload() == false;
        if (preLoadExtensions.isEmpty() == false) {
            if (preLoadExtensions.contains("*")) {
                mMapDirectory.setPreload(true);
            } else {
                return new PreLoadMMapDirectory(mMapDirectory, lockFactory, preLoadExtensions);
            }
        }
        return mMapDirectory;
    }

    /**
     * Return a {@link FilterDirectory} around the provided {@link Directory} that forcefully disables {@link IOContext#RANDOM random
     * access}.
     */
    static Directory disableRandomAdvice(Directory dir) {
        return new FilterDirectory(dir) {
            @Override
            public IndexInput openInput(String name, IOContext context) throws IOException {
                if (context.randomAccess) {
                    context = IOContext.READ;
                }
                assert context.randomAccess == false;
                return super.openInput(name, context);
            }
        };
    }

    /**
     * Returns true iff the directory is a hybrid fs directory
     */
    public static boolean isHybridFs(Directory directory) {
        Directory unwrap = FilterDirectory.unwrap(directory);
        return unwrap instanceof HybridDirectory;
    }

    static final class HybridDirectory extends NIOFSDirectory {
        private final MMapDirectory delegate;

        HybridDirectory(LockFactory lockFactory, MMapDirectory delegate) throws IOException {
            super(delegate.getDirectory(), lockFactory);
            this.delegate = delegate;
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
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
            if (ioContext == Store.READONCE_CHECKSUM) {
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
         * Force not using mmap if file is tmp fdt file.
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
         * @param name      The name of the file in Lucene index
         * @param extension The extension of the in Lucene index
         * @return whether to avoid using delegate if the file is a tmp fdt file.
         */
        static boolean avoidDelegateForFdtTempFiles(String name, LuceneFilesExtensions extension) {
            return extension == LuceneFilesExtensions.TMP && name.contains("fdt");
        }

        MMapDirectory getDelegate() {
            return delegate;
        }
    }

    // TODO it would be nice to share code between PreLoadMMapDirectory and HybridDirectory but due to the nesting aspect of
    // directories here makes it tricky. It would be nice to allow MMAPDirectory to pre-load on a per IndexInput basis.
    static final class PreLoadMMapDirectory extends MMapDirectory {
        private final MMapDirectory delegate;
        private final Set<String> preloadExtensions;

        PreLoadMMapDirectory(MMapDirectory delegate, LockFactory lockFactory, Set<String> preload) throws IOException {
            super(delegate.getDirectory(), lockFactory);
            super.setPreload(false);
            this.delegate = delegate;
            this.delegate.setPreload(true);
            this.preloadExtensions = preload;
            assert getPreload() == false;
        }

        @Override
        public void setPreload(boolean preload) {
            throw new IllegalArgumentException("can't set preload on a preload-wrapper");
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            if (useDelegate(name)) {
                // we need to do these checks on the outer directory since the inner doesn't know about pending deletes
                ensureOpen();
                ensureCanRead(name);
                return delegate.openInput(name, context);
            }
            return super.openInput(name, context);
        }

        @Override
        public synchronized void close() throws IOException {
            IOUtils.close(super::close, delegate);
        }

        boolean useDelegate(String name) {
            final String extension = FileSwitchDirectory.getExtension(name);
            return preloadExtensions.contains(extension);
        }

        MMapDirectory getDelegate() {
            return delegate;
        }
    }
}
