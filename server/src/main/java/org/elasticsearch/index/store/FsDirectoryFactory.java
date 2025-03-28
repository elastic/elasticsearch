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
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.NativeFSLockFactory;
import org.apache.lucene.store.SimpleFSLockFactory;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.plugins.IndexStorePlugin;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BiPredicate;

public class FsDirectoryFactory implements IndexStorePlugin.DirectoryFactory {

    public static final String DIRECT_IO_KEY = "*DIRECTIO*";

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
                    return new HybridDirectory(lockFactory, setPreload(mMapDirectory, preLoadExtensions));
                } else {
                    return new KeyRemovalDirectory(primaryDirectory);
                }
            case MMAPFS:
                return new KeyRemovalDirectory(setPreload(new MMapDirectory(location, lockFactory), preLoadExtensions));
            case SIMPLEFS:
            case NIOFS:
                return new KeyRemovalDirectory(new NIOFSDirectory(location, lockFactory));
            default:
                throw new AssertionError("unexpected built-in store type [" + type + "]");
        }
    }

    /** Sets the preload, if any, on the given directory based on the extensions. Returns the same directory instance. */
    // visibility and extensibility for testing
    public MMapDirectory setPreload(MMapDirectory mMapDirectory, Set<String> preLoadExtensions) {
        mMapDirectory.setPreload(getPreloadFunc(preLoadExtensions));
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
            String newName = useDelegate(name, context);
            if (newName != null) {
                // we need to do these checks on the outer directory since the inner doesn't know about pending deletes
                ensureOpen();
                ensureCanRead(newName);
                // we switch the context here since mmap checks for the READONCE context by identity
                context = context == Store.READONCE_CHECKSUM ? IOContext.READONCE : context;
                // we only use the mmap to open inputs. Everything else is managed by the NIOFSDirectory otherwise
                // we might run into trouble with files that are pendingDelete in one directory but still
                // listed in listAll() from the other. We on the other hand don't want to list files from both dirs
                // and intersect for perf reasons.
                return delegate.openInput(newName, context);
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

        static String useDelegate(String name, IOContext ioContext) {
            if (name.contains(DIRECT_IO_KEY)) {
                return name.replace(DIRECT_IO_KEY, "");
            }

            if (ioContext == Store.READONCE_CHECKSUM) {
                // If we're just reading the footer for the checksum then mmap() isn't really necessary, and it's desperately inefficient
                // if pre-loading is enabled on this file.
                return null;
            }

            final LuceneFilesExtensions extension = LuceneFilesExtensions.fromExtension(getExtension(name));
            if (extension == null || extension.shouldMmap() == false) {
                // Other files are either less performance-sensitive (e.g. stored field index, norms metadata)
                // or are large and have a random access pattern and mmap leads to page cache trashing
                // (e.g. stored fields and term vectors).
                return null;
            }
            return name;
        }

        MMapDirectory getDelegate() {
            return delegate;
        }
    }

    private static class KeyRemovalDirectory extends Directory {

        private final Directory delegate;

        private KeyRemovalDirectory(Directory delegate) {
            this.delegate = delegate;
        }

        @Override
        public String[] listAll() throws IOException {
            return delegate.listAll();
        }

        @Override
        public void deleteFile(String name) throws IOException {
            assert name.contains(DIRECT_IO_KEY) == false;
            delegate.deleteFile(name);
        }

        @Override
        public long fileLength(String name) throws IOException {
            return delegate.fileLength(name.replace(DIRECT_IO_KEY, ""));
        }

        @Override
        public IndexOutput createOutput(String name, IOContext context) throws IOException {
            return delegate.createOutput(name.replace(DIRECT_IO_KEY, ""), context);
        }

        @Override
        public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
            return delegate.createTempOutput(prefix, suffix, context);
        }

        @Override
        public void sync(Collection<String> names) throws IOException {
            assert names.stream().noneMatch(s -> s.contains(DIRECT_IO_KEY));
            delegate.sync(names);
        }

        @Override
        public void syncMetaData() throws IOException {
            delegate.syncMetaData();
        }

        @Override
        public void rename(String source, String dest) throws IOException {
            assert source.contains(DIRECT_IO_KEY) == false;
            delegate.rename(source, dest);
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            return delegate.openInput(name.replace(DIRECT_IO_KEY, ""), context);
        }

        @Override
        public Lock obtainLock(String name) throws IOException {
            return delegate.obtainLock(name.replace(DIRECT_IO_KEY, ""));
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }

        @Override
        public Set<String> getPendingDeletions() throws IOException {
            return delegate.getPendingDeletions();
        }
    }
}
