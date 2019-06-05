/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.store.SimpleFSLockFactory;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.core.internal.io.IOUtils;
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

    public static final Setting<LockFactory> INDEX_LOCK_FACTOR_SETTING = new Setting<>("index.store.fs.fs_lock", "native", (s) -> {
        switch (s) {
            case "native":
                return NativeFSLockFactory.INSTANCE;
            case "simple":
                return SimpleFSLockFactory.INSTANCE;
            default:
                throw new IllegalArgumentException("unrecognized [index.store.fs.fs_lock] \"" + s + "\": must be native or simple");
        } // can we set on both - node and index level, some nodes might be running on NFS so they might need simple rather than native
    }, Property.IndexScope, Property.NodeScope);


    @Override
    public Directory newDirectory(IndexSettings indexSettings, ShardPath path) throws IOException {
        final Path location = path.resolveIndex();
        final LockFactory lockFactory = indexSettings.getValue(INDEX_LOCK_FACTOR_SETTING);
        Files.createDirectories(location);
        return newFSDirectory(location, lockFactory, indexSettings);
    }

    protected Directory newFSDirectory(Path location, LockFactory lockFactory, IndexSettings indexSettings) throws IOException {
        final String storeType =
                indexSettings.getSettings().get(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), IndexModule.Type.FS.getSettingsKey());
        IndexModule.Type type;
        if (IndexModule.Type.FS.match(storeType)) {
            type = IndexModule.defaultStoreType(IndexModule.NODE_STORE_ALLOW_MMAP.get(indexSettings.getNodeSettings()));
        } else {
            type = IndexModule.Type.fromSettingsKey(storeType);
        }
        Set<String> preLoadExtensions = new HashSet<>(
            indexSettings.getValue(IndexModule.INDEX_STORE_PRE_LOAD_SETTING));
        switch (type) {
            case HYBRIDFS:
                // Use Lucene defaults
                final FSDirectory primaryDirectory = FSDirectory.open(location, lockFactory);
                if (primaryDirectory instanceof MMapDirectory) {
                    MMapDirectory mMapDirectory = (MMapDirectory) primaryDirectory;
                    return new HybridDirectory(setPreload(mMapDirectory, lockFactory, preLoadExtensions), lockFactory);
                } else {
                    return primaryDirectory;
                }
            case MMAPFS:
                return setPreload(new MMapDirectory(location, lockFactory), lockFactory, preLoadExtensions);
            case SIMPLEFS:
                return new SimpleFSDirectory(location, lockFactory);
            case NIOFS:
                return new NIOFSDirectory(location, lockFactory);
            default:
                throw new AssertionError("unexpected built-in store type [" + type + "]");
        }
    }

    private static FSDirectory setPreload(MMapDirectory mMapDirectory, LockFactory lockFactory,
            Set<String> preLoadExtensions) throws IOException {
        if (preLoadExtensions.isEmpty() == false
                && mMapDirectory.getPreload() == false) {
            if (preLoadExtensions.contains("*")) {
                mMapDirectory.setPreload(true);
                return mMapDirectory;
            }
            return new PreLoadMMapDirectory(mMapDirectory, lockFactory, preLoadExtensions);
        }
        return mMapDirectory;
    }

    /**
     * Returns true iff the directory is a hybrid fs directory
     */
    public static boolean isHybridFs(Directory directory) {
        Directory unwrap = FilterDirectory.unwrap(directory);
        return unwrap instanceof HybridDirectory;
    }

    static abstract class ReadOnlyFileSwitchMMapDirectory<D extends FSDirectory> extends FSDirectory {
        protected final FSDirectory delegate;
        protected final D primary;

        ReadOnlyFileSwitchMMapDirectory(D primary, FSDirectory delegate, LockFactory lockFactory) throws IOException {
            super(primary.getDirectory(), lockFactory);
            this.primary = primary;
            this.delegate = delegate;
            assert delegate.getDirectory().equals(primary.getDirectory());
        }

        @Override
        public final IndexInput openInput(String name, IOContext context) throws IOException {
            String extension = FileSwitchDirectory.getExtension(name);
            // we need to do these checks on the outer directory since the inner doesn't know about pending deletes
            ensureOpen();
            ensureCanRead(name);
            if (useDelegate(extension)) {
                // we only use the delegate to open inputs. Everything else is managed by this directory otherwise
                // we might run into trouble with files that are pendingDelete in one directory but still
                // listed in listAll() from the other. We on the other hand don't want to list files from both dirs
                // and intersect for perf reasons.
                return delegate.openInput(name, context);
            } else {
                return primary.openInput(name, context);
            }
        }

        protected abstract boolean useDelegate(String extension);

        @Override
        public synchronized void close() throws IOException {
            IOUtils.close(super::close, primary, delegate);
        }

        FSDirectory getDelegate() {
            return delegate;
        }

        D getPrimary() {
            return primary;
        }
    }

    static final class HybridDirectory extends ReadOnlyFileSwitchMMapDirectory<NIOFSDirectory> {

        HybridDirectory(FSDirectory delegate, LockFactory lockFactory) throws IOException {
            super(new NIOFSDirectory(delegate.getDirectory(), lockFactory), delegate, lockFactory);
        }

        @Override
        protected boolean useDelegate(String extension) {
            switch(extension) {
                // We are mmapping norms, docvalues as well as term dictionaries, all other files are served through NIOFS
                // this provides good random access performance and does not lead to page cache thrashing.
                case "nvd":
                case "dvd":
                case "tim":
                case "cfs":
                   return true;
                default:
                    return false;
            }
        }
    }

    static final class PreLoadMMapDirectory extends ReadOnlyFileSwitchMMapDirectory<MMapDirectory> {
        private final Set<String> preloadExtensions;

        public PreLoadMMapDirectory(MMapDirectory primary, LockFactory lockFactory, Set<String> preload) throws IOException {
            super(primary, buildDelegate(primary, lockFactory), lockFactory);
            primary.setPreload(false);
            this.preloadExtensions = preload;
        }

        private static MMapDirectory buildDelegate(MMapDirectory primary, LockFactory lockFactory) throws IOException {
            MMapDirectory mMapDirectory = new MMapDirectory(primary.getDirectory(), lockFactory);
            mMapDirectory.setPreload(true);
            return mMapDirectory;
        }

        @Override
        protected boolean useDelegate(String extension) {
            return preloadExtensions.contains(extension);
        }
    }
}
