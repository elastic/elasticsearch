/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.store.fs;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.NativeFSLockFactory;
import org.apache.lucene.store.NoLockFactory;
import org.apache.lucene.store.SimpleFSLockFactory;
import org.apache.lucene.store.bytebuffer.ByteBufferDirectory;
import org.elasticsearch.cache.memory.ByteBufferCache;
import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.lucene.store.SwitchDirectory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.IndexStore;
import org.elasticsearch.index.store.support.AbstractStore;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;

/**
 * @author kimchy (shay.banon)
 */
public abstract class FsStore extends AbstractStore {

    public static final boolean DEFAULT_SUGGEST_USE_COMPOUND_FILE = false;

    public FsStore(ShardId shardId, @IndexSettings Settings indexSettings, IndexStore indexStore) {
        super(shardId, indexSettings, indexStore);
    }

    @Override public void fullDelete() throws IOException {
        FileSystemUtils.deleteRecursively(fsDirectory().getDirectory());
        // if we are the last ones, delete also the actual index
        String[] list = fsDirectory().getDirectory().getParentFile().list();
        if (list == null || list.length == 0) {
            FileSystemUtils.deleteRecursively(fsDirectory().getDirectory().getParentFile());
        }
    }

    @Override protected void doRenameFile(String from, String to) throws IOException {
        File directory = fsDirectory().getDirectory();
        File old = new File(directory, from);
        File nu = new File(directory, to);
        if (nu.exists())
            if (!nu.delete())
                throw new IOException("Cannot delete " + nu);

        if (!old.exists()) {
            throw new FileNotFoundException("Can't rename from [" + from + "] to [" + to + "], from does not exists");
        }

        boolean renamed = false;
        for (int i = 0; i < 3; i++) {
            if (old.renameTo(nu)) {
                renamed = true;
                break;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new InterruptedIOException(e.getMessage());
            }
        }
        if (!renamed) {
            throw new IOException("Failed to rename, from [" + from + "], to [" + to + "]");
        }
    }

    public abstract FSDirectory fsDirectory();

    protected LockFactory buildLockFactory() throws IOException {
        String fsLock = componentSettings.get("fs_lock", "native");
        LockFactory lockFactory = new NoLockFactory();
        if (fsLock.equals("native")) {
            // TODO LUCENE MONITOR: this is not needed in next Lucene version
            lockFactory = new NativeFSLockFactory();
        } else if (fsLock.equals("simple")) {
            lockFactory = new SimpleFSLockFactory();
        }
        return lockFactory;
    }

    protected Tuple<SwitchDirectory, Boolean> buildSwitchDirectoryIfNeeded(Directory fsDirectory, ByteBufferCache byteBufferCache) {
        boolean cache = componentSettings.getAsBoolean("memory.enabled", false);
        if (!cache) {
            return null;
        }
        Directory memDir = new ByteBufferDirectory(byteBufferCache);
        // see http://lucene.apache.org/java/3_0_1/fileformats.html
        String[] primaryExtensions = componentSettings.getAsArray("memory.extensions", new String[]{"", "del", "gen"});
        if (primaryExtensions == null || primaryExtensions.length == 0) {
            return null;
        }
        Boolean forceUseCompound = null;
        for (String extension : primaryExtensions) {
            if (!("".equals(extension) || "del".equals(extension) || "gen".equals(extension))) {
                // caching internal CFS extension, don't use compound file extension
                forceUseCompound = false;
            }
        }

        return new Tuple<SwitchDirectory, Boolean>(new SwitchDirectory(ImmutableSet.copyOf(primaryExtensions), memDir, fsDirectory, true), forceUseCompound);
    }
}
