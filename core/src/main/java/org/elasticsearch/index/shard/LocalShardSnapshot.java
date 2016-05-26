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

package org.elasticsearch.index.shard;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.NoLockFactory;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.recovery.RecoveryState;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 */
final class LocalShardSnapshot implements Closeable {
    private final IndexShard shard;
    private final Store store;
    private final IndexCommit indexCommit;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public LocalShardSnapshot(IndexShard shard) {
        this.shard = shard;
        store = shard.store();
        store.incRef();
        boolean success = false;
        try {
            indexCommit = shard.snapshotIndex(true);
            success = true;
        } finally {
            if (success == false) {
                store.decRef();
            }
        }
    }

    Index getIndex() {
        return shard.indexSettings().getIndex();
    }

    Directory getSnapshotDirectory() {
        /* this directory will not be used for anything else but reading / copying files to another directory
         * we prevent all write operations on this directory with UOE - nobody should close it either. */
        return new FilterDirectory(store.directory()) {
            @Override
            public String[] listAll() throws IOException {
                Collection<String> fileNames = indexCommit.getFileNames();
                final String[] fileNameArray = fileNames.toArray(new String[fileNames.size()]);
                return fileNameArray;
            }

            @Override
            public void deleteFile(String name) throws IOException {
                throw new UnsupportedOperationException("this directory is read-only");
            }

            @Override
            public void sync(Collection<String> names) throws IOException {
                throw new UnsupportedOperationException("this directory is read-only");
            }

            @Override
            public void renameFile(String source, String dest) throws IOException {
                throw new UnsupportedOperationException("this directory is read-only");
            }

            @Override
            public IndexOutput createOutput(String name, IOContext context) throws IOException {
                throw new UnsupportedOperationException("this directory is read-only");
            }

            @Override
            public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
                throw new UnsupportedOperationException("this directory is read-only");
            }

            @Override
            public Lock obtainLock(String name) throws IOException {
                    /* we do explicitly a no-lock instance since we hold an index commit from a SnapshotDeletionPolicy so we
                     * can we certain that nobody messes with the files on disk. We also hold a ref on the store which means
                     * no external source will delete files either.*/
                return NoLockFactory.INSTANCE.obtainLock(in, name);
            }

            @Override
            public void close() throws IOException {
                throw new UnsupportedOperationException("nobody should close this directory wrapper");
            }
        };
    }

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            try {
                shard.releaseSnapshot(indexCommit);
            } finally {
                store.decRef();
            }
        }
    }

    ImmutableOpenMap<String, MappingMetaData> getMappings() {
        return shard.indexSettings.getIndexMetaData().getMappings();
    }

    @Override
    public String toString() {
        return "local_shard_snapshot:[" + shard.shardId() + " indexCommit: " + indexCommit + "]";
    }
}
