/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.shard;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.NoLockFactory;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.store.Store;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Set;

final class LocalShardSnapshot implements Closeable {
    private final IndexShard shard;
    private final Store store;
    private final Engine.IndexCommitRef indexCommit;

    LocalShardSnapshot(IndexShard shard) {
        this.shard = shard;
        this.store = shard.store();
        this.indexCommit = shard.acquireLastIndexCommit(true);
    }

    Index getIndex() {
        return shard.indexSettings().getIndex();
    }

    long maxSeqNo() {
        return shard.getEngine().getSeqNoStats(-1).getMaxSeqNo();
    }

    long maxUnsafeAutoIdTimestamp() {
        return Long.parseLong(shard.getEngine().commitStats().getUserData().get(Engine.MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID));
    }

    Directory getSnapshotDirectory() {
        /* this directory will not be used for anything else but reading / copying files to another directory
         * we prevent all write operations on this directory with UOE - nobody should close it either. */
        return new FilterDirectory(store.directory()) {
            @Override
            public String[] listAll() throws IOException {
                Collection<String> fileNames = indexCommit.getIndexCommit().getFileNames();
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
            public void rename(String source, String dest) throws IOException {
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

            // temporary override until LUCENE-8735 is integrated
            @Override
            public Set<String> getPendingDeletions() throws IOException {
                return in.getPendingDeletions();
            }
        };
    }

    @Override
    public void close() throws IOException {
        indexCommit.close();
    }

    IndexMetadata getIndexMetadata() {
        return shard.indexSettings.getIndexMetadata();
    }

    @Override
    public String toString() {
        return "local_shard_snapshot:[" + shard.shardId() + " indexCommit: " + indexCommit + "]";
    }
}
