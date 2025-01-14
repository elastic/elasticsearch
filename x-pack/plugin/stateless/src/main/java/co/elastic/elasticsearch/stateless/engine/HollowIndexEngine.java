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

package co.elastic.elasticsearch.stateless.engine;

import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.ReadOnlyEngine;
import org.elasticsearch.index.translog.TranslogStats;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;

/**
 * An {@link org.elasticsearch.index.engine.Engine} implementation for hollow index shards, i.e. shards that can't process ingestion
 * until they are unhollowed and the engine is swapped with an {@link co.elastic.elasticsearch.stateless.engine.IndexEngine}.
 *
 * The main objection of the hollow index engine is to decrease the memory footprint of inactive (ingestion-less) indexing shards.
 */
public class HollowIndexEngine extends ReadOnlyEngine {

    private final StatelessCommitService statelessCommitService;

    private final SetOnce<Releasable> primaryPermitsRef = new SetOnce<>();

    public HollowIndexEngine(EngineConfig config, StatelessCommitService statelessCommitService) {
        super(config, null, new TranslogStats(), true, Function.identity(), true, true);
        this.statelessCommitService = statelessCommitService;
    }

    @Override
    protected DirectoryReader open(final IndexCommit commit) throws IOException {
        final Directory directory = commit.getDirectory();
        final List<IndexCommit> indexCommits = DirectoryReader.listCommits(directory);
        final IndexCommit indexCommit = indexCommits.get(indexCommits.size() - 1);
        return new DirectoryReader(directory, new LeafReader[0], null) {
            @Override
            protected DirectoryReader doOpenIfChanged() {
                return null;
            }

            @Override
            protected DirectoryReader doOpenIfChanged(IndexCommit commit) {
                return null;
            }

            @Override
            protected DirectoryReader doOpenIfChanged(IndexWriter writer, boolean applyAllDeletes) {
                return null;
            }

            @Override
            public long getVersion() {
                return 0;
            }

            @Override
            public boolean isCurrent() {
                return true;
            }

            @Override
            public IndexCommit getIndexCommit() {
                return indexCommit;
            }

            @Override
            protected void doClose() {}

            @Override
            public CacheHelper getReaderCacheHelper() {
                return null;
            }
        };
    }

    public StatelessCommitService getStatelessCommitService() {
        return statelessCommitService;
    }

    public void callRefreshListeners() {
        for (ReferenceManager.RefreshListener listener : engineConfig.getInternalRefreshListener()) {
            try {
                listener.beforeRefresh();
                listener.afterRefresh(true);
            } catch (IOException e) {
                throw new IllegalStateException("Unable to refresh hollow shard", e);
            }
        }
    }

    public void setPrimaryPermits(Releasable primaryPermits) {
        boolean notSetBefore = primaryPermitsRef.trySet(primaryPermits);
        assert notSetBefore : primaryPermitsRef;
    }

    public boolean arePrimaryPermitsHeld() {
        return primaryPermitsRef.get() != null;
    }

    // TODO ES-10253
    // This shouldn't be publicly exposed and is currently only used by integration testing for properly shutting down
    // hollow shards until `HollowIndexEngine` makes flushes a no-op
    void releasePrimaryPermits() {
        Releasables.close(primaryPermitsRef.get());
    }

    @Override
    protected void closeNoLock(String reason, CountDownLatch closedLatch) {
        releasePrimaryPermits();
        super.closeNoLock(reason, closedLatch);
    }
}
