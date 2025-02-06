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

import co.elastic.elasticsearch.stateless.commits.HollowShardsService;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.Directory;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.engine.ReadOnlyEngine;
import org.elasticsearch.index.translog.TranslogStats;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

/**
 * An {@link org.elasticsearch.index.engine.Engine} implementation for hollow index shards, i.e. shards that can't process ingestion
 * until they are unhollowed and the engine is swapped with an {@link co.elastic.elasticsearch.stateless.engine.IndexEngine}.
 *
 * The main objective of the hollow index engine is to decrease the memory footprint of inactive (ingestion-less) indexing shards.
 */
public class HollowIndexEngine extends ReadOnlyEngine {

    private final StatelessCommitService statelessCommitService;
    private final HollowShardsService hollowShardsService;

    public HollowIndexEngine(EngineConfig config, StatelessCommitService statelessCommitService, HollowShardsService hollowShardsService) {
        super(config, null, new TranslogStats(), true, Function.identity(), true, true);
        this.statelessCommitService = statelessCommitService;
        this.hollowShardsService = hollowShardsService;
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

    @Override
    public void prepareForEngineReset() throws IOException {
        hollowShardsService.ensureHollowShard(shardId, true, "hollow index engine requires the shard to be hollow");
        logger.debug(() -> "preparing to reset hollow index engine for shard " + shardId);
    }

    @Override
    public RefreshResult refresh(String source) {
        // The reader is opened at hollowing time once and is never refreshed internally.
        return new RefreshResult(false, config().getPrimaryTermSupplier().getAsLong(), getLastCommittedSegmentInfos().getGeneration());
    }

    @Override
    public void maybeRefresh(String source, ActionListener<RefreshResult> listener) throws EngineException {
        ActionListener.completeWith(listener, () -> refresh(source));
    }
}
