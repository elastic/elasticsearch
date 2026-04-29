/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.reshard;

import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.replication.StaleRequestException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexReshardingMetadata;
import org.elasticsearch.cluster.metadata.IndexReshardingState;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.common.lucene.index.SequentialStoredFieldsLeafReader;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardSplittingQuery;
import org.elasticsearch.lucene.util.BitSets;
import org.elasticsearch.lucene.util.MatchAllBitSet;

import java.io.IOException;

public class ReshardSearchFilters {
    /**
     * Wraps the provided {@link DirectoryReader} to filter out documents that do not belong to a shard,
     * if the shard has been split but not yet cleaned up, and the coordinating node's summary indicates
     * that it is aware that a split shard is available for search.
     * For source shards, if the target is available but the coordinating node was not aware then this
     * reader will continue to supply the target's documents.
     * Target shards generally filter out documents belonging to the source, because it is assumed that
     * a request that includes the target must also include the source. But we do still want to turn off
     * filtering if the target has already moved to DONE state, because at that point it no longer contains
     * unowned documents and the filtering isn't free.
     * @param reader the reader to wrap
     * @param shardId the shard ID of the shard the reader belongs to
     * @param summary the view of the shard routing table provided by the coordinating node
     * @param indexMetadata the index metadata to check for resharding state
     * @param mapperService the mapper service to check for nested documents
     * @return a wrapped directory reader, or the original reader if no filtering is needed
     * @throws IOException if there is an error constructing the wrapped reader
     */
    // ES-13106 we'll likely need to add some caching here to avoid duplicate bitsets and query execution
    // across multiple searches.
    public static DirectoryReader maybeWrapDirectoryReader(
        DirectoryReader reader,
        ShardId shardId,
        SplitShardCountSummary summary,
        IndexMetadata indexMetadata,
        MapperService mapperService
    ) throws IOException {
        if (shouldFilter(summary, indexMetadata, shardId) == false) {
            return reader;
        }

        // this query returns the documents that are not owned by the given shard ID
        final var query = new ShardSplittingQuery(indexMetadata, shardId.id(), mapperService.hasNested());

        // and this filter returns the documents that do not match the query, i.e. the documents that are owned by the shard
        return new QueryFilterDirectoryReader(reader, query);
    }

    // visible for testing
    static boolean shouldFilter(SplitShardCountSummary summary, IndexMetadata indexMetadata, ShardId shardId) {
        if (summary.equals(SplitShardCountSummary.UNSET)) {
            /// See ES-13108 to track injecting the summary at each call site that must provide it.
            /// In the meantime we default to not filtering if the summary is not provided. This
            /// isn't always correct, hence the ticket. The end state should be to remove this check.
            return false;
        }

        if (summary.equals(SplitShardCountSummary.IRRELEVANT)) {
            // We were explicitly told that this operation won't be impacted by split, so we trust that decision and don't apply filters.
            return false;
        }

        var decision = summary.check(indexMetadata);
        return switch (decision) {
            /// If the provided summary is older, then the request was only sent to the source shard
            /// and therefore should not be filtered.
            /// However, the request can be so stale that we would not have enough data to serve it after cleaning up
            /// unowned data. In that case we have to fail the request.
            case OLDER -> {
                IndexReshardingMetadata reshardingMetadata = indexMetadata.getReshardingMetadata();
                assert reshardingMetadata != null;
                assert reshardingMetadata.isSplit();

                IndexReshardingState.Split split = reshardingMetadata.getSplit();

                // Having a non-current summary means not routing to the target shard so this is unexpected.
                assert split.isTargetShard(shardId.id()) == false : "Received a search request with stale summary on the search shard";

                if (split.sourceStateAtLeast(shardId.id(), IndexReshardingState.Split.SourceShardState.READY_FOR_CLEANUP)) {
                    /// The grace period to drain queued search requests has passed but we still received this stale search request.
                    /// We have to reject it since we are about to delete unowned data which
                    /// would make such requests impossible to fulfill (we simply won't have the data).
                    throw new StaleRequestException(shardId, summary);
                }

                /// Otherwise we are in the middle of a split and received a request that was not routed to the target shard.
                /// We should return the entirety of the source shard data.
                yield false;
            }
            case CURRENT -> {
                /// But if the summary is current, that only means that we *may* have to filter.
                /// * When no resharding is in progress, the summary should usually match, but we have no need to filter.
                /// * We do not need to filter shards that have moved to DONE, since they have already removed unowned documents.
                IndexReshardingMetadata reshardingMetadata = indexMetadata.getReshardingMetadata();
                if (reshardingMetadata == null) {
                    // This is a common case - the summary is current and there is no ongoing split, nothing to do.
                    yield false;
                }

                assert reshardingMetadata.isSplit();
                IndexReshardingState.Split split = reshardingMetadata.getSplit();

                if (split.isTargetShard(shardId.id())) {
                    /// We ensure that refresh happens between unowned data being deleted and target shard moving to DONE.
                    /// So at this point we know that there is no unowned data and we can skip filters as an optimization.
                    yield split.targetStateAtLeast(shardId.id(), IndexReshardingState.Split.TargetShardState.DONE) == false;
                } else {
                    /// Similarly since we ensure the refresh is done after deleting unowned data we can skip filtering
                    /// if the shard is DONE as an optimization.
                    yield split.sourceStateAtLeast(shardId.id(), IndexReshardingState.Split.SourceShardState.DONE) == false;
                }
            }
            case INVALID -> throw new StaleRequestException(shardId, summary);
        };
    }

    /**
     * A {@link FilterDirectoryReader} that filters out documents that match a provided query
     */
    static class QueryFilterDirectoryReader extends FilterDirectoryReader {
        private final Query query;

        QueryFilterDirectoryReader(DirectoryReader in, Query query) throws IOException {
            super(in, new SubReaderWrapper() {
                @Override
                public LeafReader wrap(LeafReader reader) {
                    return new QueryFilterLeafReader(reader, query);
                }
            });
            this.query = query;
        }

        @Override
        protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
            return new QueryFilterDirectoryReader(in, query);
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }
    }

    private static class QueryFilterLeafReader extends SequentialStoredFieldsLeafReader {
        private final Query query;

        private int numDocs = -1;
        private BitSet filteredDocs;

        protected QueryFilterLeafReader(LeafReader in, Query query) {
            super(in);
            this.query = query;
        }

        /**
         * Returns all documents that are not deleted and are owned by the current shard.
         * We need to recalculate this every time because `in.getLiveDocs()` can change when deletes are performed.
         */
        @Override
        public Bits getLiveDocs() {
            ensureFilteredDocumentsPresent();
            Bits actualLiveDocs = in.getLiveDocs();

            if (filteredDocs == null) {
                return actualLiveDocs;
            }

            if (filteredDocs instanceof MatchAllBitSet) {
                return new Bits.MatchNoBits(in.maxDoc());
            }

            var liveDocsBitsWithAllLiveCheck = actualLiveDocs == null ? new MatchAllBitSet(in.maxDoc()) : actualLiveDocs;
            return new FilterBits(liveDocsBitsWithAllLiveCheck, filteredDocs);
        }

        @Override
        public int numDocs() {
            ensureFilteredDocumentsPresent();
            return numDocs;
        }

        @Override
        public boolean hasDeletions() {
            // It is possible that there are unowned docs which we are going to present as deletes.
            return true;
        }

        @Override
        protected StoredFieldsReader doGetSequentialStoredFieldsReader(StoredFieldsReader reader) {
            return reader;
        }

        @Override
        public CacheHelper getCoreCacheHelper() {
            return in.getCoreCacheHelper();
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            // Not delegated since we change live docs.
            return null;
        }

        private void ensureFilteredDocumentsPresent() {
            if (numDocs == -1) {
                synchronized (this) {
                    if (numDocs == -1) {
                        try {
                            filteredDocs = queryFilteredDocs();
                            numDocs = calculateNumDocs(in, filteredDocs);
                        } catch (Exception e) {
                            throw new ElasticsearchException("Failed to execute filtered documents query", e);
                        }
                    }
                }
            }
        }

        // Returns a BitSet of documents that match the query, or null if no documents match.
        private BitSet queryFilteredDocs() throws IOException {
            final IndexReaderContext topLevelContext = ReaderUtil.getTopLevelContext(in.getContext());

            final IndexSearcher searcher = new IndexSearcher(topLevelContext);
            searcher.setQueryCache(null);

            final Query rewrittenQuery = searcher.rewrite(query);
            // TODO there is a possible optimization of checking for MatchAllDocsQuery which would mean that all documents are unowned.
            final Weight weight = searcher.createWeight(rewrittenQuery, ScoreMode.COMPLETE_NO_SCORES, 1f);
            final Scorer s = weight.scorer(in.getContext());
            if (s == null) {
                return null;
            } else {
                return BitSets.of(s.iterator(), in.maxDoc());
            }
        }

        private static int calculateNumDocs(LeafReader reader, BitSet unownedDocs) {
            final Bits liveDocs = reader.getLiveDocs();

            // No deleted documents are present, therefore number of documents is total minus unowned.
            if (liveDocs == null) {
                return reader.numDocs() - unownedDocs.cardinality();
            }

            if (unownedDocs instanceof MatchAllBitSet) {
                return 0;
            }

            int numDocs = 0;
            for (int i = 0; i < liveDocs.length(); i++) {
                if (liveDocs.get(i) && unownedDocs.get(i) == false) {
                    numDocs++;
                }
            }
            return numDocs;
        }
    }

    static class FilterBits implements Bits {
        private final Bits original;
        private final Bits filteredOut;

        FilterBits(Bits original, BitSet filteredOut) {
            this.original = original;
            this.filteredOut = filteredOut;
        }

        @Override
        public boolean get(int index) {
            return original.get(index) && (filteredOut.get(index) == false);
        }

        @Override
        public int length() {
            return original.length();
        }
    }
}
