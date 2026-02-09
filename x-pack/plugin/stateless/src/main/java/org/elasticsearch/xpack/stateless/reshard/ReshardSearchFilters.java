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
            // See ES-13108 to track injecting the summary at each call site that must provide it.
            // In the meantime we default to not filtering if the summary is not provided. This
            // isn't always correct, hence the ticket. The end state should be to remove this check.
            return false;
        }

        // If the provided summary reports fewer shards than the current index metadata, then the request is stale and should
        // not be filtered. This is true even if there is no ongoing split, because the request may predate a split that
        // has since been completed and removed.
        // Most of the time this will return false, e.g., when no resharding is taking place.
        // It's expected to always return false when the shard is a target shard as well, because the request would not
        // have included it if it summary predated the target shard entering SPLIT.
        // It is also possible for the coordinator to see split before the shard itself because cluster state application is
        // asynchronous. In that case we should filter because the coordinator is including the split target.
        final var currentSummary = SplitShardCountSummary.forSearch(indexMetadata, shardId.id());
        if (summary.compareTo(currentSummary) > 0) {
            return true;
        } else if (summary.compareTo(currentSummary) < 0) {
            return false;
        }

        // But if the summaries are equal, that only means that we *may* have to filter.
        // * When no resharding is in progress, the summaries will usually match, but we have no need to filter.
        // * We do not want to filter source shards if their targets are not yet at SPLIT, since the source is still responsible
        // for the target's documents.
        // * We do not need to filter target shards that have moved to DONE, since they have already removed unowned documents.
        // XXX this may not be quite correct, since although DONE means that the data has been deleted, it doesn't necessarily
        // mean that the search shard has seen the delete. It needs to filter until it has. To fix with #5404.
        IndexReshardingMetadata reshardingMetadata = indexMetadata.getReshardingMetadata();
        if (reshardingMetadata == null) {
            // no resharding is in progress, so no need to filter
            return false;
        }
        assert reshardingMetadata.isSplit();

        final var split = reshardingMetadata.getSplit();
        boolean hasUnownedDocs = false;

        if (split.isSourceShard(shardId.id())
            && split.getSourceShardState(shardId.id()) != IndexReshardingState.Split.SourceShardState.DONE
            && split.allTargetStatesAtLeast(shardId.id(), IndexReshardingState.Split.TargetShardState.SPLIT)) {
            hasUnownedDocs = true;
        } else if (split.isTargetShard(shardId.id())
            // the target shard may still believe it is in handoff because it hasn't applied the latest cluster state yet
            && split.getTargetShardState(shardId.id()) != IndexReshardingState.Split.TargetShardState.DONE) {
                hasUnownedDocs = true;
            }

        return hasUnownedDocs;
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
