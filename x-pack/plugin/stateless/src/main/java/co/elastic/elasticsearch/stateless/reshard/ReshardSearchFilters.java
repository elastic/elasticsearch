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

package co.elastic.elasticsearch.stateless.reshard;

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
     * if the shard has been split but not yet cleaned up.
     * @param reader the reader to wrap
     * @param shardId the shard ID of the shard the reader belongs to
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
        IndexMetadata indexMetadata,
        MapperService mapperService
    ) throws IOException {
        IndexReshardingMetadata reshardingMetadata = indexMetadata.getReshardingMetadata();
        if (reshardingMetadata == null) {
            return reader;
        }

        assert reshardingMetadata.isSplit();

        if (reshardingMetadata.getSplit().isTargetShard(shardId.id()) == false) {
            return reader;
        }

        IndexReshardingState.Split.TargetShardState state = reshardingMetadata.getSplit().getTargetShardState(shardId.id());
        if (state != IndexReshardingState.Split.TargetShardState.HANDOFF && state != IndexReshardingState.Split.TargetShardState.SPLIT) {
            return reader;
        }

        // this query returns the documents that are not owned by the given shard ID
        final var query = new ShardSplittingQuery(indexMetadata, shardId.id(), mapperService.hasNested());

        // and this filter returns the documents that do not match the query, i.e. the documents that are owned by the shard
        return new QueryFilterDirectoryReader(reader, query);
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
