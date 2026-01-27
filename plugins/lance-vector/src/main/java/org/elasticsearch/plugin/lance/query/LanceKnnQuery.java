/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.lance.query;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.plugin.lance.profile.LanceTimingContext;
import org.elasticsearch.plugin.lance.profile.LanceTimer;
import org.elasticsearch.plugin.lance.storage.LanceDataset;
import org.elasticsearch.plugin.lance.storage.LanceDatasetConfig;
import org.elasticsearch.plugin.lance.storage.LanceDatasetRegistry;
import org.elasticsearch.search.vectors.QueryProfilerProvider;
import org.elasticsearch.search.profile.query.QueryProfiler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A minimal Lucene query that joins Lance candidates to Lucene documents by _id.
 * This is intentionally simple for Phase 1 and uses a fake dataset loader.
 * <p>
 * Implements QueryProfilerProvider to provide detailed timing information
 * when profiling is enabled via the profile=true search parameter.
 */
public class LanceKnnQuery extends Query implements QueryProfilerProvider {
    private static final Logger logger = LogManager.getLogger(LanceKnnQuery.class);

    private final String fieldName;
    private final String storageUri;
    private final float[] queryVector;
    private final int k;
    private final int numCandidates;
    private final String similarity;
    private final Query filter;
    private final int dims;
    private final String ossEndpoint;
    private final String ossAccessKeyId;
    private final String ossAccessKeySecret;

    // Thread-local timing context for profiling
    private final ThreadLocal<LanceTimingContext> timingContext = new ThreadLocal<>();

    public LanceKnnQuery(
        String fieldName,
        String storageUri,
        float[] queryVector,
        int k,
        int numCandidates,
        String similarity,
        Query filter,
        int dims,
        String ossEndpoint,
        String ossAccessKeyId,
        String ossAccessKeySecret
    ) {
        this.fieldName = Objects.requireNonNull(fieldName);
        this.storageUri = Objects.requireNonNull(storageUri);
        this.queryVector = Objects.requireNonNull(queryVector);
        this.k = k;
        this.numCandidates = numCandidates;
        this.similarity = similarity == null ? "cosine" : similarity;
        this.filter = filter;
        this.dims = dims;
        this.ossEndpoint = ossEndpoint;
        this.ossAccessKeyId = ossAccessKeyId;
        this.ossAccessKeySecret = ossAccessKeySecret;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        // Initialize timing context for this query
        LanceTimingContext context = LanceTimingContext.getOrCreate();
        timingContext.set(context);
        // Activate timing collection - this enables LanceTimer to record timings
        context.activate();

        // Use unified registry that automatically selects RealLanceDataset for .lance files
        // and FakeLanceDataset for JSON test files
        // For OSS URIs, include OSS configuration
        LanceDatasetConfig config;
        if (storageUri.startsWith("oss://") && ossEndpoint != null) {
            config = new LanceDatasetConfig("_id", "vector", dims, ossEndpoint, ossAccessKeyId, ossAccessKeySecret);
        } else {
            config = new LanceDatasetConfig("_id", "vector", dims, null, null, null);
        }

        LanceDataset dataset;
        List<LanceDataset.Candidate> candidates;
        try (var timer = new LanceTimer(LanceTimingContext.LanceTimingStage.REGISTRY_CACHE_LOOKUP)) {
            dataset = LanceDatasetRegistry.getOrLoad(storageUri, dims, config);
            candidates = dataset.search(queryVector, numCandidates, similarity);
        }

        logger.debug(
            "Lance KNN: dataset has {} dims, searched with numCandidates={}, got {} candidates",
            dataset.dims(),
            numCandidates,
            candidates.size()
        );
        // Store filter query - we'll create the weight per-leaf to handle ES's DFS phase
        // which may use a different IndexSearcher than the one passed here
        final Query filterQuery = this.filter;
        final float queryBoost = boost;
        final int topK = k;
        return new Weight(this) {
        @Override
        public Explanation explain(LeafReaderContext context, int doc) throws IOException {
            ScorerSupplier supplier = scorerSupplier(context);
            if (supplier == null) {
                return Explanation.noMatch("no matching docs");
            }
            Scorer scorer = supplier.get(1);
            int advanced = scorer.iterator().advance(doc);
            if (advanced == doc) {
                return Explanation.match(scorer.score(), "matched lance candidate");
            }
            return Explanation.noMatch("not in candidate set");
        }

        @Override
        public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
            // Create filter weight using an IndexSearcher from the context's top-level reader
            // This is necessary because ES's DFS phase may call scorerSupplier with a context
            // from a different top-level reader than the searcher passed to createWeight
            Weight filterWeight = null;
            if (filterQuery != null) {
                IndexSearcher contextSearcher = new IndexSearcher(context.parent);
                filterWeight = filterQuery.createWeight(contextSearcher, ScoreMode.COMPLETE_NO_SCORES, 1f);
            }
            Map<Integer, Float> docScores = buildDocScores(context, candidates, filterWeight, topK);
            if (docScores.isEmpty()) {
                return null;
            }
            List<Integer> docIds = new ArrayList<>(docScores.keySet());
            Collections.sort(docIds);

            return new ScorerSupplier() {
                @Override
                public Scorer get(long leadCost) throws IOException {
                    DocIdSetIterator it = new DocIdSetIterator() {
                        int idx = -1;

                        @Override
                        public int docID() {
                            if (idx < 0) {
                                return -1; // Not yet positioned
                            } else if (idx >= docIds.size()) {
                                return NO_MORE_DOCS;
                            }
                            return docIds.get(idx);
                        }

                        @Override
                        public int nextDoc() {
                            idx++;
                            if (idx >= docIds.size()) {
                                return NO_MORE_DOCS;
                            }
                            return docIds.get(idx);
                        }

                        @Override
                        public int advance(int target) {
                            while (idx + 1 < docIds.size() && docIds.get(idx + 1) < target) {
                                idx++;
                            }
                            if (idx + 1 < docIds.size()) {
                                idx++;
                                return docIds.get(idx);
                            }
                            return NO_MORE_DOCS;
                        }

                        @Override
                        public long cost() {
                            return docIds.size();
                        }
                    };
                    return new LanceScorer(it, docScores, queryBoost);
                }

                @Override
                public long cost() {
                    return docIds.size();
                }
            };
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
            return false;
        }
    };
    }

    private static class LanceScorer extends Scorer {
        private final DocIdSetIterator iterator;
        private final Map<Integer, Float> docScores;
        private final float boost;

        LanceScorer(DocIdSetIterator iterator, Map<Integer, Float> docScores, float boost) {
            this.iterator = iterator;
            this.docScores = docScores;
            this.boost = boost;
        }

        @Override
        public DocIdSetIterator iterator() {
            return iterator;
        }

        @Override
        public int docID() {
            return iterator.docID();
        }

        @Override
        public float score() {
            return docScores.getOrDefault(docID(), 0f) * boost;
        }

        @Override
        public float getMaxScore(int upTo) {
            return Float.POSITIVE_INFINITY;
        }
    }

    private static Map<Integer, Float> buildDocScores(
        LeafReaderContext context,
        List<LanceDataset.Candidate> candidates,
        Weight filterWeight,
        int k
    ) throws IOException {
        if (candidates.isEmpty()) {
            return Map.of();
        }
        var reader = context.reader();
        var terms = reader.terms(IdFieldMapper.NAME);
        if (terms == null) {
            return Map.of();
        }
        // Build filter bitset once if filter is present
        java.util.BitSet filterBitSet = null;
        if (filterWeight != null) {
            try (var timer = new LanceTimer(LanceTimingContext.LanceTimingStage.FILTER_PROCESSING)) {
                ScorerSupplier filterSupplier = filterWeight.scorerSupplier(context);
                if (filterSupplier != null) {
                    Scorer filterScorer = filterSupplier.get(1);
                    filterBitSet = new java.util.BitSet(reader.maxDoc());
                    DocIdSetIterator filterIter = filterScorer.iterator();
                    for (int doc = filterIter.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = filterIter.nextDoc()) {
                        filterBitSet.set(doc);
                    }
                } else {
                    // Filter matched no documents in this segment
                    return Map.of();
                }
            }
        }

        Map<Integer, Float> scores = new java.util.HashMap<>();
        try (var timer = new LanceTimer(LanceTimingContext.LanceTimingStage.ID_MATCHING)) {
            for (LanceDataset.Candidate c : candidates) {
                BytesRef encodedId = Uid.encodeId(c.id());
                Term term = new Term(IdFieldMapper.NAME, encodedId);
                PostingsEnum postings = reader.postings(term);
                if (postings == null) {
                    continue;
                }
                for (int doc = postings.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = postings.nextDoc()) {
                    if (filterBitSet != null && filterBitSet.get(doc) == false) {
                        continue;
                    }
                    scores.merge(doc, c.score(), Math::max);
                }
            }
        }

        // Keep only top k by score
        try (var timer = new LanceTimer(LanceTimingContext.LanceTimingStage.SCORE_AGGREGATION)) {
            if (scores.size() > k) {
                return scores.entrySet()
                    .stream()
                    .sorted(Map.Entry.<Integer, Float>comparingByValue().reversed())
                    .limit(k)
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            }
        }
        return scores;
    }

    @Override
    public String toString(String field) {
        return "LanceKnnQuery(" + fieldName + ", uri=" + storageUri + ")";
    }

    @Override
    public boolean equals(Object obj) {
        if (sameClassAs(obj) == false) {
            return false;
        }
        LanceKnnQuery other = (LanceKnnQuery) obj;
        return k == other.k
            && numCandidates == other.numCandidates
            && fieldName.equals(other.fieldName)
            && storageUri.equals(other.storageUri)
            && similarity.equals(other.similarity);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldName, storageUri, k, numCandidates, similarity);
    }

    @Override
    public void visit(QueryVisitor visitor) {
        visitor.visitLeaf(this);
    }

    /**
     * Get the timing breakdown for this query.
     * This method is called to retrieve timing information.
     *
     * @return Map containing timing breakdown, or null if profiling is not active
     */
    private Map<String, Object> getTimingBreakdown() {
        LanceTimingContext context = timingContext.get();
        if (context != null && context.isActive()) {
            Map<String, Object> timing = context.toDebugMap();
            // Log the timing breakdown for debugging
            logger.info("Lance kNN timing breakdown: {}", timing);
            return timing;
        }
        return null;
    }

    /**
     * Store the profiling information in the QueryProfiler.
     * This is called by Elasticsearch when profiling is enabled.
     * <p>
     * Note: This method logs the timing breakdown. Full Profile API integration
     * will be added in a future update to include timing in the profile response.
     *
     * @param queryProfiler the query profiler (not currently used for custom timing)
     */
    @Override
    public void profile(QueryProfiler queryProfiler) {
        Map<String, Object> timing = getTimingBreakdown();
        if (timing != null && timing.isEmpty() == false) {
            // Get the profile breakdown for this query and add Lance timing to debug info
            org.elasticsearch.search.profile.query.QueryProfileBreakdown breakdown =
                queryProfiler.getQueryBreakdown(this);
            if (breakdown != null) {
                // Inject Lance timing into the debug map using the new API
                breakdown.putAllDebugData(timing);
                logger.debug("Lance kNN profile timing: {}", timing);
            }
            // Clean up: deactivate and clear the timing context after profiling
            LanceTimingContext context = timingContext.get();
            if (context != null) {
                context.deactivate();
                context.clear();
            }
        }
    }
}
