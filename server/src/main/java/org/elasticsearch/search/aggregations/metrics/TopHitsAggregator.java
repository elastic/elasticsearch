/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.metrics;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MultiCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopFieldCollectorManager;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.TopScoreDocCollectorManager;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.MaxScoreCollector;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.TopDocsAndMaxScore;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongObjectPagedHashMap;
import org.elasticsearch.common.util.LongObjectPagedHashMap.Cursor;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.subphase.InnerHitsContext;
import org.elasticsearch.search.fetch.subphase.InnerHitsContext.InnerHitSubContext;
import org.elasticsearch.search.internal.SubSearchContext;
import org.elasticsearch.search.profile.ProfileResult;
import org.elasticsearch.search.rescore.RescoreContext;
import org.elasticsearch.search.sort.SortAndFormats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.IntConsumer;

class TopHitsAggregator extends MetricsAggregator {

    static class Collectors {
        public final TopDocsCollector<?> topDocsCollector;
        public final MaxScoreCollector maxScoreCollector;
        public final Collector collector;

        Collectors(TopDocsCollector<?> topDocsCollector, MaxScoreCollector maxScoreCollector) {
            this.topDocsCollector = topDocsCollector;
            this.maxScoreCollector = maxScoreCollector;
            collector = MultiCollector.wrap(topDocsCollector, maxScoreCollector);
        }
    }

    /**
     * Builds the Lucene collectors of a bucket. It is package-private so tests can measure the estimates against what the aggregator
     * really builds.
     */
    static Collectors newCollectors(@Nullable SortAndFormats sort, int topN, boolean trackScores) {
        if (sort == null) {
            return new Collectors(new TopScoreDocCollectorManager(topN, null, Integer.MAX_VALUE).newCollector(), null);
        }
        // TODO: can we pass trackTotalHits=subSearchContext.trackTotalHits(){
        // Note that this would require to catch CollectionTerminatedException
        return new Collectors(
            new TopFieldCollectorManager(sort.sort, topN, null, Integer.MAX_VALUE).newCollector(),
            trackScores ? new MaxScoreCollector() : null
        );
    }

    /*
     * Estimates of the heap a bucket's Lucene collectors hold. BigArrays does not track it, so these are all the request breaker sees.
     * The collectors were measured with their queues full, and the leaf collectors in heap dumps.
     */
    /** A {@code Collectors} with a {@link TopScoreDocCollector}, without its queue. */
    static final long SCORE_COLLECTOR_BYTES = 136;
    /** A slot of the queue of a {@link TopScoreDocCollector}, which is allocated for every hit up front. */
    static final long SCORE_SLOT_BYTES = Long.BYTES;
    /** A {@code Collectors} with a {@link TopFieldCollector} and its queue, without its comparators and slots. */
    static final long FIELD_COLLECTOR_BYTES = 344;
    /** The comparator of one sort field, without its slots. */
    static final long FIELD_COMPARATOR_BYTES = 192;
    /** A slot of the queue of a {@link TopFieldCollector}, with the entry it holds once filled. */
    static final long FIELD_QUEUE_SLOT_BYTES = 40;
    /**
     * One slot of a sort field's comparator once filled. A keyword comparator copies the term and measured 88 bytes for a 10 byte term,
     * but about 360 bytes for a 256 byte term, so long terms are under-charged. A numeric one measured 8 bytes. Both are charged this,
     * which over-charges the few numeric sorts that get here.
     */
    static final long FIELD_COMPARATOR_SLOT_BYTES = 96;
    /** What {@code track_scores} adds to a field sort: a {@link MaxScoreCollector} and the {@link MultiCollector} around both. */
    static final long TRACK_SCORES_COLLECTOR_BYTES = 72;
    /** What {@code track_scores} adds to a leaf collector: the leaf collector of the {@link MultiCollector}. */
    static final long TRACK_SCORES_LEAF_COLLECTOR_BYTES = 48;
    /** A leaf collector that does not read doc values, which measured 56 bytes. */
    static final long LEAF_COLLECTOR_BYTES = 64;
    /**
     * What the leaf comparator of a field adds to a leaf collector: its own doc values reader with its input clones and buffers. For a
     * keyword field it measured 8 KB with the time series doc values format and 6.7 KB with the default one, and for a long field about
     * 5 KB with the time series format.
     */
    static final long LEAF_DOC_VALUES_BYTES = 8 * 1024;
    /** Charges for the collectors are sent to the breaker once at least this many bytes are pending, not once per bucket. */
    static final long CHARGE_BATCH_BYTES = 16 * 1024;

    private final BigArrays bigArrays;
    private final SubSearchContext subSearchContext;
    /**
     * Collects the top documents of every bucket without a Lucene collector per bucket, or {@code null} for the sorts it does
     * not support, in which case {@link #topDocsCollectors} and {@link #leafCollectors} are used.
     */
    @Nullable
    private final BucketedTopHits bucketedTopHits;
    private final LongObjectPagedHashMap<Collectors> topDocsCollectors;
    private final List<ProfileResult> fetchProfiles;
    // this must be mutable so it can be closed/replaced on each call to getLeafCollector, and closed when collection is done
    private LongObjectPagedHashMap<LeafCollector> leafCollectors;
    private final boolean isNested;
    private final SearchExecutionContext forkedSearchExecutionContext;
    private final InnerHitsContext forkedInnerHitsContext;
    /** The estimated bytes of a bucket's {@link Collectors}, charged when it is created and held until the aggregator closes. */
    private final long collectorBytes;
    /**
     * The estimated bytes of a bucket's leaf collector, charged when it is created and released when the next segment starts or
     * collection is done.
     */
    private final long leafCollectorBytes;
    /** Estimated bytes not yet sent to the request breaker. */
    private long pendingBytes;
    /** The estimated bytes of the leaf collectors of the segment being collected. */
    private long leafCollectorsBytes;

    TopHitsAggregator(
        SubSearchContext subSearchContext,
        String name,
        AggregationContext context,
        Aggregator parent,
        Map<String, Object> metadata,
        @Nullable BucketedTopHits bucketedTopHits
    ) throws IOException {
        super(name, context, parent, metadata);
        this.bigArrays = context.bigArrays();
        this.subSearchContext = subSearchContext;
        this.bucketedTopHits = bucketedTopHits;
        this.topDocsCollectors = bucketedTopHits == null ? new LongObjectPagedHashMap<>(1, bigArrays) : null;
        this.fetchProfiles = context.profiling() ? new ArrayList<>() : null;
        this.isNested = isNestedAggregationContext(parent);
        // Fork the search execution context, because the fetch phase does not support concurrent execution yet. One fork per
        // aggregator is enough: each slice gets its own aggregator and its buckets are fetched sequentially. Forking per bucket
        // instead pinned a SearchExecutionContext, and the stored fields its lookup provider had loaded, for every bucket until
        // the query context closed.
        this.forkedSearchExecutionContext = new SearchExecutionContext(subSearchContext.getSearchExecutionContext());
        // When top_hits aggregation is inside a nested aggregation, do not inherit inner_hits from parent query.
        // Query-level inner_hits operate at parent document scope, while nested aggregation top_hits operate
        // at nested document scope within buckets. Inheriting inner_hits causes conflicting fetch contexts
        // and fails when extracting nested documents by offset.
        if (isNested) {
            this.forkedInnerHitsContext = new InnerHitsContext();
        } else {
            // InnerHitSubContext is not thread-safe, so we fork it as well to support concurrent execution
            this.forkedInnerHitsContext = new InnerHitsContext(
                getForkedInnerHits(subSearchContext.innerHits().getInnerHits(), forkedSearchExecutionContext)
            );
        }
        if (bucketedTopHits == null) {
            this.collectorBytes = collectorBytes(subSearchContext.sort(), topN(), subSearchContext.trackScores());
            this.leafCollectorBytes = leafCollectorBytes(subSearchContext.sort(), subSearchContext.trackScores());
        } else {
            this.collectorBytes = 0;
            this.leafCollectorBytes = 0;
        }
    }

    /**
     * The number of hits each bucket's Lucene collector keeps.
     */
    private int topN() {
        int topN = subSearchContext.from() + subSearchContext.size();
        if (subSearchContext.sort() == null) {
            for (RescoreContext rescoreContext : subSearchContext.rescore()) {
                topN = Math.max(rescoreContext.getWindowSize(), topN);
            }
        }
        // In the QueryPhase we don't need this protection, because it is build into the IndexSearcher,
        // but here we create collectors ourselves and we need prevent OOM because of crazy an offset and size.
        return Math.min(topN, subSearchContext.searcher().getIndexReader().maxDoc());
    }

    /**
     * Estimates what a bucket's {@link Collectors} hold for the life of the aggregator: the collector, its comparators, and a slot in
     * its queue and in each comparator for every hit.
     */
    static long collectorBytes(@Nullable SortAndFormats sort, int topN, boolean trackScores) {
        if (sort == null) {
            return SCORE_COLLECTOR_BYTES + topN * SCORE_SLOT_BYTES;
        }
        int sortFields = sort.sort.getSort().length;
        long perHit = FIELD_QUEUE_SLOT_BYTES + sortFields * FIELD_COMPARATOR_SLOT_BYTES;
        long bytes = FIELD_COLLECTOR_BYTES + sortFields * FIELD_COMPARATOR_BYTES + topN * perHit;
        return trackScores ? bytes + TRACK_SCORES_COLLECTOR_BYTES : bytes;
    }

    /**
     * Estimates what a bucket's leaf collector holds while its segment is collected. Each leaf comparator of a field opens its own
     * doc values reader, so a leaf collector that sorts on fields holds a reader per field. Sorting on the score or the doc id reads
     * no doc values.
     */
    static long leafCollectorBytes(@Nullable SortAndFormats sort, boolean trackScores) {
        long bytes = LEAF_COLLECTOR_BYTES;
        if (sort != null) {
            if (trackScores) {
                bytes += TRACK_SCORES_LEAF_COLLECTOR_BYTES;
            }
            for (SortField sortField : sort.sort.getSort()) {
                if (sortField.getType() != SortField.Type.SCORE && sortField.getType() != SortField.Type.DOC) {
                    bytes += LEAF_DOC_VALUES_BYTES;
                }
            }
        }
        return bytes;
    }

    /**
     * Adds to the estimated bytes, which are sent to the request breaker in batches. Negative bytes are released once the bytes
     * still pending cannot cover them.
     */
    private void charge(long bytes) {
        pendingBytes += bytes;
        if (pendingBytes >= CHARGE_BATCH_BYTES || pendingBytes < 0) {
            addRequestCircuitBreakerBytes(pendingBytes);
            pendingBytes = 0;
        }
    }

    boolean usesBucketedSort() {
        return bucketedTopHits != null;
    }

    @Override
    public ScoreMode scoreMode() {
        SortAndFormats sort = subSearchContext.sort();
        if (sort != null) {
            return sort.sort.needsScores() || subSearchContext.trackScores() ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
        } else {
            // sort by score
            return ScoreMode.COMPLETE;
        }
    }

    @Override
    public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, LeafBucketCollector sub) throws IOException {
        if (usesBucketedSort()) {
            return bucketedTopHits.getLeafCollector(aggCtx, sub);
        }
        // Releases the leaf collectors of the previous segment. Those of the last segment are released when collection is done.
        releaseLeafCollectors();
        // Leaf collectors are created per leaf and not on the aggregator. A replay during post-collection pushes the doc ids of an
        // earlier segment, when an aggregator-level reader would already point at the next one, which trips assertions or gives
        // wrong top docs.
        leafCollectors = new LongObjectPagedHashMap<>(1, bigArrays);
        return new LeafBucketCollectorBase(sub, null) {

            // use the same Scorable for the leaf collectors
            ResettableScorable scorer = null;

            @Override
            public void setScorer(Scorable scorer) throws IOException {
                if (this.scorer != null) {
                    this.scorer.reset(scorer);
                    super.setScorer(scorer);
                } else {
                    this.scorer = new ResettableScorable(scorer);
                    super.setScorer(scorer);
                    for (Cursor<LeafCollector> leafCollector : leafCollectors) {
                        leafCollector.value.setScorer(scorer);
                    }
                }
            }

            @Override
            public void collect(int docId, long bucket) throws IOException {
                Collectors collectors = topDocsCollectors.get(bucket);
                if (collectors == null) {
                    charge(collectorBytes);
                    collectors = newCollectors(subSearchContext.sort(), topN(), subSearchContext.trackScores());
                    topDocsCollectors.put(bucket, collectors);
                }

                LeafCollector leafCollector = leafCollectors.get(bucket);
                if (leafCollector == null) {
                    charge(leafCollectorBytes);
                    leafCollectorsBytes += leafCollectorBytes;
                    leafCollector = collectors.collector.getLeafCollector(aggCtx.getLeafReaderContext());
                    if (scorer != null) {
                        leafCollector.setScorer(scorer);
                    }
                    leafCollectors.put(bucket, leafCollector);
                }
                leafCollector.collect(docId);
            }
        };
    }

    /**
     * Releases the leaf collectors of the last segment before the hits of every bucket are fetched. A parent that replays the
     * documents of its buckets, like a breadth first {@code terms}, post-collects this aggregator after the replay.
     */
    @Override
    protected void doPostCollection() {
        releaseLeafCollectors();
    }

    /** Closes the leaf collectors of the segment collected last, and releases their charge. */
    private void releaseLeafCollectors() {
        if (leafCollectors != null) {
            leafCollectors.close();
            leafCollectors = null; // set to null, so doClose does not close it again
        }
        charge(-leafCollectorsBytes);
        leafCollectorsBytes = 0;
    }

    private TopDocsAndMaxScore luceneTopDocs(long owningBucketOrdinal) throws IOException {
        Collectors collectors = topDocsCollectors.get(owningBucketOrdinal);
        if (collectors == null) {
            return null;
        }
        TopDocsCollector<?> topDocsCollector = collectors.topDocsCollector;
        TopDocs topDocs = topDocsCollector.topDocs();
        float maxScore = Float.NaN;
        if (subSearchContext.sort() == null) {
            for (RescoreContext ctx : subSearchContext.rescore()) {
                try {
                    topDocs = ctx.rescorer().rescore(topDocs, searcher(), ctx);
                } catch (IOException e) {
                    throw new ElasticsearchException("Rescore TopHits Failed", e);
                }
            }
            if (topDocs.scoreDocs.length > 0) {
                maxScore = topDocs.scoreDocs[0].score;
            }
        } else if (subSearchContext.trackScores()) {
            TopFieldCollector.populateScores(topDocs.scoreDocs, subSearchContext.searcher(), subSearchContext.query());
            maxScore = collectors.maxScoreCollector.getMaxScore();
        }
        return new TopDocsAndMaxScore(topDocs, maxScore);
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) throws IOException {
        final TopDocsAndMaxScore topDocsAndMaxScore = usesBucketedSort()
            ? bucketedTopHits.topDocs(owningBucketOrdinal, subSearchContext)
            : luceneTopDocs(owningBucketOrdinal);
        if (topDocsAndMaxScore == null) {
            return buildEmptyAggregation();
        }
        final TopDocs topDocs = topDocsAndMaxScore.topDocs;
        subSearchContext.queryResult()
            .topDocs(topDocsAndMaxScore, subSearchContext.sort() == null ? null : subSearchContext.sort().formats);
        int[] docIdsToLoad = new int[topDocs.scoreDocs.length];
        for (int i = 0; i < topDocs.scoreDocs.length; i++) {
            docIdsToLoad[i] = topDocs.scoreDocs[i].doc;
        }
        FetchSearchResult fetchResult = runFetchPhase(docIdsToLoad, this::addRequestCircuitBreakerBytes);
        if (fetchProfiles != null) {
            fetchProfiles.add(fetchResult.profileResult());
        }
        subSearchContext.registerTopHitsForRelease(fetchResult.hits());
        SearchHit[] internalHits = fetchResult.fetchResult().hits().getHits();
        for (int i = 0; i < internalHits.length; i++) {
            ScoreDoc scoreDoc = topDocs.scoreDocs[i];
            SearchHit searchHitFields = internalHits[i];
            searchHitFields.shard(subSearchContext.shardTarget());
            searchHitFields.score(scoreDoc.score);
            if (scoreDoc instanceof FieldDoc fieldDoc) {
                searchHitFields.sortValues(fieldDoc.fields, subSearchContext.sort().formats);
            }
        }
        return new InternalTopHits(
            name,
            subSearchContext.from(),
            subSearchContext.size(),
            topDocsAndMaxScore,
            fetchResult.hits(),
            metadata()
        );
    }

    private FetchSearchResult runFetchPhase(int[] docIdsToLoad, IntConsumer memoryChecker) {
        // Stays per bucket: it owns the FetchSearchResult the bucket's hits are published through.
        SubSearchContext fetchSubSearchContext = new SubSearchContext(subSearchContext) {
            @Override
            public SearchExecutionContext getSearchExecutionContext() {
                return forkedSearchExecutionContext;
            }

            @Override
            public InnerHitsContext innerHits() {
                return forkedInnerHitsContext;
            }
        };

        fetchSubSearchContext.fetchPhase().execute(fetchSubSearchContext, docIdsToLoad, null, memoryChecker);
        return fetchSubSearchContext.fetchResult();
    }

    /**
     * Check if this top_hits aggregator is a child of a nested aggregator.
     */
    private static boolean isNestedAggregationContext(Aggregator parent) {
        Aggregator current = parent;
        while (current != null) {
            if (current instanceof org.elasticsearch.search.aggregations.bucket.nested.NestedAggregator) {
                return true;
            }
            current = current.parent();
        }
        return false;
    }

    private static Map<String, InnerHitSubContext> getForkedInnerHits(
        Map<String, InnerHitSubContext> originalInnerHits,
        SearchExecutionContext searchExecutionContext
    ) {
        Map<String, InnerHitSubContext> forkedInnerHits = new HashMap<>();
        for (Map.Entry<String, InnerHitSubContext> entry : originalInnerHits.entrySet()) {
            var forkedContext = entry.getValue().copyWithSearchExecutionContext(searchExecutionContext);
            forkedInnerHits.put(entry.getKey(), forkedContext);
        }

        return forkedInnerHits;
    }

    @Override
    public InternalTopHits buildEmptyAggregation() {
        TopDocs topDocs;
        if (subSearchContext.sort() != null) {
            topDocs = new TopFieldDocs(Lucene.TOTAL_HITS_EQUAL_TO_ZERO, new FieldDoc[0], subSearchContext.sort().sort.getSort());
        } else {
            topDocs = Lucene.EMPTY_TOP_DOCS;
        }
        return new InternalTopHits(
            name,
            subSearchContext.from(),
            subSearchContext.size(),
            new TopDocsAndMaxScore(topDocs, Float.NaN),
            SearchHits.EMPTY_WITH_TOTAL_HITS,
            metadata()
        );
    }

    @Override
    public void collectDebugInfo(BiConsumer<String, Object> add) {
        super.collectDebugInfo(add);
        if (fetchProfiles != null) {
            List<Map<String, Object>> debug = new ArrayList<>();
            for (ProfileResult result : fetchProfiles) {
                Map<String, Object> resultDebug = new HashMap<>();
                resultDebug.put("time", result.getTime());
                resultDebug.put("breakdown", result.getTimeBreakdown());
                debug.add(resultDebug);
            }
            add.accept("fetch_profile", debug);
        }
    }

    @Override
    protected void doClose() {
        Releasables.close(bucketedTopHits, topDocsCollectors, leafCollectors);
    }

    private static class ResettableScorable extends Scorable {

        private Scorable scorable;

        private ResettableScorable(Scorable scorable) {
            this.scorable = scorable;
        }

        private void reset(Scorable scorable) {
            this.scorable = scorable;
        }

        @Override
        public float score() throws IOException {
            return scorable.score();
        }

        @Override
        public float smoothingScore(int docId) throws IOException {
            return scorable.smoothingScore(docId);
        }

        @Override
        public void setMinCompetitiveScore(float minScore) throws IOException {
            scorable.setMinCompetitiveScore(minScore);
        }

        @Override
        public Collection<ChildScorable> getChildren() throws IOException {
            return scorable.getChildren();
        }
    }
}
