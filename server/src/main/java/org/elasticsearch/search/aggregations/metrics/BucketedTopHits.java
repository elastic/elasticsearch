/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.metrics;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.common.lucene.search.TopDocsAndMaxScore;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.FloatArray;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.internal.SubSearchContext;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortFieldValidation;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.sort.SortValue;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

/**
 * Collects the top documents of every bucket of a {@code top_hits} aggregation with a {@link BucketedSort}.
 * Only a single sort on a numeric field is supported, and only when the values it reports are of the type the sort field of the
 * shard result declares.
 * The Lucene collectors {@link TopHitsAggregator} otherwise uses are one collector per bucket, and for every segment one leaf
 * collector per bucket that opens its own doc values readers for the sort.
 * Everything the Lucene collectors provide to the shard result is reproduced here: the doc ids in the same order, including the
 * lower doc id winning among equal sort values; the sort values as the raw type the Lucene comparator would have produced; the
 * exact number of hits of the bucket; and, with {@code track_scores}, the highest score of any document in the bucket.
 * The state is indexed by bucket ordinal, as it is for a {@code max} or a {@code top_metrics}, so it grows with the highest ordinal
 * collected and not with the number of buckets that have documents.
 */
final class BucketedTopHits implements Releasable {

    /**
     * Stores the global doc id next to each entry of the {@link BucketedSort} and makes the lower doc id win among equal sort
     * values, which is what a Lucene {@code TopFieldCollector} does.
     */
    private static final class DocIds implements BucketedSort.TieBreakingExtraData, Releasable {
        private final BigArrays bigArrays;
        private IntArray docIds;

        DocIds(BigArrays bigArrays) {
            this.bigArrays = bigArrays;
            this.docIds = bigArrays.newIntArray(1, false);
        }

        int get(long index) {
            return docIds.get(index);
        }

        @Override
        public void swap(long lhs, long rhs) {
            int tmp = docIds.get(lhs);
            docIds.set(lhs, docIds.get(rhs));
            docIds.set(rhs, tmp);
        }

        @Override
        public Loader loader(LeafReaderContext ctx) {
            int docBase = ctx.docBase;
            return (index, doc) -> {
                docIds = bigArrays.grow(docIds, index + 1);
                docIds.set(index, docBase + doc);
            };
        }

        @Override
        public boolean betterOnTie(long lhs, long rhs) {
            return docIds.get(lhs) < docIds.get(rhs);
        }

        @Override
        public void close() {
            docIds.close();
        }
    }

    /**
     * An entry of a bucket, ordered by sort value and then by doc id, lowest doc id first for both sort orders.
     * {@link BucketedSort#getValues} reverses the whole comparison for a descending sort, so the doc id comparison is reversed
     * in that case to come out the right way round.
     */
    private record Hit(SortValue value, int doc, SortOrder order) implements Comparable<Hit> {
        @Override
        public int compareTo(Hit other) {
            int cmp = value.compareTo(other.value);
            if (cmp != 0) {
                return cmp;
            }
            return order == SortOrder.DESC ? Integer.compare(other.doc, doc) : Integer.compare(doc, other.doc);
        }
    }

    /**
     * The number of entries to keep per bucket. It is capped at the number of documents so that a large {@code from + size}, which
     * an index can allow with {@code index.max_inner_result_window}, does not allocate slots that can never be filled. A
     * {@code BucketedSort} stores that many slots for every bucket it sees.
     */
    static int bucketSize(int from, int size, int maxDoc) {
        return Math.min(from + size, maxDoc);
    }

    /**
     * Builds the collection for a {@code top_hits}, or returns {@code null} if it cannot be used and the Lucene collectors have
     * to be. This is decided here, once, when the aggregator is created.
     */
    @Nullable
    static BucketedTopHits tryCreate(
        AggregationContext context,
        SubSearchContext subSearchContext,
        @Nullable List<SortBuilder<?>> sortBuilders
    ) throws IOException {
        if (sortBuilders == null || sortBuilders.size() != 1 || subSearchContext.sort() == null) {
            // No sort is score order with rescoring, and several sorts are not supported by a BucketedSort at all.
            return null;
        }
        if (sortBuilders.get(0) instanceof FieldSortBuilder fieldSort) {
            if (fieldSort.getNestedSort() != null) {
                return null;
            }
            int bucketSize = bucketSize(
                subSearchContext.from(),
                subSearchContext.size(),
                subSearchContext.searcher().getIndexReader().maxDoc()
            );
            if (bucketSize < 1) {
                return null;
            }
            return create(context, subSearchContext, fieldSort, bucketSize);
        }
        return null;
    }

    @Nullable
    private static BucketedTopHits create(
        AggregationContext context,
        SubSearchContext subSearchContext,
        FieldSortBuilder fieldSort,
        int bucketSize
    ) throws IOException {
        BigArrays bigArrays = context.bigArrays();
        DocIds docIds = new DocIds(bigArrays);
        BucketedSort sort = null;
        LongArray hitCounts = null;
        FloatArray maxScores = null;
        boolean success = false;
        try {
            try {
                sort = context.buildBucketedSort(fieldSort, bucketSize, docIds);
            } catch (IllegalArgumentException | QueryShardException e) {
                // The sort was already built for the Lucene collectors, so it is valid. This means the BucketedSort does not
                // support it, for example on a field that is not numeric, or on _doc.
                return null;
            }
            RawValues raw = rawValues(sort);
            if (raw == null || raw.type() != SortFieldValidation.getType(subSearchContext.sort().sort.getSort()[0])) {
                // The shard results are merged using the type each sort field declares, and the values have to be of that type. They
                // are not always: int, short and byte fields in indices created before IndexVersions.INDEX_INT_SORT_INT_TYPE
                // declare a long sort, which the Lucene collectors fill with longs, while the sort built here is an int one and reports
                // integers. Merging the two fails
                // with a ClassCastException, so the Lucene collectors keep those.
                return null;
            }
            hitCounts = bigArrays.newLongArray(1);
            boolean trackScores = subSearchContext.trackScores();
            maxScores = trackScores ? bigArrays.newFloatArray(1, false) : null;
            BucketedTopHits result = new BucketedTopHits(bigArrays, sort, docIds, hitCounts, maxScores, raw.convert());
            success = true;
            return result;
        } finally {
            if (success == false) {
                Releasables.close(sort, docIds, hitCounts, maxScores);
            }
        }
    }

    /**
     * The raw values a kind of {@link BucketedSort} reports: the type of Lucene sort they belong to, and how to turn a
     * {@link SortValue} back into the raw value the Lucene comparator for the same sort holds in {@link FieldDoc#fields}.
     */
    private record RawValues(SortField.Type type, Function<SortValue, Object> convert) {}

    /**
     * The raw values of a {@link BucketedSort}, or {@code null} if it is not a numeric one.
     */
    @Nullable
    private static RawValues rawValues(BucketedSort sort) {
        if (sort instanceof BucketedSort.ForLongs) {
            return new RawValues(SortField.Type.LONG, value -> value.numberValue().longValue());
        }
        if (sort instanceof BucketedSort.ForInts) {
            return new RawValues(SortField.Type.INT, value -> value.numberValue().intValue());
        }
        if (sort instanceof BucketedSort.ForDoubles) {
            return new RawValues(
                SortField.Type.DOUBLE,
                value -> value.numberValue() == null ? Double.NaN : value.numberValue().doubleValue()
            );
        }
        if (sort instanceof BucketedSort.ForFloats) {
            return new RawValues(SortField.Type.FLOAT, value -> value.numberValue() == null ? Float.NaN : value.numberValue().floatValue());
        }
        return null;
    }

    private final BigArrays bigArrays;
    private final BucketedSort sort;
    private final DocIds docIds;
    /**
     * The number of documents collected for each bucket. The Lucene collectors report it as the total hits of the bucket, and it
     * is not the number of entries kept.
     */
    private LongArray hitCounts;
    /**
     * The highest score of any document collected for each bucket, only with {@code track_scores}. It is the maximum over all
     * the documents of the bucket and not only over the ones kept.
     */
    @Nullable
    private FloatArray maxScores;
    private final Function<SortValue, Object> rawValue;

    private BucketedTopHits(
        BigArrays bigArrays,
        BucketedSort sort,
        DocIds docIds,
        LongArray hitCounts,
        @Nullable FloatArray maxScores,
        Function<SortValue, Object> rawValue
    ) {
        this.bigArrays = bigArrays;
        this.sort = sort;
        this.docIds = docIds;
        this.hitCounts = hitCounts;
        this.maxScores = maxScores;
        this.rawValue = rawValue;
    }

    /**
     * A collector for one segment. It shares the doc values of the segment between all the buckets.
     */
    LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, LeafBucketCollector sub) throws IOException {
        BucketedSort.Leaf leaf = sort.forLeaf(aggCtx.getLeafReaderContext());
        return new LeafBucketCollectorBase(sub, null) {
            private Scorable scorer;

            @Override
            public void setScorer(Scorable scorer) throws IOException {
                this.scorer = scorer;
                leaf.setScorer(scorer);
                super.setScorer(scorer);
            }

            @Override
            public void collect(int doc, long bucket) throws IOException {
                hitCounts = bigArrays.grow(hitCounts, bucket + 1);
                long collected = hitCounts.increment(bucket, 1);
                if (maxScores != null) {
                    maxScores = bigArrays.grow(maxScores, bucket + 1);
                    float score = scorer.score();
                    maxScores.set(bucket, collected == 1 ? score : Math.max(maxScores.get(bucket), score));
                }
                leaf.collect(doc, bucket);
            }
        };
    }

    /**
     * The top documents of a bucket, in the form the Lucene collectors produce them, or {@code null} if no document was
     * collected for it.
     */
    @Nullable
    TopDocsAndMaxScore topDocs(long bucket, SubSearchContext subSearchContext) throws IOException {
        long hits = bucket < hitCounts.size() ? hitCounts.get(bucket) : 0;
        if (hits == 0) {
            return null;
        }
        SortOrder order = sort.getOrder();
        List<Hit> top = sort.getValues(bucket, (index, value) -> new Hit(value, docIds.get(index), order));
        FieldDoc[] docs = new FieldDoc[top.size()];
        for (int i = 0; i < docs.length; i++) {
            Hit hit = top.get(i);
            docs[i] = new FieldDoc(hit.doc(), Float.NaN, new Object[] { rawValue.apply(hit.value()) });
        }
        TopFieldDocs topDocs = new TopFieldDocs(
            new TotalHits(hits, TotalHits.Relation.EQUAL_TO),
            docs,
            subSearchContext.sort().sort.getSort()
        );
        float maxScore = Float.NaN;
        if (maxScores != null) {
            TopFieldCollector.populateScores(docs, subSearchContext.searcher(), subSearchContext.query());
            maxScore = maxScores.get(bucket);
        }
        return new TopDocsAndMaxScore(topDocs, maxScore);
    }

    @Override
    public void close() {
        Releasables.close(sort, docIds, hitCounts, maxScores);
    }
}
