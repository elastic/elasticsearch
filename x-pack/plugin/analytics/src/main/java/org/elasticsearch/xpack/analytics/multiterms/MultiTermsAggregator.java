/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.multiterms;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.common.util.ObjectArrayPriorityQueue;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.DeferableBucketAggregator;
import org.elasticsearch.search.aggregations.bucket.terms.BucketAndOrd;
import org.elasticsearch.search.aggregations.bucket.terms.BucketPriorityQueue;
import org.elasticsearch.search.aggregations.bucket.terms.BytesKeyedBucketOrds;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static org.elasticsearch.search.aggregations.InternalOrder.isKeyOrder;
import static org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator.aggsUsedForSorting;
import static org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator.descendsFromNestedAggregator;
import static org.elasticsearch.xpack.analytics.multiterms.MultiTermsAggregationBuilder.REGISTRY_KEY;

/**
 * Collects the {@code multi_terms} aggregation, which functions like the
 * {@code terms} aggregation, but supports multiple fields that are treated
 * as a tuple.
 */
class MultiTermsAggregator extends DeferableBucketAggregator {

    protected final List<DocValueFormat> formats;
    protected final TermsAggregator.BucketCountThresholds bucketCountThresholds;
    protected final BucketOrder order;
    protected final Comparator<BucketAndOrd<InternalMultiTerms.Bucket>> partiallyBuiltBucketComparator;
    protected final Set<Aggregator> aggsUsedForSorting;
    protected final SubAggCollectionMode collectMode;
    private final List<TermValuesSource> values;
    private final boolean showTermDocCountError;
    private final boolean needsScore;
    private final List<InternalMultiTerms.KeyConverter> keyConverters;

    private final BytesKeyedBucketOrds bucketOrds;

    protected MultiTermsAggregator(
        String name,
        AggregatorFactories factories,
        AggregationContext context,
        Aggregator parent,
        List<ValuesSourceConfig> configs,
        List<DocValueFormat> formats,
        boolean showTermDocCountError,
        BucketOrder order,
        SubAggCollectionMode collectMode,
        TermsAggregator.BucketCountThresholds bucketCountThresholds,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, factories, context, parent, metadata);
        this.bucketCountThresholds = bucketCountThresholds;
        this.order = order;
        partiallyBuiltBucketComparator = order == null ? null : order.partiallyBuiltBucketComparator(this);
        this.formats = formats;
        this.showTermDocCountError = showTermDocCountError;
        if (subAggsNeedScore() && descendsFromNestedAggregator(parent) || context.isInSortOrderExecutionRequired()) {
            /**
             * Force the execution to depth_first because we need to access the score of
             * nested documents in a sub-aggregation and we are not able to generate this score
             * while replaying deferred documents.
             *
             * We also force depth_first for time-series aggs executions since they need to be visited in a particular order (index
             * sort order) which might be changed by the breadth_first execution.
             */
            this.collectMode = SubAggCollectionMode.DEPTH_FIRST;
        } else {
            this.collectMode = collectMode;
        }
        aggsUsedForSorting = aggsUsedForSorting(this, order);
        this.needsScore = configs.stream().anyMatch(c -> c.getValuesSource().needsScores());
        values = configs.stream()
            .map(c -> context.getValuesSourceRegistry().getAggregator(REGISTRY_KEY, c).build(c))
            .collect(Collectors.toList());
        keyConverters = values.stream().map(TermValuesSource::keyConverter).collect(Collectors.toList());
        bucketOrds = BytesKeyedBucketOrds.build(context.bigArrays(), cardinality);
    }

    private boolean subAggsNeedScore() {
        for (Aggregator subAgg : subAggregators) {
            if (subAgg.scoreMode().needsScores()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public ScoreMode scoreMode() {
        if (needsScore) {
            return ScoreMode.COMPLETE;
        }
        return super.scoreMode();
    }

    @Override
    protected boolean shouldDefer(Aggregator aggregator) {
        return collectMode == SubAggCollectionMode.BREADTH_FIRST && aggsUsedForSorting.contains(aggregator) == false;
    }

    List<TermValues> termValuesList(LeafReaderContext ctx) throws IOException {
        List<TermValues> termValuesList = new ArrayList<>();
        for (TermValuesSource termValuesSource : values) {
            termValuesList.add(termValuesSource.getValues(ctx));
        }
        return termValuesList;
    }

    static List<List<Object>> docTerms(List<TermValues> termValuesList, int doc) throws IOException {
        List<List<Object>> terms = new ArrayList<>();
        for (TermValues termValues : termValuesList) {
            List<Object> collectValues = termValues.collectValues(doc);
            if (collectValues == null) {
                return null;
            }
            terms.add(collectValues);
        }
        return terms;
    }

    /**
     * Packs a list of terms into ByteRef so we can use BytesKeyedBucketOrds
     *
     * TODO: this is a temporary solution, we should replace it with a more optimal mechanism instead of relying on BytesKeyedBucketOrds
     */
    static BytesRef packKey(List<Object> terms) {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.writeCollection(terms, StreamOutput::writeGenericValue);
            return output.bytes().toBytesRef();
        } catch (IOException ex) {
            throw ExceptionsHelper.convertToRuntime(ex);
        }
    }

    /**
     * Unpacks ByteRef back into a list of terms
     *
     * TODO: this is a temporary solution, we should replace it with a more optimal mechanism instead of relying on BytesKeyedBucketOrds
     */
    static List<Object> unpackTerms(BytesRef termsBytes) {
        try (StreamInput input = new BytesArray(termsBytes).streamInput()) {
            return input.readCollectionAsList(StreamInput::readGenericValue);
        } catch (IOException ex) {
            throw ExceptionsHelper.convertToRuntime(ex);
        }
    }

    @Override
    public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, LeafBucketCollector sub) throws IOException {
        List<TermValues> termValuesList = termValuesList(aggCtx.getLeafReaderContext());

        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                List<List<Object>> terms = docTerms(termValuesList, doc);
                if (terms != null) {
                    List<Object> path = new ArrayList<>(terms.size());
                    new CheckedConsumer<Integer, IOException>() {
                        @Override
                        public void accept(Integer start) throws IOException {
                            for (Object term : terms.get(start)) {
                                if (start == path.size()) {
                                    path.add(term);
                                } else {
                                    path.set(start, term);
                                }
                                if (start < terms.size() - 1) {
                                    this.accept(start + 1);
                                } else {
                                    long bucketOrd = bucketOrds.add(owningBucketOrd, packKey(path));
                                    if (bucketOrd < 0) { // already seen
                                        bucketOrd = -1 - bucketOrd;
                                        collectExistingBucket(sub, doc, bucketOrd);
                                    } else {
                                        collectBucket(sub, doc, bucketOrd);
                                    }
                                }
                            }
                        }
                    }.accept(0);
                }
            }
        };
    }

    @Override
    protected void doClose() {
        Releasables.close(bucketOrds);
    }

    @Override
    public InternalAggregation[] buildAggregations(LongArray owningBucketOrds) throws IOException {
        try (
            LongArray otherDocCounts = bigArrays().newLongArray(owningBucketOrds.size(), true);
            ObjectArray<InternalMultiTerms.Bucket[]> topBucketsPerOrd = bigArrays().newObjectArray(owningBucketOrds.size())
        ) {
            try (IntArray bucketsToCollect = bigArrays().newIntArray(owningBucketOrds.size())) {
                long ordsToCollect = 0;
                for (long ordIdx = 0; ordIdx < owningBucketOrds.size(); ordIdx++) {
                    int size = (int) Math.min(bucketOrds.bucketsInOrd(owningBucketOrds.get(ordIdx)), bucketCountThresholds.getShardSize());
                    ordsToCollect += size;
                    bucketsToCollect.set(ordIdx, size);
                }
                try (LongArray ordsArray = bigArrays().newLongArray(ordsToCollect)) {
                    long ordsCollected = 0;
                    for (long ordIdx = 0; ordIdx < owningBucketOrds.size(); ordIdx++) {
                        final long owningBucketOrd = owningBucketOrds.get(ordIdx);
                        long bucketsInOrd = bucketOrds.bucketsInOrd(owningBucketOrd);

                        int size = (int) Math.min(bucketsInOrd, bucketCountThresholds.getShardSize());
                        try (
                            ObjectArrayPriorityQueue<BucketAndOrd<InternalMultiTerms.Bucket>> ordered = new BucketPriorityQueue<>(
                                size,
                                bigArrays(),
                                partiallyBuiltBucketComparator
                            )
                        ) {
                            BucketAndOrd<InternalMultiTerms.Bucket> spare = null;
                            BytesRef spareKey = null;
                            BytesKeyedBucketOrds.BucketOrdsEnum ordsEnum = bucketOrds.ordsEnum(owningBucketOrd);
                            while (ordsEnum.next()) {
                                long docCount = bucketDocCount(ordsEnum.ord());
                                otherDocCounts.increment(ordIdx, docCount);
                                if (docCount < bucketCountThresholds.getShardMinDocCount()) {
                                    continue;
                                }
                                if (spare == null) {
                                    checkRealMemoryCBForInternalBucket();
                                    spare = new BucketAndOrd<>(
                                        new InternalMultiTerms.Bucket(null, 0, null, showTermDocCountError, 0, formats, keyConverters)
                                    );
                                    spareKey = new BytesRef();
                                }
                                ordsEnum.readValue(spareKey);
                                spare.bucket.terms = unpackTerms(spareKey);
                                spare.bucket.docCount = docCount;
                                spare.ord = ordsEnum.ord();
                                spare = ordered.insertWithOverflow(spare);
                            }

                            // Get the top buckets
                            int orderedSize = (int) ordered.size();
                            InternalMultiTerms.Bucket[] buckets = new InternalMultiTerms.Bucket[orderedSize];
                            for (int i = orderedSize - 1; i >= 0; --i) {
                                BucketAndOrd<InternalMultiTerms.Bucket> bucketAndOrd = ordered.pop();
                                buckets[i] = bucketAndOrd.bucket;
                                ordsArray.set(ordsCollected + i, bucketAndOrd.ord);
                                otherDocCounts.increment(ordIdx, -buckets[i].getDocCount());
                            }
                            topBucketsPerOrd.set(ordIdx, buckets);
                            ordsCollected += orderedSize;
                        }
                    }
                    buildSubAggsForAllBuckets(topBucketsPerOrd, ordsArray, (b, a) -> b.aggregations = a);
                }
            }

            return buildAggregations(
                Math.toIntExact(owningBucketOrds.size()),
                ordIdx -> buildResult(otherDocCounts.get(ordIdx), topBucketsPerOrd.get(ordIdx))
            );
        }
    }

    InternalMultiTerms buildResult(long otherDocCount, InternalMultiTerms.Bucket[] topBuckets) {
        final BucketOrder reduceOrder;
        if (isKeyOrder(order) == false) {
            reduceOrder = InternalOrder.key(true);
            Arrays.sort(topBuckets, reduceOrder.comparator());
        } else {
            reduceOrder = order;
        }
        return new InternalMultiTerms(
            name,
            reduceOrder,
            order,
            bucketCountThresholds.getRequiredSize(),
            bucketCountThresholds.getMinDocCount(),
            bucketCountThresholds.getShardSize(),
            showTermDocCountError,
            otherDocCount,
            Arrays.asList(topBuckets),
            0,
            formats,
            keyConverters,
            metadata()
        );
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalMultiTerms(
            name,
            order,
            order,
            bucketCountThresholds.getRequiredSize(),
            bucketCountThresholds.getMinDocCount(),
            bucketCountThresholds.getShardSize(),
            showTermDocCountError,
            0,
            emptyList(),
            0,
            formats,
            keyConverters,
            metadata()
        );
    }

    static TermValuesSource buildNumericTermValues(ValuesSourceConfig config) {
        final ValuesSource.Numeric vs = (ValuesSource.Numeric) config.getValuesSource();
        if (vs.isFloatingPoint()) {
            return new DoubleTermValuesSource(config);
        } else {
            return new LongTermValuesSource(config);
        }
    }

    /**
     * Capture type-specific functionality of each term that comprises the multi_term key
     */
    interface TermValuesSource {
        /**
         * used in getLeafCollector to obtain a doc values for the given type
         */
        TermValues getValues(LeafReaderContext ctx) throws IOException;

        /**
         * Returns a key converter that knows how to convert key values into user-friendly representation and format them as a string
         */
        InternalMultiTerms.KeyConverter keyConverter();
    }

    interface TermValues {
        /**
         * Returns a list of values retrieved from doc values for the given document
         */
        List<Object> collectValues(int doc) throws IOException;
    }

    /**
     * Handles non-float and date doc values
     */
    static class LongTermValuesSource implements TermValuesSource {
        final ValuesSource.Numeric source;
        final InternalMultiTerms.KeyConverter converter;

        LongTermValuesSource(ValuesSourceConfig config) {
            this.source = (ValuesSource.Numeric) config.getValuesSource();
            if (config.format() == DocValueFormat.UNSIGNED_LONG_SHIFTED) {
                converter = InternalMultiTerms.KeyConverter.UNSIGNED_LONG;
            } else {
                converter = InternalMultiTerms.KeyConverter.LONG;
            }
        }

        @Override
        public TermValues getValues(LeafReaderContext ctx) throws IOException {
            final SortedNumericDocValues values = source.longValues(ctx);
            final NumericDocValues singleton = DocValues.unwrapSingleton(values);
            return singleton != null ? getValues(singleton) : getValues(values);
        }

        public TermValues getValues(SortedNumericDocValues values) {
            return doc -> {
                if (values.advanceExact(doc)) {
                    final List<Object> objects = new ArrayList<>();
                    final int valuesCount = values.docValueCount();
                    long previous = Long.MAX_VALUE;
                    for (int i = 0; i < valuesCount; ++i) {
                        final long val = values.nextValue();
                        if (previous != val || i == 0) {
                            objects.add(val);
                            previous = val;
                        }
                    }
                    return objects;
                } else {
                    return null;
                }
            };
        }

        public TermValues getValues(NumericDocValues values) {
            return doc -> {
                if (values.advanceExact(doc)) {
                    return List.of(values.longValue());
                } else {
                    return null;
                }
            };
        }

        @Override
        public InternalMultiTerms.KeyConverter keyConverter() {
            return converter;
        }
    }

    /**
     * Handles float and date doc values
     */
    static class DoubleTermValuesSource implements TermValuesSource {
        final ValuesSource.Numeric source;

        DoubleTermValuesSource(ValuesSourceConfig config) {
            this.source = (ValuesSource.Numeric) config.getValuesSource();
        }

        @Override
        public TermValues getValues(LeafReaderContext ctx) throws IOException {
            final SortedNumericDoubleValues values = source.doubleValues(ctx);
            final NumericDoubleValues singleton = FieldData.unwrapSingleton(values);
            return singleton != null ? getValues(singleton) : getValues(values);
        }

        public TermValues getValues(SortedNumericDoubleValues values) {
            return doc -> {
                if (values.advanceExact(doc)) {
                    final List<Object> objects = new ArrayList<>();
                    final int valuesCount = values.docValueCount();
                    double previous = Double.MAX_VALUE;
                    for (int i = 0; i < valuesCount; ++i) {
                        final double val = values.nextValue();
                        if (previous != val || i == 0) {
                            objects.add(val);
                            previous = val;
                        }
                    }
                    return objects;
                } else {
                    return null;
                }
            };
        }

        public TermValues getValues(NumericDoubleValues values) {
            return doc -> {
                if (values.advanceExact(doc)) {
                    return List.of(values.doubleValue());
                } else {
                    return null;
                }
            };
        }

        @Override
        public InternalMultiTerms.KeyConverter keyConverter() {
            return InternalMultiTerms.KeyConverter.DOUBLE;
        }
    }

    /**
     * Base class for string and ip doc values
     */
    abstract static class BinaryTermValuesSource implements TermValuesSource {
        private final ValuesSource source;
        final BytesRefBuilder previous = new BytesRefBuilder();

        BinaryTermValuesSource(ValuesSourceConfig source) {
            this.source = source.getValuesSource();
        }

        @Override
        public TermValues getValues(LeafReaderContext ctx) throws IOException {
            final SortedBinaryDocValues values = source.bytesValues(ctx);
            final BinaryDocValues singleton = FieldData.unwrapSingleton(values);
            return singleton != null ? getValues(singleton) : getValues(values);
        }

        private TermValues getValues(SortedBinaryDocValues values) {
            return doc -> {
                if (values.advanceExact(doc)) {
                    final int valuesCount = values.docValueCount();
                    final List<Object> objects = new ArrayList<>(valuesCount);
                    // SortedBinaryDocValues don't guarantee uniqueness so we
                    // need to take care of dups
                    previous.clear();
                    for (int i = 0; i < valuesCount; ++i) {
                        final BytesRef bytes = values.nextValue();
                        if (i > 0 && previous.get().equals(bytes)) {
                            continue;
                        }
                        previous.copyBytes(bytes);
                        objects.add(BytesRef.deepCopyOf(bytes));
                    }
                    return objects;
                } else {
                    return null;
                }
            };
        }

        private TermValues getValues(BinaryDocValues values) {
            return doc -> {
                if (values.advanceExact(doc)) {
                    return List.of(BytesRef.deepCopyOf(values.binaryValue()));
                } else {
                    return null;
                }
            };
        }
    }

    /**
     * String doc values
     */
    static class StringTermValuesSource extends BinaryTermValuesSource {

        StringTermValuesSource(ValuesSourceConfig source) {
            super(source);
        }

        @Override
        public InternalMultiTerms.KeyConverter keyConverter() {
            return InternalMultiTerms.KeyConverter.STRING;
        }

    }

    /**
     * IP doc values
     */
    static class IPTermValuesSource extends BinaryTermValuesSource {

        IPTermValuesSource(ValuesSourceConfig source) {
            super(source);
        }

        @Override
        public InternalMultiTerms.KeyConverter keyConverter() {
            return InternalMultiTerms.KeyConverter.IP;
        }
    }

}
