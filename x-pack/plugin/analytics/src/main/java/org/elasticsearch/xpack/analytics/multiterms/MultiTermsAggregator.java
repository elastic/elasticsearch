/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.multiterms;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Releasables;
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
    protected final Comparator<InternalMultiTerms.Bucket> partiallyBuiltBucketComparator;
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
        partiallyBuiltBucketComparator = order == null ? null : order.partiallyBuiltBucketComparator(b -> b.bucketOrd, this);
        this.formats = formats;
        this.showTermDocCountError = showTermDocCountError;
        if (subAggsNeedScore() && descendsFromNestedAggregator(parent)) {
            /**
             * Force the execution to depth_first because we need to access the score of
             * nested documents in a sub-aggregation and we are not able to generate this score
             * while replaying deferred documents.
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

    List<List<Object>> docTerms(List<TermValues> termValuesList, int doc) throws IOException {
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
            return input.readList(StreamInput::readGenericValue);
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
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        InternalMultiTerms.Bucket[][] topBucketsPerOrd = new InternalMultiTerms.Bucket[owningBucketOrds.length][];
        long[] otherDocCounts = new long[owningBucketOrds.length];
        for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
            long bucketsInOrd = bucketOrds.bucketsInOrd(owningBucketOrds[ordIdx]);

            int size = (int) Math.min(bucketsInOrd, bucketCountThresholds.getShardSize());
            PriorityQueue<InternalMultiTerms.Bucket> ordered = new BucketPriorityQueue<>(size, partiallyBuiltBucketComparator);
            InternalMultiTerms.Bucket spare = null;
            BytesRef spareKey = null;
            BytesKeyedBucketOrds.BucketOrdsEnum ordsEnum = bucketOrds.ordsEnum(owningBucketOrds[ordIdx]);
            while (ordsEnum.next()) {
                long docCount = bucketDocCount(ordsEnum.ord());
                otherDocCounts[ordIdx] += docCount;
                if (docCount < bucketCountThresholds.getShardMinDocCount()) {
                    continue;
                }
                if (spare == null) {
                    spare = new InternalMultiTerms.Bucket(null, 0, null, showTermDocCountError, 0, formats, keyConverters);
                    spareKey = new BytesRef();
                }
                ordsEnum.readValue(spareKey);
                spare.terms = unpackTerms(spareKey);
                spare.docCount = docCount;
                spare.bucketOrd = ordsEnum.ord();
                spare = ordered.insertWithOverflow(spare);
            }

            // Get the top buckets
            InternalMultiTerms.Bucket[] bucketsForOrd = new InternalMultiTerms.Bucket[ordered.size()];
            topBucketsPerOrd[ordIdx] = bucketsForOrd;
            for (int b = ordered.size() - 1; b >= 0; --b) {
                topBucketsPerOrd[ordIdx][b] = ordered.pop();
                otherDocCounts[ordIdx] -= topBucketsPerOrd[ordIdx][b].getDocCount();
            }
        }

        buildSubAggsForAllBuckets(topBucketsPerOrd, b -> b.bucketOrd, (b, a) -> b.aggregations = a);

        InternalAggregation[] result = new InternalAggregation[owningBucketOrds.length];
        for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
            result[ordIdx] = buildResult(otherDocCounts[ordIdx], topBucketsPerOrd[ordIdx]);
        }
        return result;
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
            List.of(topBuckets),
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
            SortedNumericDocValues values = source.longValues(ctx);
            return doc -> {
                if (values.advanceExact(doc)) {
                    List<Object> objects = new ArrayList<>();
                    int valuesCount = values.docValueCount();
                    long previous = Long.MAX_VALUE;
                    for (int i = 0; i < valuesCount; ++i) {
                        long val = values.nextValue();
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
            SortedNumericDoubleValues values = source.doubleValues(ctx);
            return doc -> {
                if (values.advanceExact(doc)) {
                    List<Object> objects = new ArrayList<>();
                    int valuesCount = values.docValueCount();
                    double previous = Double.MAX_VALUE;
                    for (int i = 0; i < valuesCount; ++i) {
                        double val = values.nextValue();
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
            SortedBinaryDocValues values = source.bytesValues(ctx);
            return doc -> {
                if (values.advanceExact(doc)) {
                    int valuesCount = values.docValueCount();
                    List<Object> objects = new ArrayList<>(valuesCount);
                    // SortedBinaryDocValues don't guarantee uniqueness so we
                    // need to take care of dups
                    previous.clear();
                    for (int i = 0; i < valuesCount; ++i) {
                        BytesRef bytes = values.nextValue();
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
