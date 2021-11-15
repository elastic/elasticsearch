/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.confidence;

import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.InvalidAggregationPathException;
import org.elasticsearch.search.aggregations.bucket.IteratorAndCurrent;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilter;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilters;
import org.elasticsearch.search.aggregations.bucket.range.InternalRange;
import org.elasticsearch.search.aggregations.metrics.InternalCardinality;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalSum;
import org.elasticsearch.search.aggregations.metrics.InternalValueCount;
import org.elasticsearch.search.aggregations.metrics.Percentiles;
import org.elasticsearch.search.aggregations.pipeline.AbstractPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers;
import org.elasticsearch.search.aggregations.support.AggregationPath;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.ml.aggs.heuristic.LongBinomialDistribution;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

public class InternalConfidenceAggregation extends InternalMultiBucketAggregation<
    InternalConfidenceAggregation,
    InternalConfidenceAggregation.Bucket> {

    public static class Bucket extends InternalBucket implements MultiBucketsAggregation.Bucket {
        final long key;
        final long docCount;
        InternalAggregations aggregations;

        public Bucket(long key, long docCount, InternalAggregations aggregations) {
            this.key = key;
            this.docCount = docCount;
            this.aggregations = aggregations;
        }

        public Bucket(StreamInput in) throws IOException {
            key = in.readVLong();
            docCount = in.readVLong();
            aggregations = InternalAggregations.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(key);
            out.writeVLong(getDocCount());
            aggregations.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(CommonFields.DOC_COUNT.getPreferredName(), docCount);
            builder.field(CommonFields.KEY.getPreferredName(), key);
            aggregations.toXContentInternal(builder, params);
            builder.endObject();
            return builder;
        }

        @Override
        public Object getKey() {
            return key;
        }

        public long getRawKey() {
            return key;
        }

        @Override
        public String getKeyAsString() {
            return Long.toString(key);
        }

        @Override
        public long getDocCount() {
            return docCount;
        }

        @Override
        public Aggregations getAggregations() {
            return aggregations;
        }

        @Override
        public String toString() {
            return "Bucket{" + "key=" + getKeyAsString() + ", docCount=" + docCount + ", aggregations=" + aggregations.asMap() + "}\n";
        }

    }

    private final List<Bucket> buckets;
    private final List<ConfidenceBuckets> confidences;
    private final int confidenceInterval;
    private final double probability;
    private final double pLower;
    private final double pUpper;
    private final boolean keyed;
    private final Map<String, String> bucketPaths;

    protected InternalConfidenceAggregation(
        String name,
        int confidenceInterval,
        double probability,
        boolean keyed,
        Map<String, String> bucketPaths,
        Map<String, Object> metadata
    ) {
        this(name, confidenceInterval, probability, keyed, bucketPaths, metadata, new ArrayList<>(), new ArrayList<>());
    }

    protected InternalConfidenceAggregation(
        String name,
        int confidenceInterval,
        double probability,
        boolean keyed,
        Map<String, String> bucketPaths,
        Map<String, Object> metadata,
        List<Bucket> buckets,
        List<ConfidenceBuckets> confidences
    ) {
        super(name, metadata);
        this.confidenceInterval = confidenceInterval;
        this.buckets = buckets;
        this.probability = probability;
        this.confidences = confidences;
        double calculatedConfidenceInterval = confidenceInterval / 100.0;
        this.pLower = (1.0 - calculatedConfidenceInterval) / 2.0;
        this.pUpper = (1.0 + calculatedConfidenceInterval) / 2.0;
        this.bucketPaths = bucketPaths;
        this.keyed = keyed;
    }

    public InternalConfidenceAggregation(StreamInput in) throws IOException {
        super(in);
        this.buckets = in.readList(Bucket::new);
        this.probability = in.readDouble();
        this.confidenceInterval = in.readVInt();
        this.confidences = in.readList(ConfidenceBuckets::new);
        this.bucketPaths = in.readMap(StreamInput::readString, StreamInput::readString);
        this.keyed = in.readBoolean();
        double calculatedConfidenceInterval = confidenceInterval / 100.0;
        this.pLower = (1.0 - calculatedConfidenceInterval) / 2.0;
        this.pUpper = (1.0 + calculatedConfidenceInterval) / 2.0;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeList(buckets);
        out.writeDouble(probability);
        out.writeVInt(confidenceInterval);
        out.writeList(confidences);
        out.writeMap(bucketPaths, StreamOutput::writeString, StreamOutput::writeString);
        out.writeBoolean(keyed);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (keyed) {
            builder.startObject("confidences");
        } else {
            builder.startArray("confidences");
        }
        for (ConfidenceBuckets confidences : confidences) {
            confidences.toXContent(builder, params);
        }
        if (keyed) {
            builder.endObject();
        } else {
            builder.endArray();
        }
        return builder;
    }

    @Override
    public InternalConfidenceAggregation create(List<Bucket> buckets) {
        return new InternalConfidenceAggregation(
            name,
            confidenceInterval,
            probability,
            keyed,
            bucketPaths,
            super.metadata,
            buckets,
            confidences
        );
    }

    @Override
    public Bucket createBucket(InternalAggregations aggregations, Bucket prototype) {
        return new Bucket(prototype.key, prototype.docCount, aggregations);
    }

    public Bucket createBucket(long key, long docCount, InternalAggregations aggregations) {
        return new Bucket(key, docCount, aggregations);
    }

    @Override
    protected Bucket reduceBucket(List<Bucket> buckets, AggregationReduceContext context) {
        assert buckets.size() > 0;
        List<InternalAggregations> aggregations = new ArrayList<>(buckets.size());
        long docCount = 0;
        for (Bucket bucket : buckets) {
            docCount += bucket.docCount;
            aggregations.add((InternalAggregations) bucket.getAggregations());
        }
        InternalAggregations aggs = InternalAggregations.reduce(aggregations, context);
        return createBucket(buckets.get(0).key, docCount, aggs);
    }

    @Override
    public List<Bucket> getBuckets() {
        return buckets;
    }

    @Override
    public String getWriteableName() {
        return ConfidenceAggregationBuilder.NAME;
    }

    private List<Bucket> reduceBuckets(List<InternalAggregation> aggregations, AggregationReduceContext reduceContext) {
        final PriorityQueue<IteratorAndCurrent<Bucket>> pq = new PriorityQueue<>(aggregations.size()) {
            @Override
            protected boolean lessThan(IteratorAndCurrent<Bucket> a, IteratorAndCurrent<Bucket> b) {
                return a.current().key < b.current().key;
            }
        };
        for (InternalAggregation aggregation : aggregations) {
            InternalConfidenceAggregation confidence = (InternalConfidenceAggregation) aggregation;
            if (confidence.buckets.isEmpty() == false) {
                pq.add(new IteratorAndCurrent<>(confidence.buckets.iterator()));
            }
        }

        List<Bucket> reducedBuckets = new ArrayList<>();
        if (pq.size() > 0) {
            // list of buckets coming from different shards that have the same key
            List<Bucket> currentBuckets = new ArrayList<>();
            long key = pq.top().current().key;

            do {
                final IteratorAndCurrent<Bucket> top = pq.top();
                if (top.current().key != key) {
                    // The key changes, reduce what we already buffered and reset the buffer for current buckets.
                    // Using Double.compare instead of != to handle NaN correctly.
                    final Bucket reduced = reduceBucket(currentBuckets, reduceContext);
                    reducedBuckets.add(reduced);
                    currentBuckets.clear();
                    key = top.current().key;
                }

                currentBuckets.add(top.current());

                if (top.hasNext()) {
                    top.next();
                    assert top.current().key > key : "shards must return data sorted by key";
                    pq.updateTop();
                } else {
                    pq.pop();
                }
            } while (pq.size() > 0);

            if (currentBuckets.isEmpty() == false) {
                final Bucket reduced = reduceBucket(currentBuckets, reduceContext);
                reducedBuckets.add(reduced);
            }
        }

        return reducedBuckets;
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, AggregationReduceContext reduceContext) {
        List<Bucket> mergedBuckets = reduceBuckets(aggregations, reduceContext);
        if (reduceContext.isFinalReduce() == false || mergedBuckets.isEmpty()) {
            return new InternalConfidenceAggregation(
                name,
                this.confidenceInterval,
                probability,
                keyed,
                bucketPaths,
                metadata,
                mergedBuckets,
                List.of()
            );
        }

        Map<String, List<String>> bucketPaths = this.bucketPaths.entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> AggregationPath.parse(e.getValue()).getPathElementsAsStringList()));

        Map<String, ConfidenceBuilder> builders = new HashMap<>(bucketPaths.size(), 1.0f);
        final Bucket calculatedValueBucket = mergedBuckets.get(0);
        bucketPaths.forEach((name, path) -> {
            InternalAggregation aggregation = calculatedValueBucket.aggregations.get(path.get(0));
            builders.put(name, factory(aggregation, name, path.subList(1, path.size())));
        });

        for (Bucket b : mergedBuckets.subList(1, mergedBuckets.size())) {
            bucketPaths.forEach((name, path) -> {
                InternalAggregation aggregation = b.aggregations.get(path.get(0));
                builders.get(name).addAgg(aggregation);
            });
        }
        return new InternalConfidenceAggregation(
            name,
            this.confidenceInterval,
            probability,
            this.keyed,
            this.bucketPaths,
            metadata,
            mergedBuckets,
            builders.values().stream().map(ConfidenceBuilder::build).collect(Collectors.toList())
        );
    }

    ConfidenceBuilder factory(InternalAggregation internalAggregation, String name, List<String> path) {
        if (internalAggregation instanceof InternalNumericMetricsAggregation.SingleValue) {
            Function<InternalAggregation, double[]> valueExtractor = i -> new double[] {
                ((InternalNumericMetricsAggregation.SingleValue) i).value() };
            Function<InternalAggregation, String[]> keyExtractor = i -> new String[] { "value" };
            if (internalAggregation instanceof InternalSum) {
                return new SumConfidenceBuilder(name, internalAggregation, valueExtractor, keyExtractor);
            } else if (internalAggregation instanceof InternalValueCount || internalAggregation instanceof InternalCardinality) {
                return new CountConfidenceBuilder(name, internalAggregation, valueExtractor, keyExtractor);
            }
            return new MeanConfidenceBuilder(name, internalAggregation, valueExtractor, keyExtractor);
        } else if (internalAggregation instanceof Percentiles) {
            return new MeanConfidenceBuilder(name, internalAggregation, percentiles -> {
                List<Double> values = new ArrayList<>();
                ((Percentiles) percentiles).iterator().forEachRemaining(p -> values.add(p.getValue()));
                return values.stream().mapToDouble(Double::doubleValue).toArray();
            }, percentiles -> {
                List<String> keys = new ArrayList<>();
                ((Percentiles) percentiles).iterator().forEachRemaining(p -> keys.add(Double.toString(p.getPercent())));
                return keys.toArray(String[]::new);
            });
        } else if (internalAggregation instanceof InternalRange) {
            Function<InternalAggregation, double[]> valueExtractor = i -> {
                InternalRange<?, ?> range = (InternalRange<?, ?>) i;
                return range.getBuckets()
                    .stream()
                    .mapToDouble(b -> BucketHelpers.resolveBucketValue(range, b, path, BucketHelpers.GapPolicy.INSERT_ZEROS))
                    .toArray();
            };
            Function<InternalAggregation, String[]> keyExtractor = i -> {
                InternalRange<?, ?> range = (InternalRange<?, ?>) i;
                return range.getBuckets().stream().map(InternalRange.Bucket::getKeyAsString).toArray(String[]::new);
            };
            return getConfidenceViaPath(name, internalAggregation, path, valueExtractor, keyExtractor);
        } else if (internalAggregation instanceof InternalFilters) {
            Function<InternalAggregation, double[]> valueExtractor = i -> {
                InternalFilters filters = (InternalFilters) i;
                return filters.getBuckets()
                    .stream()
                    .mapToDouble(b -> BucketHelpers.resolveBucketValue(filters, b, path, BucketHelpers.GapPolicy.INSERT_ZEROS))
                    .toArray();
            };
            Function<InternalAggregation, String[]> keyExtractor = i -> {
                InternalFilters filters = (InternalFilters) i;
                return filters.getBuckets().stream().map(InternalFilters.InternalBucket::getKeyAsString).toArray(String[]::new);
            };
            return getConfidenceViaPath(name, internalAggregation, path, valueExtractor, keyExtractor);
        } else if (internalAggregation instanceof InternalFilter) {
            Function<InternalAggregation, double[]> valueExtractor = i -> {
                InternalFilter filter = (InternalFilter) i;
                return new double[] { resolveFilterValue(filter, path) };
            };
            Function<InternalAggregation, String[]> keyExtractor = i -> {
                InternalFilter filter = (InternalFilter) i;
                return new String[] { filter.getName() };
            };
            return getConfidenceViaPath(name, internalAggregation, path, valueExtractor, keyExtractor);
        } else {
            throw new IllegalArgumentException("fff");
        }
    }

    private ConfidenceBuilder getConfidenceViaPath(
        String name,
        InternalAggregation internalAggregation,
        List<String> path,
        Function<InternalAggregation, double[]> valueExtractor,
        Function<InternalAggregation, String[]> keyExtractor
    ) {
        Object property = internalAggregation.getProperty(path);
        if (path.get(path.size() - 1).equals("_count") || property instanceof InternalValueCount) {
            return new CountConfidenceBuilder(name, internalAggregation, valueExtractor, keyExtractor);
        } else if (property instanceof InternalSum) {
            return new SumConfidenceBuilder(name, internalAggregation, valueExtractor, keyExtractor);
        } else {
            return new MeanConfidenceBuilder(name, internalAggregation, valueExtractor, keyExtractor);
        }
    }

    public static double resolveFilterValue(InternalFilter agg, List<String> aggPathAsList) {
        try {
            Object propertyValue = agg.getProperty(aggPathAsList);

            if (propertyValue == null) {
                throw new AggregationExecutionException(
                    AbstractPipelineAggregationBuilder.BUCKETS_PATH_FIELD.getPreferredName()
                        + " must reference either a number value or a single value numeric metric aggregation"
                );
            } else {
                double value;
                if (propertyValue instanceof Number) {
                    value = ((Number) propertyValue).doubleValue();
                } else if (propertyValue instanceof InternalNumericMetricsAggregation.SingleValue) {
                    value = ((InternalNumericMetricsAggregation.SingleValue) propertyValue).value();
                } else {
                    throw new IllegalArgumentException("f");
                }
                // doc count never has missing values so gap policy doesn't apply here
                if (aggPathAsList.size() == 1 && "_count".equals(aggPathAsList.get(0))) {
                    return value;
                } else {
                    return 0.0;
                }

            }
        } catch (InvalidAggregationPathException e) {
            throw new IllegalArgumentException("f");
        }
    }

    public static class ConfidenceBuckets implements ToXContentObject, Writeable {
        final double[] upper;
        final double[] lower;
        final double[] value;
        final String[] key;
        final String name;
        final boolean keyed;

        public ConfidenceBuckets(String name, boolean keyed, double[] upper, double[] lower, double[] value, String[] keys) {
            this.name = name;
            this.upper = upper;
            this.lower = lower;
            this.value = value;
            this.key = keys;
            this.keyed = keyed;
        }

        public ConfidenceBuckets(StreamInput in) throws IOException {
            this.name = in.readString();
            this.upper = in.readDoubleArray();
            this.lower = in.readDoubleArray();
            this.value = in.readDoubleArray();
            this.key = in.readStringArray();
            this.keyed = in.readBoolean();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (keyed) {
                builder.startObject(name);
            } else {
                builder.startObject();
                builder.field(CommonFields.KEY.getPreferredName(), name);
            }
            builder.field("calculated", zipIt(value));
            builder.field("upper", zipIt(upper));
            builder.field("lower", zipIt(lower));
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeDoubleArray(upper);
            out.writeDoubleArray(lower);
            out.writeDoubleArray(value);
            out.writeStringArray(key);
            out.writeBoolean(keyed);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ConfidenceBuckets that = (ConfidenceBuckets) o;
            return Arrays.compare(that.upper, upper) == 0
                && Arrays.compare(that.lower, lower) == 0
                && Arrays.compare(that.value, value) == 0;
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(name);
            result = 31 * result + Arrays.hashCode(upper);
            result = 31 * result + Arrays.hashCode(lower);
            result = 31 * result + Arrays.hashCode(value);
            result = 31 * result + Arrays.hashCode(key);
            return result;
        }

        Map<String, Double> zipIt(double[] v) {
            Map<String, Double> map = new LinkedHashMap<>();
            for (int i = 0; i < v.length; i++) {
                map.put(key[i], v[i]);
            }
            return map;
        }
    }

    public abstract class ConfidenceBuilder {
        protected final String name;
        protected final InternalAggregation calculatedValue;
        protected final List<InternalAggregation> values = new ArrayList<>();
        protected final Function<InternalAggregation, double[]> extractor;
        protected final Function<InternalAggregation, String[]> keyExtractor;

        public ConfidenceBuilder(
            String name,
            InternalAggregation calculatedValue,
            Function<InternalAggregation, double[]> extractor,
            Function<InternalAggregation, String[]> keyExtractor
        ) {
            this.name = name;
            this.calculatedValue = calculatedValue;
            this.extractor = extractor;
            this.keyExtractor = keyExtractor;
        }

        public ConfidenceBuilder addAgg(InternalAggregation agg) {
            values.add(agg);
            return this;
        }

        public ConfidenceBuckets build() {
            // seen_value
            double[] calculated = extractor.apply(calculatedValue);
            double[][] extractedValues = new double[this.values.size()][];
            int i = 0;
            for (InternalAggregation agg : this.values) {
                extractedValues[i++] = extractor.apply(agg);
            }
            Arrays.sort(extractedValues, Comparator.comparingDouble((double[] o) -> o[0]).reversed());

            double[] upper = new double[calculated.length];
            double[] lower = new double[calculated.length];
            double[] center = new double[calculated.length];
            for (int j = 0; j < calculated.length; j++) {
                upper[j] = calculate(pUpper, calculated[j], extractedValues, j);
                center[j] = calculate(0.5, calculated[j], extractedValues, j);
                lower[j] = calculate(pLower, calculated[j], extractedValues, j);
            }
            return overallInterval(calculated, center, upper, lower, keyExtractor.apply(calculatedValue));
        }

        double calculate(double p, double value, double[][] values, int index) {
            final double r = Math.max(p, 1 - p);
            final int n = (int) Math.floor(r * values.length);
            double alpha = Math.abs(r - (n + 1.0) / values.length);
            double beta = Math.abs(r - n / (double) values.length);
            final double z = alpha + beta;
            alpha /= z;
            beta /= z;
            final double shift;
            if (p < 0.5) {
                shift = n < values.length
                    ? alpha * values[values.length - n][index] + beta * values[values.length - n - 1][index]
                    : values[0][index];
            } else {
                shift = n + 1 < values.length ? alpha * values[n - 1][index] + beta * values[n][index] : values[values.length - 1][index];
            }
            return value * 2.0 - shift;
        }

        abstract ConfidenceBuckets overallInterval(double[] calculated, double[] center, double[] upper, double[] lower, String[] keys);
    }

    public final class CountConfidenceBuilder extends ConfidenceBuilder {

        public CountConfidenceBuilder(
            String name,
            InternalAggregation calculatedValue,
            Function<InternalAggregation, double[]> extractor,
            Function<InternalAggregation, String[]> keyExtractor
        ) {
            super(name, calculatedValue, extractor, keyExtractor);
        }

        @Override
        public ConfidenceBuilder addAgg(InternalAggregation agg) {
            // pass, unnecessary for count
            return this;
        }

        public ConfidenceBuckets build() {
            // seen_value
            double[] calculated = extractor.apply(calculatedValue);

            double[] upper = new double[calculated.length];
            double[] lower = new double[calculated.length];
            double[] center = new double[calculated.length];
            for (int j = 0; j < calculated.length; j++) {
                upper[j] = new LongBinomialDistribution((long) (calculated[j] / probability), probability).inverseCumulativeProbability(
                    pUpper
                );
                center[j] = calculated[j];
                lower[j] = new LongBinomialDistribution((long) (calculated[j] / probability), probability).inverseCumulativeProbability(
                    pLower
                );
            }
            return overallInterval(calculated, center, upper, lower, keyExtractor.apply(calculatedValue));
        }

        @Override
        ConfidenceBuckets overallInterval(double[] calculated, double[] center, double[] upper, double[] lower, String[] keys) {
            for (int i = 0; i < upper.length; i++) {
                upper[i] = calculated[i] + (1 - probability) / probability * upper[i];
                lower[i] = calculated[i] + (1 - probability) / probability * lower[i];
                center[i] = calculated[i] + (1 - probability) / probability * center[i];
            }
            return new ConfidenceBuckets(name, keyed, upper, lower, center, keys);
        }
    }

    public final class MeanConfidenceBuilder extends ConfidenceBuilder {
        public MeanConfidenceBuilder(
            String name,
            InternalAggregation calculatedValue,
            Function<InternalAggregation, double[]> extractor,
            Function<InternalAggregation, String[]> keyExtractor
        ) {
            super(name, calculatedValue, extractor, keyExtractor);
        }

        @Override
        ConfidenceBuckets overallInterval(double[] calculated, double[] center, double[] upper, double[] lower, String[] keys) {
            for (int i = 0; i < upper.length; i++) {
                upper[i] = probability * calculated[i] + (1 - probability) * upper[i];
                lower[i] = probability * calculated[i] + (1 - probability) * lower[i];
                center[i] = probability * calculated[i] + (1 - probability) * center[i];
            }
            return new ConfidenceBuckets(name, keyed, upper, lower, center, keys);
        }
    }

    public final class SumConfidenceBuilder extends ConfidenceBuilder {

        public SumConfidenceBuilder(
            String name,
            InternalAggregation calculatedValue,
            Function<InternalAggregation, double[]> extractor,
            Function<InternalAggregation, String[]> keyExtractor
        ) {
            super(name, calculatedValue, extractor, keyExtractor);
        }

        @Override
        ConfidenceBuckets overallInterval(double[] calculated, double[] center, double[] upper, double[] lower, String[] keys) {
            for (int i = 0; i < upper.length; i++) {
                upper[i] = calculated[i] + (1 - probability) / probability * upper[i];
                lower[i] = calculated[i] + (1 - probability) / probability * lower[i];
                center[i] = calculated[i] + (1 - probability) / probability * center[i];
            }
            return new ConfidenceBuckets(name, keyed, upper, lower, center, keys);
        }
    }
}
