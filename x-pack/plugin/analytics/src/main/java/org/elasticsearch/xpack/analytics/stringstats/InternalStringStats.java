/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.analytics.stringstats;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.StatsAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class InternalStringStats extends InternalAggregation {

    enum Metrics {
        count, min_length, max_length, avg_length, entropy;

        public static Metrics resolve(String name) {
            return Metrics.valueOf(name);
        }
    }

    private static final DocValueFormat DEFAULT_FORMAT = DocValueFormat.RAW;

    private DocValueFormat format = DEFAULT_FORMAT;
    private final boolean showDistribution;
    private final long count;
    private final long totalLength;
    private final int minLength;
    private final int maxLength;
    private final Map<String, Long> charOccurrences;

    public InternalStringStats(String name, long count, long totalLength, int minLength, int maxLength,
                               Map<String, Long> charOccurences, boolean showDistribution,
                               DocValueFormat formatter,
                               List<PipelineAggregator> pipelineAggregators,
                               Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
        this.format = formatter;
        this.showDistribution = showDistribution;
        this.count = count;
        this.totalLength = totalLength;
        this.minLength = minLength;
        this.maxLength = maxLength;
        this.charOccurrences = charOccurences;
    }

    /** Read from a stream. */
    public InternalStringStats(StreamInput in) throws IOException {
        super(in);
        format = in.readNamedWriteable(DocValueFormat.class);
        showDistribution = in.readBoolean();
        count = in.readVLong();
        totalLength = in.readVLong();
        minLength = in.readVInt();
        maxLength = in.readVInt();
        charOccurrences = in.<String, Long>readMap(StreamInput::readString, StreamInput::readLong);
    }

    @Override
    protected final void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        out.writeBoolean(showDistribution);
        out.writeVLong(count);
        out.writeVLong(totalLength);
        out.writeVInt(minLength);
        out.writeVInt(maxLength);
        out.writeMap(charOccurrences, StreamOutput::writeString, StreamOutput::writeLong);
    }

    public String getWriteableName() {
        return StatsAggregationBuilder.NAME;
    }

    public long getCount() {
        return count;
    }

    public int getMinLength() {
        return minLength;
    }

    public int getMaxLength() {
        return maxLength;
    }

    public double getAvgLength() {
        return (double) totalLength / count;
    }

    public double getEntropy() {
        double sum = 0.0;
        double compensation = 0.0;
        for (double p : getDistribution().values()) {
            if (p > 0) {
                // Compute the sum of double values with Kahan summation algorithm which is more
                // accurate than naive summation.
                double value = p * log2(p);
                if (Double.isFinite(value) == false) {
                    sum += value;
                } else if (Double.isFinite(sum)) {
                    double corrected = value - compensation;
                    double newSum = sum + corrected;
                    compensation = (newSum - sum) - corrected;
                    sum = newSum;
                }
            }
        }
        return -sum;
    }

    /**
     * Convert the character occurrences map to character frequencies.
     *
     * @return A map with the character as key and the probability of
     * this character to occur as value. The map is ordered by frequency descending.
     */
    public Map<String, Double> getDistribution() {
       return charOccurrences.entrySet().stream()
            .sorted((e1, e2) -> e2.getValue().compareTo(e1.getValue()))
            .collect(
                Collectors.toMap(e -> e.getKey(), e -> (double) e.getValue() / totalLength,
                    (e1, e2) -> e2, LinkedHashMap::new)
            );
    }

    /** Calculate base 2 logarithm */
    static double log2(double d) {
        return Math.log(d) / Math.log(2.0);
    }

    public String getCountAsString() {
        return format.format(getCount()).toString();
    }

    public String getMinLengthAsString() {
        return format.format(getMinLength()).toString();
    }

    public String getMaxLengthAsString() {
        return format.format(getMaxLength()).toString();
    }

    public String getAvgLengthAsString() {
        return format.format(getAvgLength()).toString();
    }

    public String getEntropyAsString() {
        return format.format(getEntropy()).toString();
    }

    public Object value(String name) {
        Metrics metrics = Metrics.valueOf(name);
        switch (metrics) {
            case count: return this.count;
            case min_length: return this.minLength;
            case max_length: return this.maxLength;
            case avg_length: return this.getAvgLength();
            case entropy: return this.getEntropy();
            default:
                throw new IllegalArgumentException("Unknown value [" + name + "] in common stats aggregation");
        }
    }

    @Override
    public InternalStringStats doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        long count = 0;
        long totalLength = 0;
        int minLength = Integer.MAX_VALUE;
        int maxLength = Integer.MIN_VALUE;
        Map<String, Long> occurs = new HashMap<>();

        for (InternalAggregation aggregation : aggregations) {
            InternalStringStats stats = (InternalStringStats) aggregation;
            count += stats.getCount();
            minLength = Math.min(minLength, stats.getMinLength());
            maxLength = Math.max(maxLength, stats.getMaxLength());
            totalLength += stats.totalLength;
            stats.charOccurrences.forEach((k, v) ->
                occurs.merge(k, v, (oldValue, newValue) -> oldValue + newValue)
            );
        }

        return new InternalStringStats(name, count, totalLength, minLength, maxLength, occurs,
            showDistribution, format, pipelineAggregators(), getMetaData());
    }

    @Override
    public Object getProperty(List<String> path) {
        if (path.isEmpty()) {
            return this;
        } else if (path.size() == 1) {
            return value(path.get(0));
        } else {
            throw new IllegalArgumentException("path not supported for [" + getName() + "]: " + path);
        }
    }

    static class Fields {
        public static final String COUNT = "count";
        public static final String MIN_LENGTH = "min_length";
        public static final String MIN_LENGTH_AS_STRING = "min_length_as_string";
        public static final String MAX_LENGTH = "max_length";
        public static final String MAX_LENGTH_AS_STRING = "max_as_string";
        public static final String AVG_LENGTH = "avg_length";
        public static final String AVG_LENGTH_AS_STRING = "avg_length_as_string";
        public static final String ENTROPY = "entropy";
        public static final String ENTROPY_AS_STRING = "entropy_string";
        public static final String DISTRIBUTION = "distribution";
        public static final String DISTRIBUTION_AS_STRING = "distribution_string";
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(Fields.COUNT, count);
        if (count > 0) {
            builder.field(Fields.MIN_LENGTH, minLength);
            builder.field(Fields.MAX_LENGTH, maxLength);
            builder.field(Fields.AVG_LENGTH, getAvgLength());
            builder.field(Fields.ENTROPY, getEntropy());
            if (showDistribution == true) {
                builder.field(Fields.DISTRIBUTION, getDistribution());
            }
            if (format != DocValueFormat.RAW) {
                builder.field(Fields.MIN_LENGTH_AS_STRING, format.format(getMinLength()));
                builder.field(Fields.MAX_LENGTH_AS_STRING, format.format(getMaxLength()));
                builder.field(Fields.AVG_LENGTH_AS_STRING, format.format(getAvgLength()));
                builder.field(Fields.ENTROPY_AS_STRING, format.format(getEntropy()));
                if (showDistribution == true) {
                    builder.startObject(Fields.DISTRIBUTION_AS_STRING);
                    for (Map.Entry<String, Double> e: getDistribution().entrySet()) {
                        builder.field(e.getKey(), format.format(e.getValue()).toString());
                    }
                    builder.endObject();
                }
            }
        } else {
            builder.nullField(Fields.MIN_LENGTH);
            builder.nullField(Fields.MAX_LENGTH);
            builder.nullField(Fields.AVG_LENGTH);
            builder.field(Fields.ENTROPY, 0.0d);

            if (showDistribution == true) {
                builder.nullField(Fields.DISTRIBUTION);
            }
        }
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), count, minLength, maxLength, totalLength, charOccurrences, showDistribution);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        InternalStringStats other = (InternalStringStats) obj;
        return count == other.count &&
            minLength == other.minLength &&
            maxLength == other.maxLength &&
            totalLength == other.totalLength &&
            showDistribution == other.showDistribution;
    }
}
