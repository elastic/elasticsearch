/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.analytics.stringstats;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.CompensatedSum;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class InternalStringStats extends InternalAggregation {

    enum Metrics {
        count {
            Object getFieldValue(InternalStringStats stats) {
                return stats.getCount();
            }
        },
        min_length {
            Object getFieldValue(InternalStringStats stats) {
                return stats.getMinLength();
            }
        }, max_length {
            Object getFieldValue(InternalStringStats stats) {
                return stats.getMaxLength();
            }
        },
        avg_length {
            Object getFieldValue(InternalStringStats stats) {
                return stats.getAvgLength();
            }
        },
        entropy {
            Object getFieldValue(InternalStringStats stats) {
                return stats.getEntropy();
            }
        };

        abstract Object getFieldValue(InternalStringStats stats);
    }

    private final DocValueFormat format;
    private final boolean showDistribution;
    private final long count;
    private final long totalLength;
    private final int minLength;
    private final int maxLength;
    private final Map<String, Long> charOccurrences;

    public InternalStringStats(String name, long count, long totalLength, int minLength, int maxLength,
                               Map<String, Long> charOccurences, boolean showDistribution,
                               DocValueFormat formatter,
                               Map<String, Object> metadata) {
        super(name, metadata);
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
        charOccurrences = in.readMap(StreamInput::readString, StreamInput::readLong);
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
        return StringStatsAggregationBuilder.NAME;
    }

    public long getCount() {
        return count;
    }

    long getTotalLength () {
        return totalLength;
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
        // Compute the sum of double values with Kahan summation algorithm which is more
        // accurate than naive summation.
        CompensatedSum kahanSummation = new CompensatedSum(0, 0);

        for (double p : getDistribution().values()) {
            if (p > 0) {
                double value = p * log2(p);
                kahanSummation.add(value);
            }
        }
        return -kahanSummation.value();
    }

    /**
     * Convert the character occurrences map to character frequencies.
     *
     * @return A map with the character as key and the probability of
     * this character to occur as value. The map is ordered by frequency descending.
     */
    Map<String, Double> getDistribution() {
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

    Map<String, Long> getCharOccurrences() {
        return charOccurrences;
    }

    boolean getShowDistribution() {
        return showDistribution;
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
        try {
            return Metrics.valueOf(name).getFieldValue(this);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unknown value [" + name + "] in string stats aggregation");
        }
    }

    @Override
    public InternalStringStats reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
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
            showDistribution, format, getMetadata());
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
        public static final ParseField COUNT = new ParseField("count");
        public static final ParseField MIN_LENGTH = new ParseField("min_length");
        public static final ParseField MIN_LENGTH_AS_STRING = new ParseField("min_length_as_string");
        public static final ParseField MAX_LENGTH = new ParseField("max_length");
        public static final ParseField MAX_LENGTH_AS_STRING = new ParseField("max_as_string");
        public static final ParseField AVG_LENGTH = new ParseField("avg_length");
        public static final ParseField AVG_LENGTH_AS_STRING = new ParseField("avg_length_as_string");
        public static final ParseField ENTROPY = new ParseField("entropy");
        public static final ParseField ENTROPY_AS_STRING = new ParseField("entropy_string");
        public static final ParseField DISTRIBUTION = new ParseField("distribution");
        public static final ParseField DISTRIBUTION_AS_STRING = new ParseField("distribution_string");
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(Fields.COUNT.getPreferredName(), count);
        if (count > 0) {
            builder.field(Fields.MIN_LENGTH.getPreferredName(), minLength);
            builder.field(Fields.MAX_LENGTH.getPreferredName(), maxLength);
            builder.field(Fields.AVG_LENGTH.getPreferredName(), getAvgLength());
            builder.field(Fields.ENTROPY.getPreferredName(), getEntropy());
            if (showDistribution) {
                builder.field(Fields.DISTRIBUTION.getPreferredName(), getDistribution());
            }
            if (format != DocValueFormat.RAW) {
                builder.field(Fields.MIN_LENGTH_AS_STRING.getPreferredName(), format.format(getMinLength()));
                builder.field(Fields.MAX_LENGTH_AS_STRING.getPreferredName(), format.format(getMaxLength()));
                builder.field(Fields.AVG_LENGTH_AS_STRING.getPreferredName(), format.format(getAvgLength()));
                builder.field(Fields.ENTROPY_AS_STRING.getPreferredName(), format.format(getEntropy()));
                if (showDistribution) {
                    builder.startObject(Fields.DISTRIBUTION_AS_STRING.getPreferredName());
                    for (Map.Entry<String, Double> e: getDistribution().entrySet()) {
                        builder.field(e.getKey(), format.format(e.getValue()).toString());
                    }
                    builder.endObject();
                }
            }
        } else {
            builder.nullField(Fields.MIN_LENGTH.getPreferredName());
            builder.nullField(Fields.MAX_LENGTH.getPreferredName());
            builder.nullField(Fields.AVG_LENGTH.getPreferredName());
            builder.field(Fields.ENTROPY.getPreferredName(), 0.0);

            if (showDistribution) {
                builder.nullField(Fields.DISTRIBUTION.getPreferredName());
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
            Objects.equals(charOccurrences, other.charOccurrences) &&
            showDistribution == other.showDistribution;
    }
}
