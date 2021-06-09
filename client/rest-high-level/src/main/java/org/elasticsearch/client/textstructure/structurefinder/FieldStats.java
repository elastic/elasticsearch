/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.textstructure.structurefinder;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class FieldStats implements ToXContentObject {

    public static final ParseField COUNT = new ParseField("count");
    public static final ParseField CARDINALITY = new ParseField("cardinality");
    public static final ParseField MIN_VALUE = new ParseField("min_value");
    public static final ParseField MAX_VALUE = new ParseField("max_value");
    public static final ParseField MEAN_VALUE = new ParseField("mean_value");
    public static final ParseField MEDIAN_VALUE = new ParseField("median_value");
    public static final ParseField EARLIEST = new ParseField("earliest");
    public static final ParseField LATEST = new ParseField("latest");
    public static final ParseField TOP_HITS = new ParseField("top_hits");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<FieldStats, Void> PARSER = new ConstructingObjectParser<>("field_stats", true,
        a -> new FieldStats((long) a[0], (int) a[1], (Double) a[2], (Double) a[3], (Double) a[4], (Double) a[5],
            (String) a[6], (String) a[7], (List<Map<String, Object>>) a[8]));

    static {
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), COUNT);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), CARDINALITY);
        PARSER.declareDouble(ConstructingObjectParser.optionalConstructorArg(), MIN_VALUE);
        PARSER.declareDouble(ConstructingObjectParser.optionalConstructorArg(), MAX_VALUE);
        PARSER.declareDouble(ConstructingObjectParser.optionalConstructorArg(), MEAN_VALUE);
        PARSER.declareDouble(ConstructingObjectParser.optionalConstructorArg(), MEDIAN_VALUE);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), EARLIEST);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), LATEST);
        PARSER.declareObjectArray(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> p.mapOrdered(), TOP_HITS);
    }

    private final long count;
    private final int cardinality;
    private final Double minValue;
    private final Double maxValue;
    private final Double meanValue;
    private final Double medianValue;
    private final String earliestTimestamp;
    private final String latestTimestamp;
    private final List<Map<String, Object>> topHits;

    FieldStats(long count, int cardinality, Double minValue, Double maxValue, Double meanValue, Double medianValue,
               String earliestTimestamp, String latestTimestamp, List<Map<String, Object>> topHits) {
        this.count = count;
        this.cardinality = cardinality;
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.meanValue = meanValue;
        this.medianValue = medianValue;
        this.earliestTimestamp = earliestTimestamp;
        this.latestTimestamp = latestTimestamp;
        this.topHits = (topHits == null) ? Collections.emptyList() : Collections.unmodifiableList(topHits);
    }

    public long getCount() {
        return count;
    }

    public int getCardinality() {
        return cardinality;
    }

    public Double getMinValue() {
        return minValue;
    }

    public Double getMaxValue() {
        return maxValue;
    }

    public Double getMeanValue() {
        return meanValue;
    }

    public Double getMedianValue() {
        return medianValue;
    }

    public String getEarliestTimestamp() {
        return earliestTimestamp;
    }

    public String getLatestTimestamp() {
        return latestTimestamp;
    }

    public List<Map<String, Object>> getTopHits() {
        return topHits;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {

        builder.startObject();
        builder.field(COUNT.getPreferredName(), count);
        builder.field(CARDINALITY.getPreferredName(), cardinality);
        if (minValue != null) {
            builder.field(MIN_VALUE.getPreferredName(), toIntegerIfInteger(minValue));
        }
        if (maxValue != null) {
            builder.field(MAX_VALUE.getPreferredName(), toIntegerIfInteger(maxValue));
        }
        if (meanValue != null) {
            builder.field(MEAN_VALUE.getPreferredName(), toIntegerIfInteger(meanValue));
        }
        if (medianValue != null) {
            builder.field(MEDIAN_VALUE.getPreferredName(), toIntegerIfInteger(medianValue));
        }
        if (earliestTimestamp != null) {
            builder.field(EARLIEST.getPreferredName(), earliestTimestamp);
        }
        if (latestTimestamp != null) {
            builder.field(LATEST.getPreferredName(), latestTimestamp);
        }
        if (topHits.isEmpty() == false) {
            builder.field(TOP_HITS.getPreferredName(), topHits);
        }
        builder.endObject();

        return builder;
    }

    static Number toIntegerIfInteger(double d) {

        if (d >= Integer.MIN_VALUE && d <= Integer.MAX_VALUE && Double.compare(d, StrictMath.rint(d)) == 0) {
            return (int) d;
        }

        return d;
    }

    @Override
    public int hashCode() {

        return Objects.hash(count, cardinality, minValue, maxValue, meanValue, medianValue, earliestTimestamp, latestTimestamp, topHits);
    }

    @Override
    public boolean equals(Object other) {

        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        FieldStats that = (FieldStats) other;
        return this.count == that.count &&
            this.cardinality == that.cardinality &&
            Objects.equals(this.minValue, that.minValue) &&
            Objects.equals(this.maxValue, that.maxValue) &&
            Objects.equals(this.meanValue, that.meanValue) &&
            Objects.equals(this.medianValue, that.medianValue) &&
            Objects.equals(this.earliestTimestamp, that.earliestTimestamp) &&
            Objects.equals(this.latestTimestamp, that.latestTimestamp) &&
            Objects.equals(this.topHits, that.topHits);
    }
}
