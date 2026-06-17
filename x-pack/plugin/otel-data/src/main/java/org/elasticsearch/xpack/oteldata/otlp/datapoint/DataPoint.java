/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.datapoint;

import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.AggregationTemporality;
import io.opentelemetry.proto.metrics.v1.ExponentialHistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.HistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.SummaryDataPoint;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.oteldata.otlp.docbuilder.HistogramMapping;
import org.elasticsearch.xpack.oteldata.otlp.docbuilder.MappingHints;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static io.opentelemetry.proto.metrics.v1.AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE;
import static io.opentelemetry.proto.metrics.v1.AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA;

/**
 * Represents a metrics data point in the OpenTelemetry metrics data model.
 * This interface defines methods to access various properties of a data point,
 * such as its timestamp, attributes, unit, metric name, and methods to build
 * the metric value in a specific format.
 * The reason this class is needed is that the generated classes from the
 * OpenTelemetry proto definitions don't implement a common interface,
 * which makes it difficult to handle different types of data points uniformly.
 */
public interface DataPoint {

    /**
     * Returns the timestamp of the data point in Unix nanoseconds.
     *
     * @return the timestamp in nanoseconds
     */
    long getTimestampUnixNano();

    /**
     * Returns the start timestamp of the data point in Unix nanoseconds.
     * This allows detecting when a sequence of  observations is unbroken.
     * This field indicates to consumers the start time for points with cumulative and delta temporality,
     * and can support correct rate calculation.
     *
     * @return the start timestamp in nanoseconds
     */
    long getStartTimestampUnixNano();

    /**
     * Returns the attributes associated with the data point.
     *
     * @return a list of key-value pairs representing the attributes
     */
    List<KeyValue> getAttributes();

    /**
     * Returns the unit of measurement for the data point.
     *
     * @return the unit as a string
     */
    String getUnit();

    /**
     * Returns the name of the metric associated with the data point.
     *
     * @return the metric name as a string
     */
    String getMetricName();

    /**
     * Builds the metric value for the data point and writes it to the provided XContentBuilder.
     *
     * @param mappingHints hints for building the metric value
     * @param builder the XContentBuilder to write the metric value to
     * @param scratch a reusable, temporary buffer for building exponential histograms
     * @throws IOException if an I/O error occurs while writing to the builder
     */
    void buildMetricValue(MappingHints mappingHints, XContentBuilder builder, ExponentialHistogramConverter.BucketBuffer scratch)
        throws IOException;

    /**
     * Returns the dynamic template name for the data point based on its type and value.
     * This is used to dynamically map the appropriate field type according to the data point's characteristics.
     *
     * @param mappingHints hints for building the dynamic template
     * @return the dynamic template name as a string
     */
    String getDynamicTemplate(MappingHints mappingHints);

    /**
     * Returns the aggregation temporality of this data point.
     * Returns the OTLP {@link AggregationTemporality} for metric types that have one (sums, histograms),
     * or {@code null} for metric types without temporality (e.g. gauges, summaries).
     *
     * @return the aggregation temporality, or null
     */
    @Nullable
    AggregationTemporality getTemporality();

    /**
     * Validates whether the data point can be indexed into Elasticsearch.
     *
     * @param errors a set to collect validation error messages
     * @param mappingHints the effective mapping hints for this data point
     * @return true if the data point is valid, false otherwise
     */
    boolean isValid(Set<String> errors, MappingHints mappingHints);

    /**
     * Returns the {@code _doc_count} for the data point.
     * This is used when {@link MappingHints#docCount()} is true.
     *
     * @return the {@code _doc_count}
     */
    long getDocCount();

    record Number(NumberDataPoint dataPoint, Metric metric) implements DataPoint {

        @Override
        public long getTimestampUnixNano() {
            return dataPoint.getTimeUnixNano();
        }

        @Override
        public List<KeyValue> getAttributes() {
            return dataPoint.getAttributesList();
        }

        @Override
        public long getStartTimestampUnixNano() {
            return dataPoint.getStartTimeUnixNano();
        }

        @Override
        public String getUnit() {
            return metric.getUnit();
        }

        @Override
        public String getMetricName() {
            return metric.getName();
        }

        @Override
        public void buildMetricValue(MappingHints mappingHints, XContentBuilder builder, ExponentialHistogramConverter.BucketBuffer scratch)
            throws IOException {
            switch (dataPoint.getValueCase()) {
                case AS_DOUBLE -> builder.value(dataPoint.getAsDouble());
                case AS_INT -> builder.value(dataPoint.getAsInt());
            }
        }

        @Override
        public long getDocCount() {
            return 1;
        }

        @Override
        public String getDynamicTemplate(MappingHints mappingHints) {
            String type;
            if (metric.hasSum() && metric.getSum().getIsMonotonic()) {
                if (metric.getSum().getAggregationTemporality() == AGGREGATION_TEMPORALITY_DELTA
                    && IndexSettings.TIME_SERIES_TEMPORALITY_FEATURE_FLAG.isEnabled() == false) {
                    type = "gauge_";
                } else {
                    type = "counter_";
                }
            } else {
                // TODO add support for up/down counters - for now we represent them as gauges
                type = "gauge_";
            }
            if (dataPoint.getValueCase() == NumberDataPoint.ValueCase.AS_INT) {
                return type + "long";
            } else if (dataPoint.getValueCase() == NumberDataPoint.ValueCase.AS_DOUBLE) {
                return type + "double";
            } else {
                return null;
            }
        }

        @Override
        public @Nullable AggregationTemporality getTemporality() {
            if (metric.hasSum()) {
                return switch (metric.getSum().getAggregationTemporality()) {
                    case AGGREGATION_TEMPORALITY_CUMULATIVE -> AGGREGATION_TEMPORALITY_CUMULATIVE;
                    case AGGREGATION_TEMPORALITY_DELTA -> AGGREGATION_TEMPORALITY_DELTA;
                    default -> null;
                };
            }
            return null;
        }

        @Override
        public boolean isValid(Set<String> errors, MappingHints mappingHints) {
            return true;
        }
    }

    record ExponentialHistogram(ExponentialHistogramDataPoint dataPoint, Metric metric) implements DataPoint {

        @Override
        public long getTimestampUnixNano() {
            return dataPoint.getTimeUnixNano();
        }

        @Override
        public List<KeyValue> getAttributes() {
            return dataPoint.getAttributesList();
        }

        @Override
        public long getStartTimestampUnixNano() {
            return dataPoint.getStartTimeUnixNano();
        }

        @Override
        public String getUnit() {
            return metric.getUnit();
        }

        @Override
        public String getMetricName() {
            return metric.getName();
        }

        @Override
        public void buildMetricValue(MappingHints mappingHints, XContentBuilder builder, ExponentialHistogramConverter.BucketBuffer scratch)
            throws IOException {
            switch (mappingHints.histogramMapping()) {
                case AGGREGATE_METRIC_DOUBLE -> buildAggregateMetricDouble(builder, dataPoint.getSum(), dataPoint.getCount());
                case TDIGEST -> buildTDigest(builder);
                case HISTOGRAM_RAW -> buildRawHistogram(builder);
                case EXPONENTIAL_HISTOGRAM -> ExponentialHistogramConverter.buildExponentialHistogram(dataPoint, builder);
            }
        }

        private void buildRawHistogram(XContentBuilder builder) throws IOException {
            builder.startObject();
            builder.startArray("counts");
            RawHistogramConverter.counts(dataPoint, builder::value);
            builder.endArray();
            builder.startArray("values");
            RawHistogramConverter.values(dataPoint, builder::value);
            builder.endArray();
            builder.endObject();
        }

        private void buildTDigest(XContentBuilder builder) throws IOException {
            builder.startObject();
            builder.startArray("counts");
            TDigestConverter.counts(dataPoint, builder::value);
            builder.endArray();
            builder.startArray("values");
            TDigestConverter.centroidValues(dataPoint, builder::value);
            builder.endArray();
            builder.endObject();
        }

        @Override
        public long getDocCount() {
            return dataPoint.getCount();
        }

        @Override
        public String getDynamicTemplate(MappingHints mappingHints) {
            return getHistogramDynamicTemplate(mappingHints);
        }

        @Override
        public @Nullable AggregationTemporality getTemporality() {
            return switch (metric.getExponentialHistogram().getAggregationTemporality()) {
                case AGGREGATION_TEMPORALITY_CUMULATIVE -> AGGREGATION_TEMPORALITY_CUMULATIVE;
                case AGGREGATION_TEMPORALITY_DELTA -> AGGREGATION_TEMPORALITY_DELTA;
                default -> null;
            };
        }

        @Override
        public boolean isValid(Set<String> errors, MappingHints mappingHints) {
            if (metric.getExponentialHistogram().getAggregationTemporality() == AGGREGATION_TEMPORALITY_CUMULATIVE) {
                if (IndexSettings.TIME_SERIES_TEMPORALITY_FEATURE_FLAG.isEnabled() == false) {
                    errors.add("cumulative exponential histogram metrics are not supported, ignoring " + metric.getName());
                    return false;
                } else if (mappingHints.histogramMapping() != HistogramMapping.EXPONENTIAL_HISTOGRAM) {
                    errors.add(
                        "cumulative exponential histogram metrics are only supported when stored as exponential_histogram, ignoring "
                            + metric.getName()
                    );
                    return false;
                }
            }
            return true;
        }
    }

    record Histogram(HistogramDataPoint dataPoint, Metric metric) implements DataPoint {
        @Override
        public long getTimestampUnixNano() {
            return dataPoint.getTimeUnixNano();
        }

        @Override
        public List<KeyValue> getAttributes() {
            return dataPoint.getAttributesList();
        }

        @Override
        public long getStartTimestampUnixNano() {
            return dataPoint.getStartTimeUnixNano();
        }

        @Override
        public String getUnit() {
            return metric.getUnit();
        }

        @Override
        public String getMetricName() {
            return metric.getName();
        }

        @Override
        public void buildMetricValue(MappingHints mappingHints, XContentBuilder builder, ExponentialHistogramConverter.BucketBuffer scratch)
            throws IOException {
            switch (mappingHints.histogramMapping()) {
                case EXPONENTIAL_HISTOGRAM -> ExponentialHistogramConverter.buildExponentialHistogram(
                    dataPoint,
                    metric.getHistogram().getAggregationTemporality(),
                    builder,
                    scratch
                );
                case TDIGEST -> buildTDigest(builder);
                case HISTOGRAM_RAW -> buildRawHistogram(builder);
                case AGGREGATE_METRIC_DOUBLE -> buildAggregateMetricDouble(builder, dataPoint.getSum(), dataPoint.getCount());
            }
        }

        private void buildRawHistogram(XContentBuilder builder) throws IOException {
            builder.startObject();
            builder.startArray("counts");
            RawHistogramConverter.counts(dataPoint, builder::value);
            builder.endArray();
            builder.startArray("values");
            RawHistogramConverter.values(dataPoint, builder::value);
            builder.endArray();
            builder.endObject();
        }

        private void buildTDigest(XContentBuilder builder) throws IOException {
            builder.startObject();
            builder.startArray("counts");
            TDigestConverter.counts(dataPoint, builder::value);
            builder.endArray();
            builder.startArray("values");
            TDigestConverter.centroidValues(dataPoint, builder::value);
            builder.endArray();
            builder.endObject();
        }

        @Override
        public long getDocCount() {
            return dataPoint.getCount();
        }

        @Override
        public String getDynamicTemplate(MappingHints mappingHints) {
            return getHistogramDynamicTemplate(mappingHints);
        }

        @Override
        public @Nullable AggregationTemporality getTemporality() {
            return switch (metric.getHistogram().getAggregationTemporality()) {
                case AGGREGATION_TEMPORALITY_CUMULATIVE -> AGGREGATION_TEMPORALITY_CUMULATIVE;
                case AGGREGATION_TEMPORALITY_DELTA -> AGGREGATION_TEMPORALITY_DELTA;
                default -> null;
            };
        }

        @Override
        public boolean isValid(Set<String> errors, MappingHints mappingHints) {
            if (metric.getHistogram().getAggregationTemporality() == AGGREGATION_TEMPORALITY_CUMULATIVE) {
                if (IndexSettings.TIME_SERIES_TEMPORALITY_FEATURE_FLAG.isEnabled() == false) {
                    errors.add("cumulative histogram metrics are not supported, ignoring " + metric.getName());
                    return false;
                } else if (mappingHints.histogramMapping() != HistogramMapping.EXPONENTIAL_HISTOGRAM) {
                    errors.add(
                        "cumulative histogram metrics are only supported when stored as exponential_histogram, ignoring " + metric.getName()
                    );
                    return false;
                }
            }
            int bucketCountsCount = dataPoint.getBucketCountsCount();
            int explicitBoundsCount = dataPoint.getExplicitBoundsCount();
            if (bucketCountsCount > 0 && bucketCountsCount != explicitBoundsCount + 1) {
                errors.add("histogram bucket count must be one greater than explicit bounds count, ignoring " + metric.getName());
                return false;
            }
            for (int i = 1; i < explicitBoundsCount; i++) {
                if (dataPoint.getExplicitBounds(i - 1) >= dataPoint.getExplicitBounds(i)) {
                    errors.add("histogram bounds are not sorted or not unique, ignoring " + metric.getName());
                    return false;
                }
            }
            return true;
        }
    }

    private static String getHistogramDynamicTemplate(MappingHints mappingHints) {
        return switch (mappingHints.histogramMapping()) {
            case AGGREGATE_METRIC_DOUBLE -> "summary";
            case TDIGEST, HISTOGRAM_RAW -> "histogram";
            case EXPONENTIAL_HISTOGRAM -> "exponential_histogram";
        };
    }

    record Summary(SummaryDataPoint dataPoint, Metric metric) implements DataPoint {

        @Override
        public long getTimestampUnixNano() {
            return dataPoint.getTimeUnixNano();
        }

        @Override
        public List<KeyValue> getAttributes() {
            return dataPoint.getAttributesList();
        }

        @Override
        public long getStartTimestampUnixNano() {
            return dataPoint.getStartTimeUnixNano();
        }

        @Override
        public String getUnit() {
            return metric.getUnit();
        }

        @Override
        public String getMetricName() {
            return metric.getName();
        }

        @Override
        public void buildMetricValue(MappingHints mappingHints, XContentBuilder builder, ExponentialHistogramConverter.BucketBuffer scratch)
            throws IOException {
            // TODO: Add support for quantiles
            buildAggregateMetricDouble(builder, dataPoint.getSum(), dataPoint.getCount());
        }

        @Override
        public long getDocCount() {
            return dataPoint.getCount();
        }

        @Override
        public String getDynamicTemplate(MappingHints mappingHints) {
            return "summary";
        }

        @Override
        public @Nullable AggregationTemporality getTemporality() {
            return null;
        }

        @Override
        public boolean isValid(Set<String> errors, MappingHints mappingHints) {
            return true;
        }
    }

    private static void buildAggregateMetricDouble(XContentBuilder builder, double sum, long valueCount) throws IOException {
        builder.startObject();
        builder.field("sum", sum);
        builder.field("value_count", valueCount);
        builder.endObject();
    }
}
