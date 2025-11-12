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

import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.oteldata.otlp.docbuilder.MappingHints;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static io.opentelemetry.proto.metrics.v1.AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE;

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
     * @throws IOException if an I/O error occurs while writing to the builder
     */
    void buildMetricValue(MappingHints mappingHints, XContentBuilder builder) throws IOException;

    /**
     * Returns the dynamic template name for the data point based on its type and value.
     * This is used to dynamically map the appropriate field type according to the data point's characteristics.
     *
     * @param mappingHints hints for building the dynamic template
     * @return the dynamic template name as a string
     */
    String getDynamicTemplate(MappingHints mappingHints);

    /**
     * Validates whether the data point can be indexed into Elasticsearch.
     *
     * @param errors a set to collect validation error messages
     * @return true if the data point is valid, false otherwise
     */
    boolean isValid(Set<String> errors);

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
        public void buildMetricValue(MappingHints mappingHints, XContentBuilder builder) throws IOException {
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
            if (metric.hasSum()
                // TODO add support for delta counters - for now we represent them as gauges
                && metric.getSum().getAggregationTemporality() == AGGREGATION_TEMPORALITY_CUMULATIVE
                // TODO add support for up/down counters - for now we represent them as gauges
                && metric.getSum().getIsMonotonic()) {
                type = "counter_";
            } else {
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
        public boolean isValid(Set<String> errors) {
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
        public void buildMetricValue(MappingHints mappingHints, XContentBuilder builder) throws IOException {
            switch (mappingHints.histogramMapping()) {
                case AGGREGATE_METRIC_DOUBLE -> buildAggregateMetricDouble(builder, dataPoint.getSum(), dataPoint.getCount());
                case TDIGEST -> buildTDigest(builder);
                case EXPONENTIAL_HISTOGRAM -> ExponentialHistogramConverter.buildExponentialHistogram(dataPoint, builder);
            }
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
        public boolean isValid(Set<String> errors) {
            if (metric.getExponentialHistogram().getAggregationTemporality() != AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA) {
                errors.add("cumulative exponential histogram metrics are not supported, ignoring " + metric.getName());
                return false;
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
        public void buildMetricValue(MappingHints mappingHints, XContentBuilder builder) throws IOException {
            switch (mappingHints.histogramMapping()) {
                // TODO: reuse scratch
                case EXPONENTIAL_HISTOGRAM -> ExponentialHistogramConverter.buildExponentialHistogram(dataPoint, builder, new ExponentialHistogramConverter.BucketBuffer());
                case TDIGEST -> buildTDigest(builder);
                case AGGREGATE_METRIC_DOUBLE -> buildAggregateMetricDouble(builder, dataPoint.getSum(), dataPoint.getCount());
            }
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
        public boolean isValid(Set<String> errors) {
            if (metric.getHistogram().getAggregationTemporality() != AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA) {
                errors.add("cumulative histogram metrics are not supported, ignoring " + metric.getName());
                return false;
            }
            if (dataPoint.getBucketCountsCount() == 1 && dataPoint.getExplicitBoundsCount() == 0) {
                errors.add("histogram with a single bucket and no explicit bounds is not supported, ignoring " + metric.getName());
                return false;
            }
            return true;
        }
    }

    private static String getHistogramDynamicTemplate(MappingHints mappingHints) {
        return switch (mappingHints.histogramMapping()) {
            case AGGREGATE_METRIC_DOUBLE -> "summary";
            case TDIGEST -> "histogram";
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
        public void buildMetricValue(MappingHints mappingHints, XContentBuilder builder) throws IOException {
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
        public boolean isValid(Set<String> errors) {
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
