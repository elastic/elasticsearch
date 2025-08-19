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
     * @param messages a set to collect validation error messages
     * @return true if the data point is valid, false otherwise
     */
    boolean isValid(Set<String> messages);

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
            String prefix = metric.getDataCase() == Metric.DataCase.SUM ? "counter_" : "gauge_";
            if (dataPoint.getValueCase() == NumberDataPoint.ValueCase.AS_INT) {
                return prefix + "long";
            } else if (dataPoint.getValueCase() == NumberDataPoint.ValueCase.AS_DOUBLE) {
                return prefix + "double";
            } else {
                return null;
            }
        }

        @Override
        public boolean isValid(Set<String> messages) {
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
            if (mappingHints.aggregateMetricDouble()) {
                builder.startObject();
                builder.field("sum", dataPoint.getSum());
                builder.field("value_count", dataPoint.getCount());
                builder.endObject();
            } else {
                builder.startObject();
                builder.startArray("counts");
                HistogramConverter.counts(dataPoint, builder::value);
                builder.endArray();
                builder.startArray("values");
                HistogramConverter.centroidValues(dataPoint, builder::value);
                builder.endArray();
                builder.endObject();
            }
        }

        @Override
        public long getDocCount() {
            return dataPoint.getCount();
        }

        @Override
        public String getDynamicTemplate(MappingHints mappingHints) {
            if (mappingHints.aggregateMetricDouble()) {
                return "summary";
            } else {
                return "histogram";
            }
        }

        @Override
        public boolean isValid(Set<String> messages) {
            boolean valid = metric.getExponentialHistogram()
                .getAggregationTemporality() == AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA;
            if (valid == false) {
                messages.add("cumulative exponential histogram metrics are not supported, ignoring " + metric.getName());
            }
            return valid;
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
            if (mappingHints.aggregateMetricDouble()) {
                builder.startObject();
                builder.field("sum", dataPoint.getSum());
                builder.field("value_count", dataPoint.getCount());
                builder.endObject();
            } else {
                builder.startObject();
                builder.startArray("counts");
                HistogramConverter.counts(dataPoint, builder::value);
                builder.endArray();
                builder.startArray("values");
                HistogramConverter.centroidValues(dataPoint, builder::value);
                builder.endArray();
                builder.endObject();
            }
        }

        @Override
        public long getDocCount() {
            return dataPoint.getCount();
        }

        @Override
        public String getDynamicTemplate(MappingHints mappingHints) {
            if (mappingHints.aggregateMetricDouble()) {
                return "summary";
            } else {
                return "histogram";
            }
        }

        @Override
        public boolean isValid(Set<String> messages) {
            boolean valid = metric.getHistogram().getAggregationTemporality() == AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA;
            if (valid == false) {
                messages.add("cumulative histogram metrics are not supported, ignoring " + metric.getName());
            }
            return valid;
        }
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
            builder.startObject();
            builder.field("sum", dataPoint.getSum());
            builder.field("value_count", dataPoint.getCount());
            builder.endObject();
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
        public boolean isValid(Set<String> messages) {
            return true;
        }
    }
}
