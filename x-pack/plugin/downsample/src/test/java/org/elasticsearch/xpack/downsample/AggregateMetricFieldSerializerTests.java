/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.apache.lucene.internal.hppc.IntArrayList;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.aggregatemetric.mapper.AggregateMetricDoubleFieldMapper;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.downsample.MetricFieldProducerTests.createValuesInstance;
import static org.hamcrest.Matchers.equalTo;

public class AggregateMetricFieldSerializerTests extends ESTestCase {

    public void testAggregatedGaugeFieldSerialization() throws IOException {
        MetricFieldProducer producer = new MetricFieldProducer.AggregateScalarMetricFieldProducer("my-gauge");
        var docIdBuffer = IntArrayList.from(0, 1, 2);
        var valuesInstance = createValuesInstance(docIdBuffer, 55.0, 12.2, 5.5);
        producer.collect(valuesInstance, docIdBuffer);
        AggregateMetricFieldSerializer gaugeFieldSerializer = new AggregateMetricFieldSerializer("my-gauge", List.of(producer));
        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            builder.humanReadable(true).startObject();
            gaugeFieldSerializer.write(builder);
            builder.endObject();
            assertThat(Strings.toString(builder), equalTo("{\"my-gauge\":{\"max\":55.0,\"min\":5.5,\"sum\":72.7,\"value_count\":3}}"));
        }
    }

    public void testCounterFieldSerialization() throws IOException {
        MetricFieldProducer producer = new MetricFieldProducer.LastValueScalarMetricFieldProducer("my-counter");
        var docIdBuffer = IntArrayList.from(0, 1, 2);
        var valuesInstance = createValuesInstance(docIdBuffer, 55, 12, 5);
        producer.collect(valuesInstance, docIdBuffer);
        AggregateMetricFieldSerializer gaugeFieldSerializer = new AggregateMetricFieldSerializer("my-counter", List.of(producer));
        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            builder.humanReadable(true).startObject();
            gaugeFieldSerializer.write(builder);
            builder.endObject();
            assertThat(Strings.toString(builder), equalTo("{\"my-counter\":{\"last_value\":55.0}}"));
        }
    }

    public void testAggregatePreAggregatedFieldSerialization() throws IOException {
        MetricFieldProducer minProducer = new MetricFieldProducer.AggregatePreAggregatedMetricFieldProducer(
            "my-gauge",
            AggregateMetricDoubleFieldMapper.Metric.min
        );
        var docIdBuffer = IntArrayList.from(0, 1);
        var valuesInstance = createValuesInstance(docIdBuffer, 10, 5.5);
        minProducer.collect(valuesInstance, docIdBuffer);
        MetricFieldProducer maxProducer = new MetricFieldProducer.AggregatePreAggregatedMetricFieldProducer(
            "my-gauge",
            AggregateMetricDoubleFieldMapper.Metric.max
        );
        docIdBuffer = IntArrayList.from(0, 1);
        valuesInstance = createValuesInstance(docIdBuffer, 30, 55.0);
        maxProducer.collect(valuesInstance, docIdBuffer);
        MetricFieldProducer sumProducer = new MetricFieldProducer.AggregatePreAggregatedMetricFieldProducer(
            "my-gauge",
            AggregateMetricDoubleFieldMapper.Metric.sum
        );
        docIdBuffer = IntArrayList.from(0, 1);
        valuesInstance = createValuesInstance(docIdBuffer, 30, 72.7);
        sumProducer.collect(valuesInstance, docIdBuffer);
        MetricFieldProducer countProducer = new MetricFieldProducer.AggregatePreAggregatedMetricFieldProducer(
            "my-gauge",
            AggregateMetricDoubleFieldMapper.Metric.value_count
        );
        docIdBuffer = IntArrayList.from(0, 1);
        valuesInstance = createValuesInstance(docIdBuffer, 2, 3);
        countProducer.collect(valuesInstance, docIdBuffer);
        AggregateMetricFieldSerializer gaugeFieldSerializer = new AggregateMetricFieldSerializer(
            "my-gauge",
            List.of(maxProducer, minProducer, sumProducer, countProducer)
        );
        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            builder.humanReadable(true).startObject();
            gaugeFieldSerializer.write(builder);
            builder.endObject();
            assertThat(Strings.toString(builder), equalTo("{\"my-gauge\":{\"max\":55.0,\"min\":5.5,\"sum\":102.7,\"value_count\":5}}"));
        }
    }

    public void testLastValuePreAggregatedFieldSerialization() throws IOException {
        MetricFieldProducer minProducer = new MetricFieldProducer.LastValueScalarMetricFieldProducer(
            "my-gauge",
            AggregateMetricDoubleFieldMapper.Metric.min
        );
        var docIdBuffer = IntArrayList.from(0, 1);
        var valuesInstance = createValuesInstance(docIdBuffer, 10, 5.5);
        minProducer.collect(valuesInstance, docIdBuffer);
        MetricFieldProducer maxProducer = new MetricFieldProducer.LastValueScalarMetricFieldProducer(
            "my-gauge",
            AggregateMetricDoubleFieldMapper.Metric.max
        );
        docIdBuffer = IntArrayList.from(0, 1);
        valuesInstance = createValuesInstance(docIdBuffer, 30, 55.0);
        maxProducer.collect(valuesInstance, docIdBuffer);
        MetricFieldProducer sumProducer = new MetricFieldProducer.LastValueScalarMetricFieldProducer(
            "my-gauge",
            AggregateMetricDoubleFieldMapper.Metric.sum
        );
        docIdBuffer = IntArrayList.from(0, 1);
        valuesInstance = createValuesInstance(docIdBuffer, 30, 72.7);
        sumProducer.collect(valuesInstance, docIdBuffer);
        MetricFieldProducer countProducer = new MetricFieldProducer.LastValueScalarMetricFieldProducer(
            "my-gauge",
            AggregateMetricDoubleFieldMapper.Metric.value_count
        );
        docIdBuffer = IntArrayList.from(0, 1);
        valuesInstance = createValuesInstance(docIdBuffer, 2, 3);
        countProducer.collect(valuesInstance, docIdBuffer);
        AggregateMetricFieldSerializer gaugeFieldSerializer = new AggregateMetricFieldSerializer(
            "my-gauge",
            List.of(maxProducer, minProducer, sumProducer, countProducer)
        );
        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            builder.humanReadable(true).startObject();
            gaugeFieldSerializer.write(builder);
            builder.endObject();
            assertThat(Strings.toString(builder), equalTo("{\"my-gauge\":{\"max\":30.0,\"min\":10.0,\"sum\":30.0,\"value_count\":2.0}}"));
        }
    }
}
