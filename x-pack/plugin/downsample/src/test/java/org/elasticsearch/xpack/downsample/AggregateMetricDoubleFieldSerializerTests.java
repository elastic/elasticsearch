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

import static org.elasticsearch.xpack.downsample.NumericMetricFieldProducerTests.createNumericValuesInstance;
import static org.hamcrest.Matchers.equalTo;

public class AggregateMetricDoubleFieldSerializerTests extends ESTestCase {

    public void testAggregatedGaugeFieldSerialization() throws IOException {
        NumericMetricFieldProducer producer = new NumericMetricFieldProducer.AggregateGauge("my-gauge");
        var docIdBuffer = IntArrayList.from(0, 1, 2);
        var valuesInstance = createNumericValuesInstance(docIdBuffer, 55.0, 12.2, 5.5);
        producer.collect(valuesInstance, docIdBuffer);
        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            builder.humanReadable(true).startObject();
            producer.write(builder);
            builder.endObject();
            assertThat(Strings.toString(builder), equalTo("{\"my-gauge\":{\"min\":5.5,\"max\":55.0,\"sum\":72.7,\"value_count\":3}}"));
        }
        // Ensure that the AggregateMetricDouble producer serializer does not work on an aggregated gauge.
        AggregateMetricDoubleFieldProducer.Serializer aggregateMetricProducerSerializer = new AggregateMetricDoubleFieldProducer.Serializer(
            "my-gauge",
            List.of(producer)
        );
        XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent());
        builder.humanReadable(true).startObject();
        IllegalStateException error = expectThrows(IllegalStateException.class, () -> aggregateMetricProducerSerializer.write(builder));
        assertThat(error.getMessage(), equalTo("Unexpected field producer class: AggregateGauge for my-gauge field"));
    }

    public void testInvalidCounterFieldSerialization() throws IOException {
        NumericMetricFieldProducer producer = new NumericMetricFieldProducer.LastValue("my-counter");
        var docIdBuffer = IntArrayList.from(0, 1, 2);
        var valuesInstance = createNumericValuesInstance(docIdBuffer, 55, 12, 5);
        producer.collect(valuesInstance, docIdBuffer);
        AggregateMetricDoubleFieldProducer.Serializer gaugeFieldSerializer = new AggregateMetricDoubleFieldProducer.Serializer(
            "my-counter",
            List.of(producer)
        );
        XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent());
        builder.humanReadable(true).startObject();
        IllegalStateException error = expectThrows(IllegalStateException.class, () -> gaugeFieldSerializer.write(builder));
        assertThat(error.getMessage(), equalTo("Unexpected field producer class: LastValue for my-counter field"));
    }

    public void testAggregateMetricDoubleFieldSerialization() throws IOException {
        AggregateMetricDoubleFieldProducer minProducer = new AggregateMetricDoubleFieldProducer.Aggregate(
            "my-gauge",
            AggregateMetricDoubleFieldMapper.Metric.min
        );
        var docIdBuffer = IntArrayList.from(0, 1);
        var valuesInstance = createNumericValuesInstance(docIdBuffer, 10, 5.5);
        minProducer.collect(valuesInstance, docIdBuffer);
        AggregateMetricDoubleFieldProducer maxProducer = new AggregateMetricDoubleFieldProducer.Aggregate(
            "my-gauge",
            AggregateMetricDoubleFieldMapper.Metric.max
        );
        docIdBuffer = IntArrayList.from(0, 1);
        valuesInstance = createNumericValuesInstance(docIdBuffer, 30, 55.0);
        maxProducer.collect(valuesInstance, docIdBuffer);
        AggregateMetricDoubleFieldProducer sumProducer = new AggregateMetricDoubleFieldProducer.Aggregate(
            "my-gauge",
            AggregateMetricDoubleFieldMapper.Metric.sum
        );
        docIdBuffer = IntArrayList.from(0, 1);
        valuesInstance = createNumericValuesInstance(docIdBuffer, 30, 72.7);
        sumProducer.collect(valuesInstance, docIdBuffer);
        AggregateMetricDoubleFieldProducer countProducer = new AggregateMetricDoubleFieldProducer.Aggregate(
            "my-gauge",
            AggregateMetricDoubleFieldMapper.Metric.value_count
        );
        docIdBuffer = IntArrayList.from(0, 1);
        valuesInstance = createNumericValuesInstance(docIdBuffer, 2, 3);
        countProducer.collect(valuesInstance, docIdBuffer);
        AggregateMetricDoubleFieldProducer.Serializer gaugeFieldSerializer = new AggregateMetricDoubleFieldProducer.Serializer(
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
        AggregateMetricDoubleFieldProducer minProducer = randomAggregateSubMetricFieldProducer(
            "my-gauge",
            AggregateMetricDoubleFieldMapper.Metric.min
        );
        var docIdBuffer = IntArrayList.from(0, 1);
        var valuesInstance = createNumericValuesInstance(docIdBuffer, 10D, 5.5);
        minProducer.collect(valuesInstance, docIdBuffer);
        AggregateMetricDoubleFieldProducer maxProducer = randomAggregateSubMetricFieldProducer(
            "my-gauge",
            AggregateMetricDoubleFieldMapper.Metric.max
        );
        docIdBuffer = IntArrayList.from(0, 1);
        valuesInstance = createNumericValuesInstance(docIdBuffer, 30D, 55.0);
        maxProducer.collect(valuesInstance, docIdBuffer);
        AggregateMetricDoubleFieldProducer sumProducer = randomAggregateSubMetricFieldProducer(
            "my-gauge",
            AggregateMetricDoubleFieldMapper.Metric.sum
        );
        docIdBuffer = IntArrayList.from(0, 1);
        valuesInstance = createNumericValuesInstance(docIdBuffer, 30D, 72.7);
        sumProducer.collect(valuesInstance, docIdBuffer);
        AggregateMetricDoubleFieldProducer countProducer = randomAggregateSubMetricFieldProducer(
            "my-gauge",
            AggregateMetricDoubleFieldMapper.Metric.value_count
        );
        docIdBuffer = IntArrayList.from(0, 1);
        valuesInstance = createNumericValuesInstance(docIdBuffer, 2, 3);
        countProducer.collect(valuesInstance, docIdBuffer);
        AggregateMetricDoubleFieldProducer.Serializer gaugeFieldSerializer = new AggregateMetricDoubleFieldProducer.Serializer(
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

    /**
     * Serializing for a metric or a label shouldn't make a difference.
     */
    private AggregateMetricDoubleFieldProducer randomAggregateSubMetricFieldProducer(
        String name,
        AggregateMetricDoubleFieldMapper.Metric metric
    ) {
        return new AggregateMetricDoubleFieldProducer.LastValue(name, metric, randomBoolean());
    }
}
