/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus.proto;

import com.google.protobuf.InvalidProtocolBufferException;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.prometheus.proto.RemoteWriteProtos.Label;
import org.elasticsearch.xpack.prometheus.proto.RemoteWriteProtos.Sample;
import org.elasticsearch.xpack.prometheus.proto.RemoteWriteProtos.TimeSeries;
import org.elasticsearch.xpack.prometheus.proto.RemoteWriteProtos.WriteRequest;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

/**
 * Basic tests for the generated protobuf classes to ensure the protobuf generation
 * and source path configurations work correctly. These tests also serve as examples
 * of how to work with the generated protobuf classes.
 */
public class RemoteWriteProtosTests extends ESTestCase {

    /**
     * Tests building a Label message using the builder pattern.
     */
    public void testBuildLabel() {
        Label label = Label.newBuilder().setName("__name__").setValue("http_requests_total").build();

        assertThat(label.getName(), equalTo("__name__"));
        assertThat(label.getValue(), equalTo("http_requests_total"));
    }

    /**
     * Tests building a Sample message with a value and timestamp.
     */
    public void testBuildSample() {
        long timestamp = System.currentTimeMillis();
        double value = 42.5;

        Sample sample = Sample.newBuilder().setValue(value).setTimestamp(timestamp).build();

        assertThat(sample.getValue(), closeTo(value, 0.0001));
        assertThat(sample.getTimestamp(), equalTo(timestamp));
    }

    /**
     * Tests building a TimeSeries message with labels and samples.
     */
    public void testBuildTimeSeries() {
        Label nameLabel = Label.newBuilder().setName("__name__").setValue("cpu_usage").build();

        Label instanceLabel = Label.newBuilder().setName("instance").setValue("localhost:9090").build();

        Sample sample1 = Sample.newBuilder().setValue(0.75).setTimestamp(1000L).build();

        Sample sample2 = Sample.newBuilder().setValue(0.80).setTimestamp(2000L).build();

        TimeSeries timeSeries = TimeSeries.newBuilder()
            .addLabels(nameLabel)
            .addLabels(instanceLabel)
            .addSamples(sample1)
            .addSamples(sample2)
            .build();

        assertThat(timeSeries.getLabelsCount(), equalTo(2));
        assertThat(timeSeries.getLabels(0).getName(), equalTo("__name__"));
        assertThat(timeSeries.getLabels(1).getName(), equalTo("instance"));
        assertThat(timeSeries.getSamplesCount(), equalTo(2));
        assertThat(timeSeries.getSamples(0).getValue(), closeTo(0.75, 0.0001));
        assertThat(timeSeries.getSamples(1).getValue(), closeTo(0.80, 0.0001));
    }

    /**
     * Tests building a complete WriteRequest with multiple time series.
     */
    public void testBuildWriteRequest() {
        TimeSeries timeSeries1 = TimeSeries.newBuilder()
            .addLabels(Label.newBuilder().setName("__name__").setValue("metric1").build())
            .addSamples(Sample.newBuilder().setValue(1.0).setTimestamp(1000L).build())
            .build();

        TimeSeries timeSeries2 = TimeSeries.newBuilder()
            .addLabels(Label.newBuilder().setName("__name__").setValue("metric2").build())
            .addSamples(Sample.newBuilder().setValue(2.0).setTimestamp(2000L).build())
            .build();

        WriteRequest writeRequest = WriteRequest.newBuilder().addTimeseries(timeSeries1).addTimeseries(timeSeries2).build();

        assertThat(writeRequest.getTimeseriesCount(), equalTo(2));
        assertThat(writeRequest.getTimeseries(0).getLabels(0).getValue(), equalTo("metric1"));
        assertThat(writeRequest.getTimeseries(1).getLabels(0).getValue(), equalTo("metric2"));
    }

    /**
     * Tests serialization and deserialization of a WriteRequest to/from bytes.
     * This validates that the protobuf binary encoding works correctly.
     */
    public void testSerializeAndDeserialize() throws InvalidProtocolBufferException {
        WriteRequest original = WriteRequest.newBuilder()
            .addTimeseries(
                TimeSeries.newBuilder()
                    .addLabels(Label.newBuilder().setName("__name__").setValue("test_metric").build())
                    .addLabels(Label.newBuilder().setName("job").setValue("test_job").build())
                    .addSamples(Sample.newBuilder().setValue(123.456).setTimestamp(1234567890L).build())
                    .build()
            )
            .build();

        // Serialize to bytes
        byte[] serialized = original.toByteArray();

        // Deserialize from bytes
        WriteRequest deserialized = WriteRequest.parseFrom(serialized);

        // Verify the deserialized message matches the original
        assertThat(deserialized.getTimeseriesCount(), equalTo(1));
        TimeSeries ts = deserialized.getTimeseries(0);
        assertThat(ts.getLabelsCount(), equalTo(2));
        assertThat(ts.getLabels(0).getName(), equalTo("__name__"));
        assertThat(ts.getLabels(0).getValue(), equalTo("test_metric"));
        assertThat(ts.getLabels(1).getName(), equalTo("job"));
        assertThat(ts.getLabels(1).getValue(), equalTo("test_job"));
        assertThat(ts.getSamplesCount(), equalTo(1));
        assertThat(ts.getSamples(0).getValue(), closeTo(123.456, 0.0001));
        assertThat(ts.getSamples(0).getTimestamp(), equalTo(1234567890L));
    }

    /**
     * Tests that empty messages can be created and serialized.
     */
    public void testEmptyMessages() throws InvalidProtocolBufferException {
        WriteRequest emptyRequest = WriteRequest.newBuilder().build();
        assertThat(emptyRequest.getTimeseriesCount(), equalTo(0));

        byte[] serialized = emptyRequest.toByteArray();
        WriteRequest deserialized = WriteRequest.parseFrom(serialized);
        assertThat(deserialized.getTimeseriesCount(), equalTo(0));
    }

    /**
     * Tests that the default instance pattern works correctly.
     */
    public void testDefaultInstances() {
        assertThat(WriteRequest.getDefaultInstance().getTimeseriesCount(), equalTo(0));
        assertThat(TimeSeries.getDefaultInstance().getLabelsCount(), equalTo(0));
        assertThat(TimeSeries.getDefaultInstance().getSamplesCount(), equalTo(0));
        assertThat(Label.getDefaultInstance().getName(), equalTo(""));
        assertThat(Label.getDefaultInstance().getValue(), equalTo(""));
        assertThat(Sample.getDefaultInstance().getValue(), closeTo(0.0, 0.0001));
        assertThat(Sample.getDefaultInstance().getTimestamp(), equalTo(0L));
    }

    /**
     * Tests equality and hashCode for protobuf messages.
     */
    public void testEqualsAndHashCode() {
        Label label1 = Label.newBuilder().setName("name").setValue("value").build();
        Label label2 = Label.newBuilder().setName("name").setValue("value").build();
        Label label3 = Label.newBuilder().setName("name").setValue("different").build();

        assertThat(label1.equals(label2), equalTo(true));
        assertThat(label1.hashCode(), equalTo(label2.hashCode()));
        assertThat(label1.equals(label3), equalTo(false));
    }
}
