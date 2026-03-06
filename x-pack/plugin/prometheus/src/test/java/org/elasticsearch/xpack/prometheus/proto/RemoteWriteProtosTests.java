/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus.proto;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.prometheus.proto.RemoteWrite.Label;
import org.elasticsearch.xpack.prometheus.proto.RemoteWrite.Sample;
import org.elasticsearch.xpack.prometheus.proto.RemoteWrite.TimeSeries;
import org.elasticsearch.xpack.prometheus.proto.RemoteWrite.WriteRequest;

import static org.hamcrest.Matchers.equalTo;

/**
 * Basic smoke test for the generated protobuf classes.
 */
public class RemoteWriteProtosTests extends ESTestCase {

    public void testWriteRequestParsing() throws Exception {
        WriteRequest request = WriteRequest.newBuilder()
            .addTimeseries(
                TimeSeries.newBuilder()
                    .addLabels(Label.newBuilder().setName("__name__").setValue("test_metric").build())
                    .addSamples(Sample.newBuilder().setValue(42.0).setTimestamp(1000L).build())
                    .build()
            )
            .build();

        WriteRequest parsed = WriteRequest.parseFrom(request.toByteArray());

        assertThat(parsed.getTimeseriesCount(), equalTo(1));
        assertThat(parsed.getTimeseries(0).getLabels(0).getValue(), equalTo("test_metric"));
        assertThat(parsed.getTimeseries(0).getSamples(0).getValue(), equalTo(42.0));
        assertThat(parsed.getTimeseries(0).getSamples(0).getTimestamp(), equalTo(1000L));
    }
}
