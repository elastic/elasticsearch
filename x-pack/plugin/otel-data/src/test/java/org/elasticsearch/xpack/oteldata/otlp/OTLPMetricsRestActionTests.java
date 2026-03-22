/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;

import com.google.protobuf.InvalidProtocolBufferException;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.index.IndexingPressure;

import static org.hamcrest.Matchers.equalTo;

public class OTLPMetricsRestActionTests extends AbstractOTLPRestActionTests {

    @Override
    protected AbstractOTLPRestAction createAction(IndexingPressure pressure, long maxRequestSizeBytes) {
        return new OTLPMetricsRestAction(pressure, maxRequestSizeBytes);
    }

    @Override
    protected ActionType<OTLPActionResponse> actionType() {
        return OTLPMetricsTransportAction.TYPE;
    }

    @Override
    protected OTLPActionResponse createSuccessResponse() {
        return new OTLPActionResponse(ExportMetricsServiceResponse.newBuilder().build());
    }

    @Override
    protected void assertEmptyResponse(byte[] responseBytes) throws InvalidProtocolBufferException {
        assertThat(ExportMetricsServiceResponse.parseFrom(responseBytes), equalTo(ExportMetricsServiceResponse.getDefaultInstance()));
    }

    @Override
    protected String routePath() {
        return "/_otlp/v1/metrics";
    }
}
