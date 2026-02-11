/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus.rest;

import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;

import java.util.List;
import java.util.Map;

public class PrometheusRemoteWriteRestActionTests extends ESTestCase {

    public void testMediaTypesValidAcceptsProtobuf() {
        var action = new PrometheusRemoteWriteRestAction();
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_prometheus/api/v1/write")
            .withHeaders(Map.of("Content-Type", List.of("application/x-protobuf")))
            .build();
        assertTrue(action.mediaTypesValid(request));
    }

    public void testMediaTypesValidRejectsNonProtobuf() {
        var action = new PrometheusRemoteWriteRestAction();
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_prometheus/api/v1/write")
            .withHeaders(Map.of("Content-Type", List.of("application/json")))
            .build();
        assertFalse(action.mediaTypesValid(request));
    }
}
