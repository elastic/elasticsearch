/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus.rest;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class PrometheusInstantQueryRestActionTests extends ESTestCase {

    private final PrometheusInstantQueryRestAction action = new PrometheusInstantQueryRestAction();

    public void testMissingQueryParamThrows() {
        var request = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withParams(Map.of()).build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, null));
        assertThat(e.getMessage(), equalTo("required parameter \"query\" is missing"));
    }
}
