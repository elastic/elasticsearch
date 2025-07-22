/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.inference.InferencePlugin;
import org.junit.Before;

import static org.elasticsearch.xpack.inference.Utils.mockClusterService;

public class InferenceTimeoutUtilsTests extends ESTestCase {

    private ClusterService clusterService;
    private static final TimeValue configuredTimeout = TimeValue.timeValueSeconds(10);

    @Before
    public void setUp() throws Exception {
        super.setUp();
        var settings = Settings.builder().put(InferencePlugin.INFERENCE_QUERY_TIMEOUT.getKey(), configuredTimeout).build();
        clusterService = mockClusterService(settings);
    }

    public void testResolveInferenceTimeout_WithProvidedTimeout_ReturnsProvidedTimeout() {
        var providedTimeout = TimeValue.timeValueSeconds(45);
        {
            var result = InferenceTimeoutUtils.resolveInferenceTimeout(providedTimeout, InputType.SEARCH, clusterService);
            assertEquals(providedTimeout, result);
        }
        {
            var result = InferenceTimeoutUtils.resolveInferenceTimeout(providedTimeout, InputType.INTERNAL_SEARCH, clusterService);
            assertEquals(providedTimeout, result);
        }
        {
            var result = InferenceTimeoutUtils.resolveInferenceTimeout(providedTimeout, InputType.INGEST, clusterService);
            assertEquals(providedTimeout, result);
        }
    }

    public void testResolveInferenceTimeout_WithNullTimeoutAndSearchInputType_ReturnsClusterSetting() {
        {
            var result = InferenceTimeoutUtils.resolveInferenceTimeout(null, InputType.SEARCH, clusterService);
            assertEquals(configuredTimeout, result);
        }
        {
            var result = InferenceTimeoutUtils.resolveInferenceTimeout(null, InputType.INTERNAL_SEARCH, clusterService);
            assertEquals(configuredTimeout, result);
        }
    }

    public void testResolveInferenceTimeout_WithNullTimeoutAndIngestInputType_ReturnsDefaultTimeout() {
        var result = InferenceTimeoutUtils.resolveInferenceTimeout(null, InputType.INGEST, clusterService);
        assertEquals(InferenceAction.Request.DEFAULT_TIMEOUT, result);
    }
}
