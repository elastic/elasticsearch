/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class InferenceFeatureServiceTests extends ESTestCase {

    private static final NodeFeature TEST_FEATURE = new NodeFeature("test_feature");

    private ThreadPool threadPool;
    private ClusterService clusterService;
    private FeatureService featureService;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = createThreadPool();
        clusterService = createClusterService(threadPool);
        featureService = mock(FeatureService.class);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        clusterService.close();
        terminate(threadPool);
    }

    public void testHasEndpointMetadataFeatureReturnsTrueWhenClusterHasFeature() {
        when(featureService.clusterHasFeature(any(), eq(TEST_FEATURE))).thenReturn(true);

        var inferenceFeatureService = new InferenceFeatureService(clusterService, featureService);

        assertTrue(inferenceFeatureService.hasFeature(TEST_FEATURE));

        verify(featureService).clusterHasFeature(clusterService.state(), TEST_FEATURE);
    }

    public void testHasFeatureReturnsFalseWhenClusterDoesNotHaveFeature() {
        when(featureService.clusterHasFeature(any(), eq(TEST_FEATURE))).thenReturn(false);

        var inferenceFeatureService = new InferenceFeatureService(clusterService, featureService);

        assertFalse(inferenceFeatureService.hasFeature(TEST_FEATURE));

        verify(featureService).clusterHasFeature(clusterService.state(), TEST_FEATURE);
    }

    public void testConstructorThrowsWhenClusterServiceIsNull() {
        expectThrows(NullPointerException.class, () -> new InferenceFeatureService(null, featureService));
    }

    public void testConstructorThrowsWhenFeatureServiceIsNull() {
        expectThrows(NullPointerException.class, () -> new InferenceFeatureService(clusterService, null));
    }
}
