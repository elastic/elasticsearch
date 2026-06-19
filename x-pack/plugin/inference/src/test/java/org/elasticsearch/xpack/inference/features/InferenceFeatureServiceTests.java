/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.features;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.InferenceFeatures;
import org.junit.After;
import org.junit.Before;

import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityExecutors;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class InferenceFeatureServiceTests extends ESTestCase {

    private ThreadPool threadPool;
    private ClusterService clusterService;
    private FeatureService featureService;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = createThreadPool(inferenceUtilityExecutors());
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
        when(featureService.clusterHasFeature(any(), eq(InferenceFeatures.ENDPOINT_METADATA_FIELD))).thenReturn(true);

        var inferenceFeatureService = new InferenceFeatureService(clusterService, featureService);

        assertTrue(inferenceFeatureService.hasFeature(InferenceFeatures.ENDPOINT_METADATA_FIELD));

        verify(featureService).clusterHasFeature(clusterService.state(), InferenceFeatures.ENDPOINT_METADATA_FIELD);
    }

    public void testHasFeatureReturnsFalseWhenClusterDoesNotHaveFeature() {
        when(featureService.clusterHasFeature(any(), eq(InferenceFeatures.ENDPOINT_METADATA_FIELD))).thenReturn(false);

        var inferenceFeatureService = new InferenceFeatureService(clusterService, featureService);

        assertFalse(inferenceFeatureService.hasFeature(InferenceFeatures.ENDPOINT_METADATA_FIELD));

        verify(featureService).clusterHasFeature(clusterService.state(), InferenceFeatures.ENDPOINT_METADATA_FIELD);
    }

    public void testConstructorThrowsWhenClusterServiceIsNull() {
        expectThrows(NullPointerException.class, () -> new InferenceFeatureService(null, featureService));
    }

    public void testConstructorThrowsWhenFeatureServiceIsNull() {
        expectThrows(NullPointerException.class, () -> new InferenceFeatureService(clusterService, null));
    }
}
