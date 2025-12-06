/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TestPlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.inference.action.CCMEnabledActionResponse;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMFeature;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMService;
import org.junit.Before;

import static org.elasticsearch.xpack.inference.InferenceFeatures.INFERENCE_CCM_ENABLEMENT_SERVICE;
import static org.elasticsearch.xpack.inference.services.elastic.ccm.CCMFeature.CCM_UNSUPPORTED_UNTIL_UPGRADED_EXCEPTION;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportPutCCMConfigurationActionTests extends ESTestCase {

    private FeatureService featureService;
    private TransportPutCCMConfigurationAction action;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        var ccmFeature = mock(CCMFeature.class);
        when(ccmFeature.isCcmSupportedEnvironment()).thenReturn(true);

        featureService = mock(FeatureService.class);
        action = new TransportPutCCMConfigurationAction(
            mock(TransportService.class),
            mock(ClusterService.class),
            mock(ThreadPool.class),
            mock(ActionFilters.class),
            mock(CCMService.class),
            mock(ProjectResolver.class),
            ccmFeature,
            featureService
        );
    }

    public void testEnablementService_NotSupported() {
        when(featureService.clusterHasFeature(any(), eq(INFERENCE_CCM_ENABLEMENT_SERVICE))).thenReturn(false);
        var listener = new TestPlainActionFuture<CCMEnabledActionResponse>();
        action.masterOperation(null, null, ClusterState.EMPTY_STATE, listener);
        var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TimeValue.THIRTY_SECONDS));
        assertThat(exception, is(CCM_UNSUPPORTED_UNTIL_UPGRADED_EXCEPTION));
    }
}
