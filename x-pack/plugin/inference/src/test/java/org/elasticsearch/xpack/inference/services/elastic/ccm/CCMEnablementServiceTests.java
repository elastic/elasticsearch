/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.ccm;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.support.TestPlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.xpack.inference.services.elastic.ccm.CCMFeature.CCM_FORBIDDEN_EXCEPTION;
import static org.elasticsearch.xpack.inference.services.elastic.ccm.CCMFeature.CCM_UNSUPPORTED_UNTIL_UPGRADED_EXCEPTION;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CCMEnablementServiceTests extends ESTestCase {

    public void testSetEnabled_ReturnsError_WhenUnsupportedEnvironment() {
        var mockCCMFeature = mock(CCMFeature.class);
        when(mockCCMFeature.isCcmSupportedEnvironment()).thenReturn(false);
        var ccmEnablementService = new CCMEnablementService(mock(ClusterService.class), mock(FeatureService.class), mockCCMFeature);

        var listener = new TestPlainActionFuture<AcknowledgedResponse>();
        ccmEnablementService.setEnabled(ProjectId.DEFAULT, true, listener);
        var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TimeValue.THIRTY_SECONDS));

        assertThat(exception, is(CCM_FORBIDDEN_EXCEPTION));
    }

    public void testSetEnabled_ReturnsError_WhenClusterIsNotUpgradedFully() {
        var mockClusterService = mock(ClusterService.class);
        when(mockClusterService.state()).thenReturn(ClusterState.EMPTY_STATE);

        var mockFeatureService = mock(FeatureService.class);
        when(mockFeatureService.clusterHasFeature(any(), any())).thenReturn(false);

        var mockCCMFeature = mock(CCMFeature.class);
        when(mockCCMFeature.isCcmSupportedEnvironment()).thenReturn(true);
        var ccmEnablementService = new CCMEnablementService(mockClusterService, mockFeatureService, mockCCMFeature);

        var listener = new TestPlainActionFuture<AcknowledgedResponse>();
        ccmEnablementService.setEnabled(ProjectId.DEFAULT, true, listener);
        var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TimeValue.THIRTY_SECONDS));

        assertThat(exception, is(CCM_UNSUPPORTED_UNTIL_UPGRADED_EXCEPTION));
    }

    public void testIsEnabled_ReturnsFalse_WhenUnsupportedEnvironment() {
        var mockCCMFeature = mock(CCMFeature.class);
        when(mockCCMFeature.isCcmSupportedEnvironment()).thenReturn(false);
        var ccmEnablementService = new CCMEnablementService(mock(ClusterService.class), mock(FeatureService.class), mockCCMFeature);

        assertFalse(ccmEnablementService.isEnabled(ProjectId.DEFAULT));
    }

    public void testIsEnabled_ReturnsFalse_WhenClusterIsNotUpgradedFully() {
        var mockClusterService = mock(ClusterService.class);
        when(mockClusterService.state()).thenReturn(ClusterState.EMPTY_STATE);

        var mockFeatureService = mock(FeatureService.class);
        when(mockFeatureService.clusterHasFeature(any(), any())).thenReturn(true);

        var mockCCMFeature = mock(CCMFeature.class);
        when(mockCCMFeature.isCcmSupportedEnvironment()).thenReturn(false);
        var ccmEnablementService = new CCMEnablementService(mockClusterService, mockFeatureService, mockCCMFeature);

        assertFalse(ccmEnablementService.isEnabled(ProjectId.DEFAULT));
    }

    public void testIsEnabled_ReturnsFalse_WhenConfigurationNotEnabledYet() {
        var state = ClusterState.builder(ClusterState.EMPTY_STATE)
            .putProjectMetadata(
                ProjectMetadata.builder(ProjectId.DEFAULT)
                    .putCustom(CCMEnablementService.EnablementMetadata.NAME, new CCMEnablementService.EnablementMetadata(false))
                    .build()
            )
            .build();

        var mockClusterService = mock(ClusterService.class);
        when(mockClusterService.state()).thenReturn(state);

        var mockFeatureService = mock(FeatureService.class);
        when(mockFeatureService.clusterHasFeature(any(), any())).thenReturn(true);

        var mockCCMFeature = mock(CCMFeature.class);
        when(mockCCMFeature.isCcmSupportedEnvironment()).thenReturn(true);
        var ccmEnablementService = new CCMEnablementService(mockClusterService, mockFeatureService, mockCCMFeature);

        assertFalse(ccmEnablementService.isEnabled(ProjectId.DEFAULT));
    }

    public void testIsEnabled_ReturnsTrue_WhenConfigurationExists() {
        var state = ClusterState.builder(ClusterState.EMPTY_STATE)
            .putProjectMetadata(
                ProjectMetadata.builder(ProjectId.DEFAULT)
                    .putCustom(CCMEnablementService.EnablementMetadata.NAME, new CCMEnablementService.EnablementMetadata(true))
                    .build()
            )
            .build();

        var mockClusterService = mock(ClusterService.class);
        when(mockClusterService.state()).thenReturn(state);

        var mockFeatureService = mock(FeatureService.class);
        when(mockFeatureService.clusterHasFeature(any(), any())).thenReturn(true);

        var mockCCMFeature = mock(CCMFeature.class);
        when(mockCCMFeature.isCcmSupportedEnvironment()).thenReturn(true);
        var ccmEnablementService = new CCMEnablementService(mockClusterService, mockFeatureService, mockCCMFeature);

        assertTrue(ccmEnablementService.isEnabled(ProjectId.DEFAULT));
    }

    public void testIsEnabled_ReturnsFalse_WhenConfigurationDoesNotExist() {
        var mockClusterService = mock(ClusterService.class);
        when(mockClusterService.state()).thenReturn(ClusterState.EMPTY_STATE);

        var mockFeatureService = mock(FeatureService.class);
        when(mockFeatureService.clusterHasFeature(any(), any())).thenReturn(true);

        var mockCCMFeature = mock(CCMFeature.class);
        when(mockCCMFeature.isCcmSupportedEnvironment()).thenReturn(true);
        var ccmEnablementService = new CCMEnablementService(mockClusterService, mockFeatureService, mockCCMFeature);

        assertFalse(ccmEnablementService.isEnabled(ProjectId.DEFAULT));
    }
}
