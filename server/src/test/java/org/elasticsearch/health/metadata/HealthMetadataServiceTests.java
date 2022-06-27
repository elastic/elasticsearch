/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.metadata;

import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HealthMetadataServiceTests extends ESTestCase {

    private ClusterService clusterService;
    private HealthMetadataService healthMetadataService;

    @Before
    public void setup() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);healthMetadataService = new HealthMetadataService(
            new DiskThresholdSettings(Settings.EMPTY, clusterSettings),
            clusterService,
            Settings.EMPTY
        );
    }

    public void testFirstAttempt() {
        healthMetadataService.submitHealthMetadata("first-attempt", 0);
        verify(clusterService, times(1)).submitStateUpdateTask(eq("first-attempt"), any(), any(), any());
    }

    public void testRetry() {
        healthMetadataService.submitHealthMetadata("first-attempt", 1);
        verify(clusterService, times(1)).submitStateUpdateTask(eq("retry[1]-first-attempt"), any(), any(), any());
    }

    public void testNoMoreRetries() {
        healthMetadataService.submitHealthMetadata("no-more", 5);
        verify(clusterService, never()).submitStateUpdateTask(any(), any(), any(), any());
    }
}
