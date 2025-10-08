/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.elasticsearch.action.admin.indices.sampling.SamplingConfiguration;
import org.elasticsearch.action.admin.indices.sampling.SamplingMetadata;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.when;

public class UpdateSamplingConfigurationExecutorTests extends ESTestCase {

    @Mock
    private ProjectResolver projectResolver;

    @Mock
    private ClusterState clusterState;

    @Mock
    private Metadata metadata;

    private SamplingService.UpdateSamplingConfigurationExecutor executor;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        executor = new SamplingService.UpdateSamplingConfigurationExecutor(projectResolver);
    }

    public void testExecuteTaskWithUpperBoundLimit() {
        // Create a project metadata with 100 existing sampling configurations (the maximum)
        Map<String, SamplingConfiguration> existingConfigs = new HashMap<>();
        SamplingConfiguration defaultConfig = new SamplingConfiguration(
            0.5d,
            50,
            ByteSizeValue.ofMb(10),
            TimeValue.timeValueHours(1),
            null
        );

        // Add 100 configurations to reach the limit
        for (int i = 0; i < 100; i++) {
            existingConfigs.put("existing-index-" + i, defaultConfig);
        }

        SamplingMetadata existingSamplingMetadata = new SamplingMetadata(existingConfigs);
        ProjectMetadata projectMetadata = ProjectMetadata.builder(ProjectId.DEFAULT)
            .putCustom(SamplingMetadata.TYPE, existingSamplingMetadata)
            .build();

        when(projectResolver.getProjectMetadata(clusterState)).thenReturn(projectMetadata);

        // Try to add the 101st configuration - this should fail
        String newIndexName = "new-index-101";
        SamplingConfiguration newConfig = new SamplingConfiguration(0.8d, 80, ByteSizeValue.ofMb(20), TimeValue.timeValueHours(2), null);

        SamplingService.UpdateSamplingConfigurationTask task = new SamplingService.UpdateSamplingConfigurationTask(
            ProjectId.DEFAULT,
            newIndexName,
            newConfig,
            TimeValue.timeValueSeconds(30),
            null // ActionListener not needed for this test
        );

        // Execute the task and expect an IllegalStateException
        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> { executor.executeTask(task, clusterState); });

        // Verify the exception message contains expected information
        assertThat(exception.getMessage(), containsString("Cannot add sampling configuration for index [" + newIndexName + "]"));
        assertThat(exception.getMessage(), containsString("Maximum number of sampling configurations (100) already reached"));
    }

    private void assertDoesNotThrow(Runnable runnable) {
        try {
            runnable.run();
        } catch (Exception e) {
            fail("Expected no exception but got: " + e.getClass().getSimpleName() + " with message: " + e.getMessage());
        }
    }
}
