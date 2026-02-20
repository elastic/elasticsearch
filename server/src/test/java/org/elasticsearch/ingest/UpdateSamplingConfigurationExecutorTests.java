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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESTestCase;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.Map;
import java.util.function.LongSupplier;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.when;

public class UpdateSamplingConfigurationExecutorTests extends ESTestCase {

    @Mock
    private ProjectResolver projectResolver;

    @Mock
    private ClusterService clusterService;

    @Mock
    private ScriptService scriptService;

    @Mock
    private LongSupplier timeSupplier;

    @Mock
    private ClusterState clusterState;

    @Mock
    private Metadata metadata;

    private SamplingService.UpdateSamplingConfigurationExecutor executor;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);

        // Create a real SamplingService with mocked dependencies
        executor = new SamplingService.UpdateSamplingConfigurationExecutor();

        // Set up settings with MAX_CONFIGURATIONS_SETTING = 100 (the default value)
        Settings settings = Settings.builder().put("sampling.max_configurations", 100).build();

        // Mock the complete chain for checkMaxConfigLimitBreached() method:
        // clusterService.state() -> ClusterState
        when(clusterService.state()).thenReturn(clusterState);
        // currentState.metadata() -> Metadata
        when(clusterState.metadata()).thenReturn(metadata);
        // currentMetadata.settings() -> Settings (for MAX_CONFIGURATIONS_SETTING)
        when(metadata.settings()).thenReturn(settings);

        // Also mock for the executor's executeTask method
        when(clusterState.getMetadata()).thenReturn(metadata);
    }

    public void testExecuteTaskWithUpperBoundLimit() {
        // Create a project metadata with 100 existing sampling configurations (the maximum)
        Map<String, SamplingConfiguration> existingConfigs = new HashMap<>();
        SamplingConfiguration defaultConfig = new SamplingConfiguration(
            0.5d,
            50,
            ByteSizeValue.ofKb(100),
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

        // Mock both methods that get project metadata:
        // 1. For the executor's executeTask method: clusterState.metadata().getProject()
        when(metadata.getProject(ProjectId.DEFAULT)).thenReturn(projectMetadata);
        // 2. For the checkMaxConfigLimitBreached method: currentMetadata.getProject()
        // (this uses the same metadata mock, so the above mock covers both)

        // Try to add the 101st configuration - this should fail
        String newIndexName = "new-index-101";
        SamplingConfiguration newConfig = new SamplingConfiguration(0.8d, 80, ByteSizeValue.ofKb(100), TimeValue.timeValueHours(2), null);

        SamplingService.UpdateSamplingConfigurationTask task = new SamplingService.UpdateSamplingConfigurationTask(
            ProjectId.DEFAULT,
            newIndexName,
            newConfig,
            TimeValue.timeValueSeconds(30),
            null // ActionListener not needed for this test
        );

        // Execute the task and expect an IllegalStateException
        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> executor.executeTask(task, clusterState));

        // Verify the exception message contains expected information
        assertThat(exception.getMessage(), containsString("Cannot add sampling configuration for index [" + newIndexName + "]"));
        assertThat(exception.getMessage(), containsString("Maximum number of sampling configurations (100) already reached"));
    }

}
