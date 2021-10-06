/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.StartDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfigTests;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfigUpdate;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsTaskState;
import org.elasticsearch.xpack.ml.MlSingleNodeTestCase;
import org.elasticsearch.xpack.ml.dataframe.persistence.DataFrameAnalyticsConfigProvider;
import org.elasticsearch.xpack.ml.notifications.DataFrameAnalyticsAuditor;
import org.junit.Before;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.emptyMap;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class DataFrameAnalyticsConfigProviderIT extends MlSingleNodeTestCase {

    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(5);

    private DataFrameAnalyticsConfigProvider configProvider;

    @Before
    public void createComponents() throws Exception {
        configProvider = new DataFrameAnalyticsConfigProvider(client(), xContentRegistry(),
            new DataFrameAnalyticsAuditor(client(), getInstanceFromNode(ClusterService.class)));
        waitForMlTemplates();
    }

    public void testGet_ConfigDoesNotExist() throws InterruptedException {
        AtomicReference<DataFrameAnalyticsConfig> configHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        blockingCall(actionListener -> configProvider.get("missing", actionListener), configHolder, exceptionHolder);

        assertThat(configHolder.get(), is(nullValue()));
        assertThat(exceptionHolder.get(), is(notNullValue()));
        assertThat(exceptionHolder.get(), is(instanceOf(ResourceNotFoundException.class)));
    }

    public void testPutAndGet() throws InterruptedException {
        String configId = "config-id";
        // Create valid config
        DataFrameAnalyticsConfig config = DataFrameAnalyticsConfigTests.createRandom(configId);
        {  // Put the config and verify the response
            AtomicReference<DataFrameAnalyticsConfig> configHolder = new AtomicReference<>();
            AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

            blockingCall(
                actionListener -> configProvider.put(config, emptyMap(), TIMEOUT, actionListener), configHolder, exceptionHolder);

            assertThat(configHolder.get(), is(notNullValue()));
            assertThat(configHolder.get(), is(equalTo(config)));
            assertThat(exceptionHolder.get(), is(nullValue()));
        }
        {  // Get the config back and verify the response
            AtomicReference<DataFrameAnalyticsConfig> configHolder = new AtomicReference<>();
            AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

            blockingCall(actionListener -> configProvider.get(configId, actionListener), configHolder, exceptionHolder);

            assertThat(configHolder.get(), is(notNullValue()));
            assertThat(configHolder.get(), is(equalTo(config)));
            assertThat(exceptionHolder.get(), is(nullValue()));
        }
    }

    public void testPutAndGet_WithSecurityHeaders() throws InterruptedException {
        String configId = "config-id";
        DataFrameAnalyticsConfig config = DataFrameAnalyticsConfigTests.createRandom(configId);
        Map<String, String> securityHeaders = Collections.singletonMap("_xpack_security_authentication", "dummy");
        {  // Put the config and verify the response
            AtomicReference<DataFrameAnalyticsConfig> configHolder = new AtomicReference<>();
            AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

            blockingCall(actionListener -> configProvider.put(config, securityHeaders, TIMEOUT, actionListener),
                configHolder, exceptionHolder);

            assertThat(configHolder.get(), is(notNullValue()));
            assertThat(
                configHolder.get(),
                is(equalTo(
                    new DataFrameAnalyticsConfig.Builder(config)
                        .setHeaders(securityHeaders)
                        .build())));
            assertThat(exceptionHolder.get(), is(nullValue()));
        }
        {  // Get the config back and verify the response
            AtomicReference<DataFrameAnalyticsConfig> configHolder = new AtomicReference<>();
            AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

            blockingCall(actionListener -> configProvider.get(configId, actionListener), configHolder, exceptionHolder);

            assertThat(configHolder.get(), is(notNullValue()));
            assertThat(
                configHolder.get(),
                is(equalTo(
                    new DataFrameAnalyticsConfig.Builder(config)
                        .setHeaders(securityHeaders)
                        .build())));
            assertThat(exceptionHolder.get(), is(nullValue()));
        }
    }

    public void testPut_ConfigAlreadyExists() throws InterruptedException {
        String configId = "config-id";
        {  // Put the config and verify the response
            AtomicReference<DataFrameAnalyticsConfig> configHolder = new AtomicReference<>();
            AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

            DataFrameAnalyticsConfig initialConfig = DataFrameAnalyticsConfigTests.createRandom(configId);
            blockingCall(
                actionListener -> configProvider.put(initialConfig, emptyMap(), TIMEOUT, actionListener), configHolder, exceptionHolder);

            assertThat(configHolder.get(), is(notNullValue()));
            assertThat(configHolder.get(), is(equalTo(initialConfig)));
            assertThat(exceptionHolder.get(), is(nullValue()));
        }
        {  // Try putting the config with the same id and verify the response
            AtomicReference<DataFrameAnalyticsConfig> configHolder = new AtomicReference<>();
            AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

            DataFrameAnalyticsConfig configWithSameId = DataFrameAnalyticsConfigTests.createRandom(configId);
            blockingCall(
                actionListener -> configProvider.put(configWithSameId, emptyMap(), TIMEOUT, actionListener),
                configHolder,
                exceptionHolder);

            assertThat(configHolder.get(), is(nullValue()));
            assertThat(exceptionHolder.get(), is(notNullValue()));
            assertThat(exceptionHolder.get(), is(instanceOf(ResourceAlreadyExistsException.class)));
        }
    }

    public void testUpdate() throws Exception {
        String configId = "config-id";
        DataFrameAnalyticsConfig initialConfig = DataFrameAnalyticsConfigTests.createRandom(configId);
        {
            AtomicReference<DataFrameAnalyticsConfig> configHolder = new AtomicReference<>();
            AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

            blockingCall(
                actionListener -> configProvider.put(initialConfig, emptyMap(), TIMEOUT, actionListener), configHolder, exceptionHolder);

            assertNoException(exceptionHolder);
            assertThat(configHolder.get(), is(notNullValue()));
            assertThat(configHolder.get(), is(equalTo(initialConfig)));
        }
        {   // Update that changes description
            AtomicReference<DataFrameAnalyticsConfig> updatedConfigHolder = new AtomicReference<>();
            AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

            DataFrameAnalyticsConfigUpdate configUpdate =
                new DataFrameAnalyticsConfigUpdate.Builder(configId)
                    .setDescription("description-1")
                    .build();

            blockingCall(
                actionListener -> configProvider.update(configUpdate, emptyMap(), ClusterState.EMPTY_STATE, actionListener),
                updatedConfigHolder,
                exceptionHolder);
            assertNoException(exceptionHolder);
            assertThat(updatedConfigHolder.get(), is(notNullValue()));
            assertThat(
                updatedConfigHolder.get(),
                is(equalTo(
                    new DataFrameAnalyticsConfig.Builder(initialConfig)
                        .setDescription("description-1")
                        .build())));
        }
        {   // Update that changes model memory limit
            AtomicReference<DataFrameAnalyticsConfig> updatedConfigHolder = new AtomicReference<>();
            AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

            DataFrameAnalyticsConfigUpdate configUpdate =
                new DataFrameAnalyticsConfigUpdate.Builder(configId)
                    .setModelMemoryLimit(ByteSizeValue.ofBytes(1024))
                    .build();

            blockingCall(
                actionListener -> configProvider.update(configUpdate, emptyMap(), ClusterState.EMPTY_STATE, actionListener),
                updatedConfigHolder,
                exceptionHolder);

            assertNoException(exceptionHolder);
            assertThat(updatedConfigHolder.get(), is(notNullValue()));
            assertThat(
                updatedConfigHolder.get(),
                is(equalTo(
                    new DataFrameAnalyticsConfig.Builder(initialConfig)
                        .setDescription("description-1")
                        .setModelMemoryLimit(ByteSizeValue.ofBytes(1024))
                        .build())));
        }
        {   // Noop update
            AtomicReference<DataFrameAnalyticsConfig> updatedConfigHolder = new AtomicReference<>();
            AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

            DataFrameAnalyticsConfigUpdate configUpdate = new DataFrameAnalyticsConfigUpdate.Builder(configId).build();

            blockingCall(
                actionListener -> configProvider.update(configUpdate, emptyMap(), ClusterState.EMPTY_STATE, actionListener),
                updatedConfigHolder,
                exceptionHolder);

            assertNoException(exceptionHolder);
            assertThat(updatedConfigHolder.get(), is(notNullValue()));
            assertThat(
                updatedConfigHolder.get(),
                is(equalTo(
                    new DataFrameAnalyticsConfig.Builder(initialConfig)
                        .setDescription("description-1")
                        .setModelMemoryLimit(ByteSizeValue.ofBytes(1024))
                        .build())));
        }
        {   // Update that changes both description and model memory limit
            AtomicReference<DataFrameAnalyticsConfig> updatedConfigHolder = new AtomicReference<>();
            AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

            DataFrameAnalyticsConfigUpdate configUpdate =
                new DataFrameAnalyticsConfigUpdate.Builder(configId)
                    .setDescription("description-2")
                    .setModelMemoryLimit(ByteSizeValue.ofBytes(2048))
                    .build();

            blockingCall(
                actionListener -> configProvider.update(configUpdate, emptyMap(), ClusterState.EMPTY_STATE, actionListener),
                updatedConfigHolder,
                exceptionHolder);

            assertNoException(exceptionHolder);
            assertThat(updatedConfigHolder.get(), is(notNullValue()));
            assertThat(
                updatedConfigHolder.get(),
                is(equalTo(
                    new DataFrameAnalyticsConfig.Builder(initialConfig)
                        .setDescription("description-2")
                        .setModelMemoryLimit(ByteSizeValue.ofBytes(2048))
                        .build())));
        }
        {  // Update that applies security headers
            Map<String, String> securityHeaders = Collections.singletonMap("_xpack_security_authentication", "dummy");

            AtomicReference<DataFrameAnalyticsConfig> updatedConfigHolder = new AtomicReference<>();
            AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

            DataFrameAnalyticsConfigUpdate configUpdate = new DataFrameAnalyticsConfigUpdate.Builder(configId).build();

            blockingCall(
                actionListener -> configProvider.update(configUpdate, securityHeaders, ClusterState.EMPTY_STATE, actionListener),
                updatedConfigHolder,
                exceptionHolder);

            assertNoException(exceptionHolder);
            assertThat(updatedConfigHolder.get(), is(notNullValue()));
            assertThat(
                updatedConfigHolder.get(),
                is(equalTo(
                    new DataFrameAnalyticsConfig.Builder(initialConfig)
                        .setDescription("description-2")
                        .setModelMemoryLimit(ByteSizeValue.ofBytes(2048))
                        .setHeaders(securityHeaders)
                        .build())));
        }
    }

    public void testUpdate_ConfigDoesNotExist() throws InterruptedException {
        AtomicReference<DataFrameAnalyticsConfig> updatedConfigHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        DataFrameAnalyticsConfigUpdate configUpdate = new DataFrameAnalyticsConfigUpdate.Builder("missing").build();

        blockingCall(
            actionListener -> configProvider.update(configUpdate, emptyMap(), ClusterState.EMPTY_STATE, actionListener),
            updatedConfigHolder,
            exceptionHolder);

        assertThat(updatedConfigHolder.get(), is(nullValue()));
        assertThat(exceptionHolder.get(), is(notNullValue()));
        assertThat(exceptionHolder.get(), is(instanceOf(ResourceNotFoundException.class)));
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/58814")
    public void testUpdate_UpdateCannotBeAppliedWhenTaskIsRunning() throws InterruptedException {
        String configId = "config-id";
        DataFrameAnalyticsConfig initialConfig = DataFrameAnalyticsConfigTests.createRandom(configId);
        {
            AtomicReference<DataFrameAnalyticsConfig> configHolder = new AtomicReference<>();
            AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

            blockingCall(
                actionListener -> configProvider.put(initialConfig, emptyMap(), TIMEOUT, actionListener), configHolder, exceptionHolder);

            assertThat(configHolder.get(), is(notNullValue()));
            assertThat(configHolder.get(), is(equalTo(initialConfig)));
            assertThat(exceptionHolder.get(), is(nullValue()));
        }
        {   // Update that tries to change model memory limit while the analytics is running
            AtomicReference<DataFrameAnalyticsConfig> updatedConfigHolder = new AtomicReference<>();
            AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

            DataFrameAnalyticsConfigUpdate configUpdate =
                new DataFrameAnalyticsConfigUpdate.Builder(configId)
                    .setModelMemoryLimit(ByteSizeValue.ofMb(2048))
                    .build();

            ClusterState clusterState = clusterStateWithRunningAnalyticsTask(configId, DataFrameAnalyticsState.ANALYZING);
            blockingCall(
                actionListener -> configProvider.update(configUpdate, emptyMap(), clusterState, actionListener),
                updatedConfigHolder,
                exceptionHolder);

            assertThat(updatedConfigHolder.get(), is(nullValue()));
            assertThat(exceptionHolder.get(), is(notNullValue()));
            assertThat(exceptionHolder.get(), is(instanceOf(ElasticsearchStatusException.class)));
            ElasticsearchStatusException e = (ElasticsearchStatusException) exceptionHolder.get();
            assertThat(e.status(), is(equalTo(RestStatus.CONFLICT)));
            assertThat(e.getMessage(), is(equalTo("Cannot update analytics [config-id] unless it's stopped")));
        }
    }

    private static ClusterState clusterStateWithRunningAnalyticsTask(String analyticsId, DataFrameAnalyticsState analyticsState) {
        PersistentTasksCustomMetadata.Builder builder = PersistentTasksCustomMetadata.builder();
        builder.addTask(
            MlTasks.dataFrameAnalyticsTaskId(analyticsId),
            MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME,
            new StartDataFrameAnalyticsAction.TaskParams(analyticsId, Version.CURRENT, false),
            new PersistentTasksCustomMetadata.Assignment("node", "test assignment"));
        builder.updateTaskState(
            MlTasks.dataFrameAnalyticsTaskId(analyticsId),
            new DataFrameAnalyticsTaskState(analyticsState, builder.getLastAllocationId(), null));
        PersistentTasksCustomMetadata tasks = builder.build();

        return ClusterState.builder(new ClusterName("cluster"))
            .metadata(Metadata.builder().putCustom(PersistentTasksCustomMetadata.TYPE, tasks).build())
            .build();
    }

}
