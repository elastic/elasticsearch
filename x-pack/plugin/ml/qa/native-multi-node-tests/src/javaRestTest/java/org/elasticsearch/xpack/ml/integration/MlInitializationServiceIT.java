/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.MlDailyMaintenanceService;
import org.elasticsearch.xpack.ml.MlInitializationService;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Stream;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_INDEX_HIDDEN;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MlInitializationServiceIT extends MlNativeAutodetectIntegTestCase {

    private ThreadPool threadPool;
    private MlInitializationService mlInitializationService;

    @Before
    public void setUpMocks() {
        threadPool = mock(ThreadPool.class);
        when(threadPool.executor(ThreadPool.Names.SAME)).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        when(threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME)).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        MlDailyMaintenanceService mlDailyMaintenanceService = mock(MlDailyMaintenanceService.class);
        ClusterService clusterService = mock(ClusterService.class);
        mlInitializationService = new MlInitializationService(client(), threadPool, mlDailyMaintenanceService, clusterService);
    }

    public void testThatMlIndicesBecomeHiddenWhenTheNodeBecomesMaster() throws Exception {
        String[] mlHiddenIndexNames = {
            ".ml-anomalies-7",
            ".ml-state-000001",
            ".ml-stats-000001",
            ".ml-notifications-000002",
            ".ml-annotations-6"
        };
        String[] otherIndexNames = { "some-index-1", "some-other-index-2" };
        String[] allIndexNames = Stream.concat(Arrays.stream(mlHiddenIndexNames), Arrays.stream(otherIndexNames)).toArray(String[]::new);

        for (String indexName : mlHiddenIndexNames) {
            try {
                assertAcked(prepareCreate(indexName).setSettings(Collections.singletonMap(SETTING_INDEX_HIDDEN, randomBoolean())));
            } catch (ResourceAlreadyExistsException e) {
                logger.info("Index " + indexName + "already exists: {}", e.getDetailedMessage());
            }
        }
        createIndex(otherIndexNames);

        GetSettingsResponse settingsResponse =
            client().admin().indices().prepareGetSettings(allIndexNames)
                .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED_HIDDEN)
                .get();
        assertThat(settingsResponse, is(notNullValue()));
        for (String indexName : mlHiddenIndexNames) {
            Settings settings = settingsResponse.getIndexToSettings().get(indexName);
            assertThat(settings, is(notNullValue()));
        }
        for (String indexName : otherIndexNames) {
            Settings settings = settingsResponse.getIndexToSettings().get(indexName);
            assertThat(settings, is(notNullValue()));
            assertThat(
                "Index " + indexName + " expected not to be hidden but was",
                settings.getAsBoolean(SETTING_INDEX_HIDDEN, false), is(equalTo(false)));
        }

        mlInitializationService.onMaster();
        assertBusy(() -> assertTrue(mlInitializationService.areMlInternalIndicesHidden()));

        settingsResponse =
            client().admin().indices().prepareGetSettings(allIndexNames)
                .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED_HIDDEN)
                .get();
        assertThat(settingsResponse, is(notNullValue()));
        for (String indexName : mlHiddenIndexNames) {
            Settings settings = settingsResponse.getIndexToSettings().get(indexName);
            assertThat(settings, is(notNullValue()));
            assertThat(
                "Index " + indexName + " expected to be hidden but wasn't, settings = " + settings,
                settings.getAsBoolean(SETTING_INDEX_HIDDEN, false), is(equalTo(true)));
        }
        for (String indexName : otherIndexNames) {
            Settings settings = settingsResponse.getIndexToSettings().get(indexName);
            assertThat(settings, is(notNullValue()));
            assertThat(
                "Index " + indexName + " expected not to be hidden but was, settings = " + settings,
                settings.getAsBoolean(SETTING_INDEX_HIDDEN, false), is(equalTo(false)));
        }
    }

    @Override
    public Settings indexSettings() {
        return Settings.builder().put(super.indexSettings())
            .put(IndexMetadata.SETTING_DATA_PATH, (String) null)
            .build();
    }
}
