/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.exception.ResourceAlreadyExistsException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.MlDailyMaintenanceService;
import org.elasticsearch.xpack.ml.MlInitializationService;
import org.elasticsearch.xpack.ml.inference.adaptiveallocations.AdaptiveAllocationsScalerService;
import org.junit.Before;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_INDEX_HIDDEN;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MlInitializationServiceIT extends MlNativeAutodetectIntegTestCase {

    private MlInitializationService mlInitializationService;

    @Before
    public void setUpMocks() {
        final var threadPool = mock(ThreadPool.class);
        when(threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME)).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        MlDailyMaintenanceService mlDailyMaintenanceService = mock(MlDailyMaintenanceService.class);
        ClusterService clusterService = mock(ClusterService.class);
        AdaptiveAllocationsScalerService adaptiveAllocationsScalerService = mock(AdaptiveAllocationsScalerService.class);
        mlInitializationService = new MlInitializationService(
            client(),
            threadPool,
            mlDailyMaintenanceService,
            adaptiveAllocationsScalerService,
            clusterService
        );
    }

    public void testThatMlIndicesBecomeHiddenWhenTheNodeBecomesMaster() throws Exception {
        List<String> mlHiddenIndexNames = List.of(
            ".ml-anomalies-7",
            ".ml-state-000001",
            ".ml-stats-000001",
            ".ml-notifications-000002",
            ".ml-annotations-000001"
        );
        List<String> otherIndexNames = List.of("some-index-1", "some-other-index-2");
        List<String> allIndexNames = Stream.concat(mlHiddenIndexNames.stream(), otherIndexNames.stream()).collect(toList());

        for (String indexName : mlHiddenIndexNames) {
            try {
                createIndex(indexName, Settings.builder().put(SETTING_INDEX_HIDDEN, randomBoolean()).build());
            } catch (ResourceAlreadyExistsException e) {
                logger.info("Index " + indexName + " already exists: {}", e.getDetailedMessage());
            }
        }
        for (String indexName : otherIndexNames) {
            createIndex(indexName, Settings.EMPTY);
        }

        Map<String, Settings> indexToSettings = getIndexToSettingsMap(allIndexNames);
        for (String indexName : mlHiddenIndexNames) {
            assertThat(indexToSettings.get(indexName), is(notNullValue()));
        }
        for (String indexName : otherIndexNames) {
            Settings settings = indexToSettings.get(indexName);
            assertThat(settings, is(notNullValue()));
            assertThat(
                "Index " + indexName + " expected not to be hidden but was",
                settings.getAsBoolean(SETTING_INDEX_HIDDEN, false),
                is(equalTo(false))
            );
        }

        assertFalse(mlInitializationService.areMlInternalIndicesHidden());
        mlInitializationService.onMaster();
        assertBusy(() -> assertTrue(mlInitializationService.areMlInternalIndicesHidden()));

        indexToSettings = getIndexToSettingsMap(allIndexNames);
        for (String indexName : mlHiddenIndexNames) {
            Settings settings = indexToSettings.get(indexName);
            assertThat(settings, is(notNullValue()));
            assertThat(
                "Index " + indexName + " expected to be hidden but wasn't, settings = " + settings,
                settings.getAsBoolean(SETTING_INDEX_HIDDEN, false),
                is(equalTo(true))
            );
        }
        for (String indexName : otherIndexNames) {
            Settings settings = indexToSettings.get(indexName);
            assertThat(settings, is(notNullValue()));
            assertThat(
                "Index " + indexName + " expected not to be hidden but was, settings = " + settings,
                settings.getAsBoolean(SETTING_INDEX_HIDDEN, false),
                is(equalTo(false))
            );
        }
    }

    public void testThatMlAliasesBecomeHiddenWhenTheNodeBecomesMaster() throws Exception {
        List<String> mlHiddenIndexNames = List.of(
            ".ml-anomalies-7",
            ".ml-state-000001",
            ".ml-stats-000001",
            ".ml-notifications-000002",
            ".ml-annotations-000001"
        );
        for (String indexName : mlHiddenIndexNames) {
            try {
                createIndex(indexName, Settings.builder().put(SETTING_INDEX_HIDDEN, randomBoolean()).build());
            } catch (ResourceAlreadyExistsException e) {
                logger.info("Index " + indexName + " already exists: {}", e.getDetailedMessage());
            }
        }

        IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAliasAction(
            IndicesAliasesRequest.AliasActions.add().index(".ml-anomalies-7").alias(".ml-anomalies-write").writeIndex(true)
        )
            .addAliasAction(
                IndicesAliasesRequest.AliasActions.add()
                    .index(".ml-state-000001")
                    .alias(".ml-state-write")
                    .filter(QueryBuilders.termQuery("a", "b"))
            )
            .addAliasAction(
                IndicesAliasesRequest.AliasActions.add()
                    .index(".ml-stats-000001")
                    .alias(".ml-stats-write")
                    .indexRouting("some-index-routing")
            )
            .addAliasAction(
                IndicesAliasesRequest.AliasActions.add()
                    .index(".ml-notifications-000002")
                    .alias(".ml-notifications-write")
                    .searchRouting("some-search-routing")
            )
            .addAliasAction(
                IndicesAliasesRequest.AliasActions.add()
                    .index(".ml-annotations-000001")
                    .alias(".ml-annotations-write")
                    .writeIndex(true)
                    .filter(QueryBuilders.termQuery("a", "b"))
                    .indexRouting("some-index-routing")
                    .searchRouting("some-search-routing")
            );
        assertAcked(client().admin().indices().aliases(indicesAliasesRequest).get());

        assertFalse(mlInitializationService.areMlInternalIndicesHidden());
        mlInitializationService.onMaster();
        assertBusy(() -> assertTrue(mlInitializationService.areMlInternalIndicesHidden()));

        Map<String, List<AliasMetadata>> indexToAliasesMap = getIndexToAliasesMap(mlHiddenIndexNames);
        assertThat("Aliases were: " + indexToAliasesMap, indexToAliasesMap.size(), is(equalTo(5)));
        assertThat(
            indexToAliasesMap.get(".ml-anomalies-7"),
            contains(AliasMetadata.builder(".ml-anomalies-write").isHidden(true).writeIndex(true).build())
        );
        assertThat(
            indexToAliasesMap.get(".ml-state-000001"),
            contains(AliasMetadata.builder(".ml-state-write").isHidden(true).filter(QueryBuilders.termQuery("a", "b").toString()).build())
        );
        assertThat(
            indexToAliasesMap.get(".ml-stats-000001"),
            contains(AliasMetadata.builder(".ml-stats-write").isHidden(true).indexRouting("some-index-routing").build())
        );
        assertThat(
            indexToAliasesMap.get(".ml-notifications-000002"),
            contains(AliasMetadata.builder(".ml-notifications-write").isHidden(true).searchRouting("some-search-routing").build())
        );
        assertThat(
            indexToAliasesMap.get(".ml-annotations-000001"),
            contains(
                AliasMetadata.builder(".ml-annotations-read").isHidden(true).build(),
                AliasMetadata.builder(".ml-annotations-write")
                    .isHidden(true)
                    .writeIndex(true)
                    .filter(QueryBuilders.termQuery("a", "b").toString())
                    .indexRouting("some-index-routing")
                    .searchRouting("some-search-routing")
                    .build()
            )
        );
    }

    private static Map<String, Settings> getIndexToSettingsMap(List<String> indexNames) {
        GetSettingsResponse getSettingsResponse = indicesAdmin().prepareGetSettings(TEST_REQUEST_TIMEOUT)
            .setIndices(indexNames.toArray(String[]::new))
            .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED_HIDDEN)
            .get();
        assertThat(getSettingsResponse, is(notNullValue()));
        return getSettingsResponse.getIndexToSettings();
    }

    private static Map<String, List<AliasMetadata>> getIndexToAliasesMap(List<String> indexNames) {
        GetAliasesResponse getAliasesResponse = indicesAdmin().prepareGetAliases(TEST_REQUEST_TIMEOUT)
            .setIndices(indexNames.toArray(String[]::new))
            .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED_HIDDEN)
            .get();
        assertThat(getAliasesResponse, is(notNullValue()));
        return getAliasesResponse.getAliases();
    }

    @Override
    public Settings indexSettings() {
        return Settings.builder().put(super.indexSettings()).put(IndexMetadata.SETTING_DATA_PATH, (String) null).build();
    }
}
