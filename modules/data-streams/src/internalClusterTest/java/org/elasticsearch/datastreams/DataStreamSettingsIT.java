/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams;

import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.readonly.AddIndexBlockRequest;
import org.elasticsearch.action.admin.indices.readonly.AddIndexBlockResponse;
import org.elasticsearch.action.admin.indices.rollover.RolloverAction;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverResponse;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.datastreams.GetDataStreamSettingsAction;
import org.elasticsearch.action.datastreams.UpdateDataStreamSettingsAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

public class DataStreamSettingsIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(DataStreamsPlugin.class, MockTransportService.TestPlugin.class, TestPlugin.class);
    }

    public void testPutDataStreamSettings() throws Exception {
        String dataStreamName = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        putComposableIndexTemplate("my-template", List.of(dataStreamName), indexSettings(1, 0).build());
        final var createDataStreamRequest = new CreateDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, dataStreamName);
        assertAcked(client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).actionGet());
        final int numberOfShards = randomIntBetween(2, 7);
        final String newLifecycleName = randomAlphanumericOfLength(20).toLowerCase(Locale.ROOT);
        {
            List<GetDataStreamSettingsAction.DataStreamSettingsResponse> getSettingsResponses = client().execute(
                GetDataStreamSettingsAction.INSTANCE,
                new GetDataStreamSettingsAction.Request(TimeValue.THIRTY_SECONDS).indices(dataStreamName)
            ).actionGet().getDataStreamSettingsResponses();
            assertThat(getSettingsResponses.size(), equalTo(1));
            assertThat(getSettingsResponses.get(0).settings(), equalTo(Settings.EMPTY));
            Settings dataStreamSettings = Settings.builder()
                .put("index.number_of_shards", numberOfShards)
                .put("index.lifecycle.name", newLifecycleName)
                .build();
            UpdateDataStreamSettingsAction.Response putSettingsResponse = client().execute(
                UpdateDataStreamSettingsAction.INSTANCE,
                new UpdateDataStreamSettingsAction.Request(dataStreamSettings, TimeValue.THIRTY_SECONDS, TimeValue.THIRTY_SECONDS).indices(
                    dataStreamName
                )
            ).actionGet();
            List<UpdateDataStreamSettingsAction.DataStreamSettingsResponse> dataStreamSettingsResponses = putSettingsResponse
                .getDataStreamSettingsResponses();
            assertThat(dataStreamSettingsResponses.size(), equalTo(1));
            UpdateDataStreamSettingsAction.DataStreamSettingsResponse dataStreamSettingsResponse = dataStreamSettingsResponses.get(0);
            assertThat(dataStreamSettingsResponse.dataStreamName(), equalTo(dataStreamName));
            assertThat(dataStreamSettingsResponse.dataStreamSucceeded(), equalTo(true));
            assertThat(dataStreamSettingsResponse.settings().get("index.number_of_shards"), equalTo(Integer.toString(numberOfShards)));
            assertThat(
                dataStreamSettingsResponse.effectiveSettings().get("index.number_of_shards"),
                equalTo(Integer.toString(numberOfShards))
            );
            assertThat(dataStreamSettingsResponse.indicesSettingsResult().indexSettingErrors().size(), equalTo(0));
            assertThat(dataStreamSettingsResponse.indicesSettingsResult().appliedToDataStreamOnly().size(), equalTo(1));
            assertThat(
                dataStreamSettingsResponse.indicesSettingsResult().appliedToDataStreamOnly().get(0),
                equalTo("index.number_of_shards")
            );
            assertThat(dataStreamSettingsResponse.indicesSettingsResult().appliedToDataStreamAndBackingIndices().size(), equalTo(1));
            assertThat(
                dataStreamSettingsResponse.indicesSettingsResult().appliedToDataStreamAndBackingIndices().get(0),
                equalTo("index.lifecycle.name")
            );
            GetIndexResponse response = admin().indices()
                .getIndex(new GetIndexRequest(TimeValue.THIRTY_SECONDS).indices(dataStreamName))
                .actionGet();
            Settings settings = response.getSettings().values().iterator().next();
            assertThat(settings.get("index.number_of_shards"), equalTo("1"));
            assertThat(settings.get("index.lifecycle.name"), equalTo(newLifecycleName));
            getSettingsResponses = client().execute(
                GetDataStreamSettingsAction.INSTANCE,
                new GetDataStreamSettingsAction.Request(TimeValue.THIRTY_SECONDS).indices(dataStreamName)
            ).actionGet().getDataStreamSettingsResponses();
            assertThat(getSettingsResponses.size(), equalTo(1));
            assertThat(getSettingsResponses.get(0).settings(), equalTo(dataStreamSettings));
            assertThat(
                getSettingsResponses.get(0).effectiveSettings(),
                equalTo(Settings.builder().put(dataStreamSettings).put("index.number_of_replicas", "0").build())
            );
        }
        {
            // Try to set an invalid value for a valid setting, and make sure the data stream is not updated
            int invalidNumberOfShards = 2000;
            UpdateDataStreamSettingsAction.Response putSettingsResponse = client().execute(
                UpdateDataStreamSettingsAction.INSTANCE,
                new UpdateDataStreamSettingsAction.Request(
                    Settings.builder().put("index.number_of_shards", invalidNumberOfShards).build(),
                    TimeValue.THIRTY_SECONDS,
                    TimeValue.THIRTY_SECONDS
                ).indices(dataStreamName)
            ).actionGet();
            List<UpdateDataStreamSettingsAction.DataStreamSettingsResponse> dataStreamSettingsResponses = putSettingsResponse
                .getDataStreamSettingsResponses();
            assertThat(dataStreamSettingsResponses.size(), equalTo(1));
            UpdateDataStreamSettingsAction.DataStreamSettingsResponse dataStreamSettingsResponse = dataStreamSettingsResponses.get(0);
            assertThat(dataStreamSettingsResponse.dataStreamName(), equalTo(dataStreamName));
            assertThat(dataStreamSettingsResponse.dataStreamSucceeded(), equalTo(false));
        }
        {
            // Try to set an invalid setting, and make sure the data stream is not updated
            UpdateDataStreamSettingsAction.Response putSettingsResponse = client().execute(
                UpdateDataStreamSettingsAction.INSTANCE,
                new UpdateDataStreamSettingsAction.Request(
                    Settings.builder()
                        .put("index.number_of_shards", numberOfShards)
                        .put("unknown.setting", randomAlphaOfLength(20))
                        .build(),
                    TimeValue.THIRTY_SECONDS,
                    TimeValue.THIRTY_SECONDS
                ).indices(dataStreamName)
            ).actionGet();
            List<UpdateDataStreamSettingsAction.DataStreamSettingsResponse> dataStreamSettingsResponses = putSettingsResponse
                .getDataStreamSettingsResponses();
            assertThat(dataStreamSettingsResponses.size(), equalTo(1));
            UpdateDataStreamSettingsAction.DataStreamSettingsResponse dataStreamSettingsResponse = dataStreamSettingsResponses.get(0);
            assertThat(dataStreamSettingsResponse.dataStreamName(), equalTo(dataStreamName));
            assertThat(dataStreamSettingsResponse.dataStreamSucceeded(), equalTo(false));
        }
    }

    public void testPutMultipleDataStreamSettings() throws Exception {
        List<String> testDataStreamNames = new ArrayList<>();
        List<String> ignoredDataStreamNames = new ArrayList<>();
        for (int i = 0; i < randomIntBetween(2, 5); i++) {
            String dataStreamName = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
            putComposableIndexTemplate("my-template-" + i, List.of(dataStreamName), indexSettings(1, 0).build());
            final var createDataStreamRequest = new CreateDataStreamAction.Request(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                dataStreamName
            );
            assertAcked(client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).actionGet());
            testDataStreamNames.add(dataStreamName);
        }
        for (int i = 0; i < randomIntBetween(2, 5); i++) {
            String dataStreamName = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
            putComposableIndexTemplate("my-other-template-" + i, List.of(dataStreamName), indexSettings(1, 0).build());
            final var createDataStreamRequest = new CreateDataStreamAction.Request(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                dataStreamName
            );
            assertAcked(client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).actionGet());
            ignoredDataStreamNames.add(dataStreamName);
        }
        final int numberOfShards = randomIntBetween(2, 7);
        final String newLifecycleName = randomAlphanumericOfLength(20).toLowerCase(Locale.ROOT);
        {
            {
                // First, a quick check that we fetch all data streams when no data stream names are given:
                UpdateDataStreamSettingsAction.Response putSettingsResponse = client().execute(
                    UpdateDataStreamSettingsAction.INSTANCE,
                    new UpdateDataStreamSettingsAction.Request(
                        Settings.builder()
                            .put("index.number_of_shards", numberOfShards)
                            .put("index.lifecycle.name", newLifecycleName)
                            .build(),
                        TimeValue.THIRTY_SECONDS,
                        TimeValue.THIRTY_SECONDS
                    )
                ).actionGet();
                List<UpdateDataStreamSettingsAction.DataStreamSettingsResponse> dataStreamSettingsResponses = putSettingsResponse
                    .getDataStreamSettingsResponses();
                assertThat(dataStreamSettingsResponses.size(), equalTo(testDataStreamNames.size() + ignoredDataStreamNames.size()));
            }
            UpdateDataStreamSettingsAction.Response putSettingsResponse = client().execute(
                UpdateDataStreamSettingsAction.INSTANCE,
                new UpdateDataStreamSettingsAction.Request(
                    Settings.builder().put("index.number_of_shards", numberOfShards).put("index.lifecycle.name", newLifecycleName).build(),
                    TimeValue.THIRTY_SECONDS,
                    TimeValue.THIRTY_SECONDS
                ).indices(testDataStreamNames.toArray(new String[0]))
            ).actionGet();
            List<UpdateDataStreamSettingsAction.DataStreamSettingsResponse> dataStreamSettingsResponses = putSettingsResponse
                .getDataStreamSettingsResponses();
            assertThat(dataStreamSettingsResponses.size(), equalTo(testDataStreamNames.size()));
            for (int i = 0; i < testDataStreamNames.size(); i++) {
                UpdateDataStreamSettingsAction.DataStreamSettingsResponse dataStreamSettingsResponse = dataStreamSettingsResponses.get(0);
                assertThat(dataStreamSettingsResponse.dataStreamSucceeded(), equalTo(true));
                assertThat(dataStreamSettingsResponse.settings().get("index.number_of_shards"), equalTo(Integer.toString(numberOfShards)));
                assertThat(
                    dataStreamSettingsResponse.effectiveSettings().get("index.number_of_shards"),
                    equalTo(Integer.toString(numberOfShards))
                );
                assertThat(dataStreamSettingsResponse.indicesSettingsResult().indexSettingErrors().size(), equalTo(0));
                assertThat(dataStreamSettingsResponse.indicesSettingsResult().appliedToDataStreamOnly().size(), equalTo(1));
                assertThat(
                    dataStreamSettingsResponse.indicesSettingsResult().appliedToDataStreamOnly().get(0),
                    equalTo("index.number_of_shards")
                );
                assertThat(dataStreamSettingsResponse.indicesSettingsResult().appliedToDataStreamAndBackingIndices().size(), equalTo(1));
                assertThat(
                    dataStreamSettingsResponse.indicesSettingsResult().appliedToDataStreamAndBackingIndices().get(0),
                    equalTo("index.lifecycle.name")
                );
                GetIndexResponse response = admin().indices()
                    .getIndex(new GetIndexRequest(TimeValue.THIRTY_SECONDS).indices(dataStreamSettingsResponse.dataStreamName()))
                    .actionGet();
                Settings settings = response.getSettings().values().iterator().next();
                assertThat(settings.get("index.number_of_shards"), equalTo("1"));
                assertThat(settings.get("index.lifecycle.name"), equalTo(newLifecycleName));
            }
            List<GetDataStreamSettingsAction.DataStreamSettingsResponse> getSettingsResponses = client().execute(
                GetDataStreamSettingsAction.INSTANCE,
                new GetDataStreamSettingsAction.Request(TimeValue.THIRTY_SECONDS).indices(testDataStreamNames.toArray(new String[0]))
            ).actionGet().getDataStreamSettingsResponses();
            assertThat(getSettingsResponses.size(), equalTo(testDataStreamNames.size()));
        }
    }

    public void testRolloverWithDataStreamSettings() throws Exception {
        String dataStreamName = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        putComposableIndexTemplate("my-template", List.of(dataStreamName), indexSettings(1, 0).build());
        final var createDataStreamRequest = new CreateDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, dataStreamName);
        assertAcked(client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).actionGet());
        final int numberOfShards = randomIntBetween(2, 7);
        final String newLifecycleName = randomAlphanumericOfLength(20).toLowerCase(Locale.ROOT);
        client().execute(
            UpdateDataStreamSettingsAction.INSTANCE,
            new UpdateDataStreamSettingsAction.Request(
                Settings.builder().put("index.number_of_shards", numberOfShards).put("index.lifecycle.name", newLifecycleName).build(),
                TimeValue.THIRTY_SECONDS,
                TimeValue.THIRTY_SECONDS
            ).indices(dataStreamName)
        ).actionGet();

        RolloverResponse rolloverResponse = client().execute(RolloverAction.INSTANCE, new RolloverRequest(dataStreamName, null))
            .actionGet();
        assertThat(rolloverResponse.isRolledOver(), equalTo(true));
        String newIndexName = rolloverResponse.getNewIndex();
        GetIndexResponse response = admin().indices()
            .getIndex(new GetIndexRequest(TimeValue.THIRTY_SECONDS).indices(newIndexName))
            .actionGet();
        Settings settings = response.getSettings().get(newIndexName);
        assertThat(settings.get("index.number_of_shards"), equalTo(Integer.toString(numberOfShards)));
        assertThat(settings.get("index.lifecycle.name"), equalTo(newLifecycleName));
    }

    public void testIndexBlock() throws Exception {
        /*
         * This tests that if there is a block on one index, the settings changes still go through on all the other
         * indices.
         */
        String dataStreamName = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        putComposableIndexTemplate("my-template", List.of(dataStreamName), indexSettings(1, 0).build());
        final var createDataStreamRequest = new CreateDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, dataStreamName);
        assertAcked(client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).actionGet());
        Set<String> indexNames = new HashSet<>();
        for (int i = 0; i < randomIntBetween(1, 10); i++) {
            RolloverResponse rolloverResponse = client().execute(RolloverAction.INSTANCE, new RolloverRequest(dataStreamName, null))
                .actionGet();
            assertThat(rolloverResponse.isRolledOver(), equalTo(true));
            indexNames.add(rolloverResponse.getOldIndex());
        }
        String indexToBlock = randomFrom(indexNames);
        PlainActionFuture<AddIndexBlockResponse> plainActionFuture = new PlainActionFuture<>();
        indicesAdmin().addBlock(new AddIndexBlockRequest(IndexMetadata.APIBlock.METADATA, indexToBlock), plainActionFuture);
        assertThat(plainActionFuture.get().isShardsAcknowledged(), equalTo(true));
        final int numberOfShards = randomIntBetween(2, 7);
        final String newLifecycleName = randomAlphanumericOfLength(20).toLowerCase(Locale.ROOT);
        {
            UpdateDataStreamSettingsAction.Response putSettingsResponse = client().execute(
                UpdateDataStreamSettingsAction.INSTANCE,
                new UpdateDataStreamSettingsAction.Request(
                    Settings.builder().put("index.number_of_shards", numberOfShards).put("index.lifecycle.name", newLifecycleName).build(),
                    TimeValue.THIRTY_SECONDS,
                    TimeValue.THIRTY_SECONDS
                ).indices(dataStreamName)
            ).actionGet();
            List<UpdateDataStreamSettingsAction.DataStreamSettingsResponse> dataStreamSettingsResponses = putSettingsResponse
                .getDataStreamSettingsResponses();
            UpdateDataStreamSettingsAction.DataStreamSettingsResponse dataStreamSettingsResponse = dataStreamSettingsResponses.get(0);
            assertThat(dataStreamSettingsResponse.dataStreamSucceeded(), equalTo(true));
            indicesAdmin().prepareUpdateSettings(indexToBlock)
                .setSettings(Settings.builder().put("index.blocks.metadata", false).build())
                .get();
            GetIndexResponse response = admin().indices()
                .getIndex(new GetIndexRequest(TimeValue.THIRTY_SECONDS).indices(dataStreamSettingsResponse.dataStreamName()))
                .actionGet();
            for (Map.Entry<String, Settings> indexAndsettings : response.getSettings().entrySet()) {
                if (indexAndsettings.getKey().equals(indexToBlock)) {
                    assertThat(indexAndsettings.getValue().get("index.lifecycle.name"), equalTo(null));
                } else {
                    assertThat(indexAndsettings.getValue().get("index.lifecycle.name"), equalTo(newLifecycleName));
                }
            }
        }
    }

    static void putComposableIndexTemplate(String id, List<String> patterns, @Nullable Settings settings) throws IOException {
        TransportPutComposableIndexTemplateAction.Request request = new TransportPutComposableIndexTemplateAction.Request(id);
        request.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(patterns)
                .template(Template.builder().settings(settings))
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                .build()
        );
        client().execute(TransportPutComposableIndexTemplateAction.TYPE, request).actionGet();
    }

    public static class TestPlugin extends Plugin {
        /*
         * index.lifecycle.name is one of the settings allowed by TransportUpdateDataStreamSettingsAction, but it is in the ilm plugin. We
         * add it here so that it is available for testing.
         */
        public static final Setting<String> LIFECYCLE_SETTING = Setting.simpleString(
            "index.lifecycle.name",
            "",
            Setting.Property.IndexScope
        );

        @Override
        public List<Setting<?>> getSettings() {
            return List.of(LIFECYCLE_SETTING);
        }
    }

}
