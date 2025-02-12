/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.migrate.action;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.datastreams.GetDataStreamAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xcontent.json.JsonXContent;
//import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.StartILMRequest;
import org.elasticsearch.xpack.core.ilm.action.ILMActions;
import org.elasticsearch.xpack.core.ilm.action.PutLifecycleRequest;
//import org.elasticsearch.xpack.ilm.IndexLifecycle;
import org.elasticsearch.xpack.migrate.MigratePlugin;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class CopyIndexMetadataTransportActionIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(
//            LocalStateCompositeXPackPlugin.class,
            MigratePlugin.class,
            ReindexPlugin.class,
            MockTransportService.TestPlugin.class,
            DataStreamsPlugin.class
        );
    }

    public void testCreationDate() throws Exception {
        var sourceIndex = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        indicesAdmin().create(new CreateIndexRequest(sourceIndex)).get();

        // so creation date is different
        safeSleep(2);

        var destIndex = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        indicesAdmin().create(new CreateIndexRequest(destIndex)).get();

        // verify source and dest date are actually different before copying
        var settingsResponse = indicesAdmin().getSettings(new GetSettingsRequest().indices(sourceIndex, destIndex)).actionGet();
        var indexToSettings = settingsResponse.getIndexToSettings();
        var sourceDate = indexToSettings.get(sourceIndex).getAsLong(IndexMetadata.SETTING_CREATION_DATE, 0L);
        {
            var destDate = indexToSettings.get(destIndex).getAsLong(IndexMetadata.SETTING_CREATION_DATE, 0L);
            assertTrue(sourceDate > 0);
            assertTrue(destDate > 0);
            assertNotEquals(sourceDate, destDate);
        }

        // copy over the metadata
        copyMetadata(sourceIndex, destIndex);

        var destDate = indicesAdmin().getSettings(new GetSettingsRequest().indices(sourceIndex, destIndex))
            .actionGet()
            .getIndexToSettings()
            .get(destIndex)
            .getAsLong(IndexMetadata.SETTING_CREATION_DATE, 0L);
        assertEquals(sourceDate, destDate);
    }

    public void testILMState() throws Exception {

        Map<String, Phase> phases = Map.of(
            "warm",
            new Phase(
                "warm",
                TimeValue.ZERO,
                Map.of("rollover", new org.elasticsearch.xpack.core.ilm.RolloverAction(null, null, null, 1L, null, null, null, null, null, null))
            )
        );

        var policyName = "my-policy";
        LifecyclePolicy policy = new LifecyclePolicy(policyName, phases);
        PutLifecycleRequest putLifecycleRequest = new PutLifecycleRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, policy);
        assertAcked(client().execute(ILMActions.PUT, putLifecycleRequest).actionGet());

        var dataStream = createDataStream(policyName);

        assertAcked(safeGet(client().execute(ILMActions.START, new StartILMRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT))));

        // rollover a few times
        createDocument(dataStream);
        createDocument(dataStream);
        createDocument(dataStream);

        var writeIndex = safeGet(
            indicesAdmin().execute(
                GetDataStreamAction.INSTANCE,
                new GetDataStreamAction.Request(TEST_REQUEST_TIMEOUT, new String[] { dataStream })
            )
        ).getDataStreams().get(0).getDataStream().getWriteIndex().getName();

        var getIndexResponse = indicesAdmin().getIndex(new GetIndexRequest(TEST_REQUEST_TIMEOUT).indices(dataStream)).get();
        for (var backingIndex : getIndexResponse.indices()) {

            var destIndex = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
            indicesAdmin().create(new CreateIndexRequest(destIndex)).get();

            var metadataBefore = safeGet(
                clusterAdmin().state(new ClusterStateRequest(TEST_REQUEST_TIMEOUT).indices(backingIndex, destIndex))
            ).getState().metadata();
            IndexMetadata source = metadataBefore.index(backingIndex);
            IndexMetadata destBefore = metadataBefore.index(destIndex);

            assertNotEquals(
                source.getCustomData(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY),
                destBefore.getCustomData(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY)
            );

            // sanity check not equal before the copy
            if (backingIndex.equals(writeIndex)) {
                assertTrue(source.getRolloverInfos().isEmpty());
                assertTrue(destBefore.getRolloverInfos().isEmpty());
            } else {
                assertNotEquals(source.getRolloverInfos(), destBefore.getRolloverInfos());
            }

            // copy over the metadata
            copyMetadata(backingIndex, destIndex);

            var metadataAfter = safeGet(clusterAdmin().state(new ClusterStateRequest(TEST_REQUEST_TIMEOUT).indices(destIndex)))
                .getState()
                .metadata();
            IndexMetadata destAfter = metadataAfter.index(destIndex);

            // now rollover info should be equal
            assertEquals(source.getRolloverInfos(), destAfter.getRolloverInfos());

            Map<String, String> customData = source.getCustomData(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY);
            //
            assertEquals(
                source.getCustomData(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY),
                destAfter.getCustomData(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY)
            );

        }
    }




    public void testRolloverInfos() throws Exception {

        var dataStream = createDataStream(null);
        Map<String, Phase> phases = Map.of(
            "warm",
            new Phase(
                "warm",
                TimeValue.ZERO,
                Map.of("rollover", new org.elasticsearch.xpack.core.ilm.RolloverAction(null, null, null, 1L, null, null, null, null, null, null))
            )
        );

        var policyName = "my-policy";
        LifecyclePolicy policy = new LifecyclePolicy(policyName, phases);
        PutLifecycleRequest putLifecycleRequest = new PutLifecycleRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, policy);
        assertAcked(client().execute(ILMActions.PUT, putLifecycleRequest).actionGet());



        // rollover a few times
        createDocument(dataStream);
        assertTrue(indicesAdmin().rolloverIndex(new RolloverRequest(dataStream, null)).get().isAcknowledged());
        createDocument(dataStream);
        assertTrue(indicesAdmin().rolloverIndex(new RolloverRequest(dataStream, null)).get().isAcknowledged());
        createDocument(dataStream);
        assertTrue(indicesAdmin().rolloverIndex(new RolloverRequest(dataStream, null)).get().isAcknowledged());

        var writeIndex = safeGet(
            indicesAdmin().execute(
                GetDataStreamAction.INSTANCE,
                new GetDataStreamAction.Request(TEST_REQUEST_TIMEOUT, new String[] { dataStream })
            )
        ).getDataStreams().get(0).getDataStream().getWriteIndex().getName();

        var getIndexResponse = indicesAdmin().getIndex(new GetIndexRequest(TEST_REQUEST_TIMEOUT).indices(dataStream)).get();
        for (var backingIndex : getIndexResponse.indices()) {

            var destIndex = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
            indicesAdmin().create(new CreateIndexRequest(destIndex)).get();

            var metadataBefore = safeGet(
                clusterAdmin().state(new ClusterStateRequest(TEST_REQUEST_TIMEOUT).indices(backingIndex, destIndex))
            ).getState().metadata();
            IndexMetadata source = metadataBefore.index(backingIndex);
            IndexMetadata destBefore = metadataBefore.index(destIndex);

//            assertNotEquals(
//                source.getCustomData(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY),
//                destBefore.getCustomData(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY)
//            );

            // sanity check not equal before the copy
            if (backingIndex.equals(writeIndex)) {
                assertTrue(source.getRolloverInfos().isEmpty());
                assertTrue(destBefore.getRolloverInfos().isEmpty());
            } else {
                assertNotEquals(source.getRolloverInfos(), destBefore.getRolloverInfos());
            }

            // copy over the metadata
            copyMetadata(backingIndex, destIndex);

            var metadataAfter = safeGet(clusterAdmin().state(new ClusterStateRequest(TEST_REQUEST_TIMEOUT).indices(destIndex)))
                .getState()
                .metadata();
            IndexMetadata destAfter = metadataAfter.index(destIndex);

            // now rollover info should be equal
            assertEquals(source.getRolloverInfos(), destAfter.getRolloverInfos());

            Map<String, String> customData = source.getCustomData(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY);
            //
            assertEquals(
                source.getCustomData(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY),
                destAfter.getCustomData(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY)
            );

        }
    }

    private String createDataStream(String ilmPolicy) throws Exception {
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.getDefault());

        Settings settings = ilmPolicy != null ?
            Settings.builder().put(IndexMetadata.LIFECYCLE_NAME, ilmPolicy).build()
            : null;

        String mapping = """
                {
                    "properties": {
                        "@timestamp": {
                            "type":"date"
                        },
                        "data":{
                            "type":"keyword"
                        }
                    }
                }
            """;
        Template idxTemplate = new Template(settings, new CompressedXContent(mapping), null);

        ComposableIndexTemplate template = ComposableIndexTemplate.builder()
            .indexPatterns(List.of(dataStreamName + "*"))
            .template(idxTemplate)
            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, false))
            .build();

        assertAcked(
            client().execute(
                TransportPutComposableIndexTemplateAction.TYPE,
                new TransportPutComposableIndexTemplateAction.Request(dataStreamName + "_template").indexTemplate(template)
            )
        );
        assertAcked(
            client().execute(
                CreateDataStreamAction.INSTANCE,
                new CreateDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, dataStreamName)
            )
        );
        return dataStreamName;
    }

    private long createDocument(String dataStreamName) throws Exception {
        // Get some randomized but reasonable timestamps on the data since not all of it is guaranteed to arrive in order.
        long timeSeed = System.currentTimeMillis();
        long timestamp = randomLongBetween(timeSeed - TimeUnit.HOURS.toMillis(5), timeSeed);
        client().index(
            new IndexRequest(dataStreamName).opType(DocWriteRequest.OpType.CREATE)
                .source(
                    JsonXContent.contentBuilder()
                        .startObject()
                        .field("@timestamp", timestamp)
                        .field("data", randomAlphaOfLength(25))
                        .endObject()
                )
        ).get();
        indicesAdmin().refresh(new RefreshRequest(".ds-" + dataStreamName + "*").indicesOptions(IndicesOptions.lenientExpandOpenHidden()))
            .get();
        return timestamp;
    }

    private void copyMetadata(String sourceIndex, String destIndex) {
        assertAcked(
            client().execute(
                CopyIndexMetadataAction.INSTANCE,
                new CopyIndexMetadataAction.Request(TEST_REQUEST_TIMEOUT, sourceIndex, destIndex)
            )
        );
    }
}
