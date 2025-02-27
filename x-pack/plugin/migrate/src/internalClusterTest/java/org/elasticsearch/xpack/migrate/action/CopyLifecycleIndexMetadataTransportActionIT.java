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
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.ingest.common.IngestCommonPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.StartILMRequest;
import org.elasticsearch.xpack.core.ilm.StopILMRequest;
import org.elasticsearch.xpack.core.ilm.action.GetStatusAction;
import org.elasticsearch.xpack.core.ilm.action.ILMActions;
import org.elasticsearch.xpack.core.ilm.action.PutLifecycleRequest;
import org.elasticsearch.xpack.ilm.IndexLifecycle;
import org.elasticsearch.xpack.migrate.MigratePlugin;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class CopyLifecycleIndexMetadataTransportActionIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(
            LocalStateCompositeXPackPlugin.class,
            MigratePlugin.class,
            DataStreamsPlugin.class,
            IngestCommonPlugin.class,
            IndexLifecycle.class
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(LifecycleSettings.LIFECYCLE_POLL_INTERVAL, "1s")
            // This just generates less churn and makes it easier to read the log file if needed
            .put(LifecycleSettings.LIFECYCLE_HISTORY_INDEX_ENABLED, false)
            .build();
    }

    public void testCreationDate() {
        var sourceIndex = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        safeGet(indicesAdmin().create(new CreateIndexRequest(sourceIndex)));

        // so creation date is different
        safeSleep(2);

        var destIndex = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        safeGet(indicesAdmin().create(new CreateIndexRequest(destIndex)));

        // verify source and dest date are actually different before copying
        var settingsResponse = indicesAdmin().getSettings(new GetSettingsRequest(TEST_REQUEST_TIMEOUT).indices(sourceIndex, destIndex))
            .actionGet();
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

        var destDate = indicesAdmin().getSettings(new GetSettingsRequest(TEST_REQUEST_TIMEOUT).indices(sourceIndex, destIndex))
            .actionGet()
            .getIndexToSettings()
            .get(destIndex)
            .getAsLong(IndexMetadata.SETTING_CREATION_DATE, 0L);
        assertEquals(sourceDate, destDate);
    }

    public void testILMState() throws Exception {

        Map<String, Phase> phases = Map.of(
            "hot",
            new Phase(
                "hot",
                TimeValue.ZERO,
                Map.of(
                    "rollover",
                    new org.elasticsearch.xpack.core.ilm.RolloverAction(null, null, null, 1L, null, null, null, null, null, null)
                )
            )
        );

        var policyName = "my-policy";
        LifecyclePolicy policy = new LifecyclePolicy(policyName, phases);
        PutLifecycleRequest putLifecycleRequest = new PutLifecycleRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, policy);
        assertAcked(client().execute(ILMActions.PUT, putLifecycleRequest).actionGet());

        // create data stream with a document and wait for ILM to roll it over
        var dataStream = createDataStream(policyName);
        createDocument(dataStream);
        assertAcked(safeGet(client().execute(ILMActions.START, new StartILMRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT))));
        assertBusy(() -> {
            var getIndexResponse = safeGet(indicesAdmin().getIndex(new GetIndexRequest(TEST_REQUEST_TIMEOUT).indices(dataStream)));
            assertTrue(getIndexResponse.indices().length > 1);
        });
        // stop ILM so source does not change after copying metadata
        assertAcked(safeGet(client().execute(ILMActions.STOP, new StopILMRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT))));
        assertBusy(() -> {
            var statusResponse = safeGet(
                client().execute(GetStatusAction.INSTANCE, new AcknowledgedRequest.Plain(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT))
            );
            assertEquals(OperationMode.STOPPED, statusResponse.getMode());
        });

        var getIndexResponse = safeGet(indicesAdmin().getIndex(new GetIndexRequest(TEST_REQUEST_TIMEOUT).indices(dataStream)));
        for (var backingIndex : getIndexResponse.indices()) {
            var destIndex = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
            safeGet(indicesAdmin().create(new CreateIndexRequest(destIndex)));

            IndexMetadata destBefore = getClusterMetadata(destIndex).getProject().index(destIndex);
            assertNull(destBefore.getCustomData(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY));

            // copy over the metadata
            copyMetadata(backingIndex, destIndex);

            var metadataAfter = getClusterMetadata(backingIndex, destIndex);
            IndexMetadata sourceAfter = metadataAfter.getProject().index(backingIndex);
            IndexMetadata destAfter = metadataAfter.getProject().index(destIndex);
            assertNotNull(destAfter.getCustomData(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY));
            assertEquals(
                sourceAfter.getCustomData(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY),
                destAfter.getCustomData(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY)
            );

        }
    }

    public void testRolloverInfos() throws Exception {
        var dataStream = createDataStream(null);

        // rollover a few times
        createDocument(dataStream);
        rollover(dataStream);
        createDocument(dataStream);
        rollover(dataStream);
        createDocument(dataStream);
        var writeIndex = rollover(dataStream);

        var getIndexResponse = safeGet(indicesAdmin().getIndex(new GetIndexRequest(TEST_REQUEST_TIMEOUT).indices(dataStream)));
        for (var backingIndex : getIndexResponse.indices()) {

            var destIndex = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
            safeGet(indicesAdmin().create(new CreateIndexRequest(destIndex)));

            var metadataBefore = getClusterMetadata(backingIndex, destIndex);
            IndexMetadata source = metadataBefore.getProject().index(backingIndex);
            IndexMetadata destBefore = metadataBefore.getProject().index(destIndex);

            // sanity check not equal before the copy
            if (backingIndex.equals(writeIndex)) {
                assertTrue(source.getRolloverInfos().isEmpty());
                assertTrue(destBefore.getRolloverInfos().isEmpty());
            } else {
                assertNotEquals(source.getRolloverInfos(), destBefore.getRolloverInfos());
            }

            // copy over the metadata
            copyMetadata(backingIndex, destIndex);

            // now rollover info should be equal
            IndexMetadata destAfter = getClusterMetadata(destIndex).getProject().index(destIndex);
            assertEquals(source.getRolloverInfos(), destAfter.getRolloverInfos());
        }
    }

    private String createDataStream(String ilmPolicy) throws Exception {
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.getDefault());

        Settings settings = ilmPolicy != null ? Settings.builder().put(IndexMetadata.LIFECYCLE_NAME, ilmPolicy).build() : null;

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
        safeGet(
            client().index(
                new IndexRequest(dataStreamName).opType(DocWriteRequest.OpType.CREATE)
                    .source(
                        JsonXContent.contentBuilder()
                            .startObject()
                            .field("@timestamp", timestamp)
                            .field("data", randomAlphaOfLength(25))
                            .endObject()
                    )
            )
        );
        safeGet(
            indicesAdmin().refresh(
                new RefreshRequest(".ds-" + dataStreamName + "*").indicesOptions(IndicesOptions.lenientExpandOpenHidden())
            )
        );
        return timestamp;
    }

    private void copyMetadata(String sourceIndex, String destIndex) {
        assertAcked(
            client().execute(
                CopyLifecycleIndexMetadataAction.INSTANCE,
                new CopyLifecycleIndexMetadataAction.Request(TEST_REQUEST_TIMEOUT, sourceIndex, destIndex)
            )
        );
    }

    private String rollover(String dataStream) {
        var rolloverResponse = safeGet(indicesAdmin().rolloverIndex(new RolloverRequest(dataStream, null)));
        assertTrue(rolloverResponse.isAcknowledged());
        return rolloverResponse.getNewIndex();
    }

    private Metadata getClusterMetadata(String... indices) {
        return safeGet(clusterAdmin().state(new ClusterStateRequest(TEST_REQUEST_TIMEOUT).indices(indices))).getState().metadata();
    }
}
