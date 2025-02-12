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
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.ingest.common.IngestCommonPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.migrate.MigratePlugin;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class CopyIndexMetadataTransportActionIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(
            MigratePlugin.class,
            ReindexPlugin.class,
            MockTransportService.TestPlugin.class,
            DataStreamsPlugin.class,
            IngestCommonPlugin.class
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

    public void testRolloverInfos() throws Exception {

        var dataStream = createDataStream();

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
        }
    }

    private String createDataStream() throws Exception {
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.getDefault());

        Template idxTemplate = new Template(null, new CompressedXContent("""
            {"properties":{"@timestamp":{"type":"date"},"data":{"type":"keyword"}}}
            """), null);

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
