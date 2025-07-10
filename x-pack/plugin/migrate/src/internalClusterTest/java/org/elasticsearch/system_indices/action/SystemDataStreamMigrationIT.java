/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.system_indices.action;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.indices.ExecutorNames;
import org.elasticsearch.indices.SystemDataStreamDescriptor;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;

public class SystemDataStreamMigrationIT extends AbstractFeatureMigrationIntegTest {
    private static final String TEST_DATA_STREAM_NAME = ".test-data-stream";
    private static final String DATA_STREAM_FEATURE = "ds-feature";
    private static volatile SystemDataStreamDescriptor systemDataStreamDescriptor = createSystemDataStreamDescriptor(
        NEEDS_UPGRADE_INDEX_VERSION
    );

    private static SystemDataStreamDescriptor createSystemDataStreamDescriptor(IndexVersion indexVersion) {
        try {
            return new SystemDataStreamDescriptor(
                TEST_DATA_STREAM_NAME,
                "system data stream test",
                SystemDataStreamDescriptor.Type.EXTERNAL,
                ComposableIndexTemplate.builder()
                    .template(
                        Template.builder()
                            .mappings(new CompressedXContent("""
                                {
                                    "properties": {
                                      "@timestamp" : {
                                        "type": "date"
                                      },
                                      "count": {
                                        "type": "long"
                                      }
                                    }
                                }"""))
                            .dataStreamOptions(DataStreamTestHelper.createDataStreamOptionsTemplate(true))
                            .settings(indexSettings(indexVersion, 1, 0))
                    )
                    .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                    .build(),
                Map.of(),
                List.of("product"),
                ORIGIN,
                ExecutorNames.DEFAULT_SYSTEM_DATA_STREAM_THREAD_POOLS
            );
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings)).build();
    }

    @Override
    protected boolean forbidPrivateIndexSettings() {
        // We need to be able to set the index creation version manually.
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(DataStreamsPlugin.class);
        plugins.add(DataStreamTestPlugin.class);
        plugins.add(MapperExtrasPlugin.class);
        return plugins;
    }

    @After
    public void restoreDescriptor() {
        // we need to do it in after, because we need to have systemDataStreamDescriptor in a correct state
        // before next super.setup() is called
        systemDataStreamDescriptor = createSystemDataStreamDescriptor(NEEDS_UPGRADE_INDEX_VERSION);
    }

    private static void indexDocsToDataStream(String dataStreamName) {
        BulkRequestBuilder bulkBuilder = client().prepareBulk();
        for (int i = 0; i < INDEX_DOC_COUNT; i++) {
            IndexRequestBuilder requestBuilder = ESIntegTestCase.prepareIndex(dataStreamName)
                .setId(Integer.toString(i))
                .setRequireDataStream(true)
                .setOpType(DocWriteRequest.OpType.CREATE)
                .setSource(DataStream.TIMESTAMP_FIELD_NAME, 1741271969000L, FIELD_NAME, "words words");
            bulkBuilder.add(requestBuilder);
        }

        BulkResponse actionGet = bulkBuilder.get();
        assertThat(actionGet.hasFailures() ? actionGet.buildFailureMessage() : "", actionGet.hasFailures(), equalTo(false));

        // Index docs to failure store too
        bulkBuilder = client().prepareBulk();
        for (int i = 0; i < INDEX_DOC_COUNT; i++) {
            IndexRequestBuilder requestBuilder = ESIntegTestCase.prepareIndex(dataStreamName)
                .setId(Integer.toString(i))
                .setRequireDataStream(true)
                .setOpType(DocWriteRequest.OpType.CREATE)
                .setSource(DataStream.TIMESTAMP_FIELD_NAME, 1741271969000L, "count", "not-a-number");
            bulkBuilder.add(requestBuilder);
        }

        actionGet = bulkBuilder.get();
        assertThat(actionGet.hasFailures() ? actionGet.buildFailureMessage() : "", actionGet.hasFailures(), equalTo(false));
    }

    public void testMigrateSystemDataStream() throws Exception {
        createDataStream();

        indexDocsToDataStream(TEST_DATA_STREAM_NAME);

        simulateClusterUpgrade();

        executeMigration(DATA_STREAM_FEATURE);

        // Waiting for shards to stabilize if indices were moved around
        ensureGreen();

        ProjectMetadata finalMetadata = assertMetadataAfterMigration(DATA_STREAM_FEATURE);

        DataStream dataStream = finalMetadata.dataStreams().get(TEST_DATA_STREAM_NAME);
        assertNotNull(dataStream);
        assertThat(dataStream.isSystem(), is(true));
        List<Index> backingIndices = dataStream.getIndices();
        assertThat(backingIndices, hasSize(2));
        for (Index backingIndex : backingIndices) {
            IndexMetadata indexMetadata = finalMetadata.index(backingIndex);
            assertThat(indexMetadata.isSystem(), is(true));
            assertThat(indexMetadata.getCreationVersion(), is(IndexVersion.current()));
        }

        // Migrate action does not migrate the failure store indices
        // here we check that they are preserved.
        List<Index> failureIndices = dataStream.getFailureIndices();
        assertThat(failureIndices, hasSize(1));
        for (Index failureIndex : failureIndices) {
            IndexMetadata indexMetadata = finalMetadata.index(failureIndex);
            assertThat(indexMetadata.isSystem(), is(true));
            assertThat(indexMetadata.getCreationVersion(), is(IndexVersion.current()));
        }
    }

    public void testMigrationRestartAfterFailure() throws Exception {
        createDataStream();

        indexDocsToDataStream(TEST_DATA_STREAM_NAME);

        simulateClusterUpgrade();

        TestPlugin.BlockingActionFilter blockingActionFilter = blockAction(TransportCreateIndexAction.TYPE.name());

        startMigration(DATA_STREAM_FEATURE);

        GetFeatureUpgradeStatusRequest getStatusRequest = new GetFeatureUpgradeStatusRequest(TEST_REQUEST_TIMEOUT);
        assertBusy(() -> {
            GetFeatureUpgradeStatusResponse statusResponse = client().execute(GetFeatureUpgradeStatusAction.INSTANCE, getStatusRequest)
                .get();
            logger.info(Strings.toString(statusResponse));
            assertThat(statusResponse.getUpgradeStatus(), equalTo(GetFeatureUpgradeStatusResponse.UpgradeStatus.ERROR));
        }, 30, TimeUnit.SECONDS);

        blockingActionFilter.unblockAllActions();
        ensureGreen();

        executeMigration(DATA_STREAM_FEATURE);
        ensureGreen();

        assertMetadataAfterMigration(DATA_STREAM_FEATURE);
    }

    private void simulateClusterUpgrade() throws Exception {
        String indexVersionCreated = systemDataStreamDescriptor.getComposableIndexTemplate()
            .template()
            .settings()
            .get(IndexMetadata.SETTING_VERSION_CREATED);
        assertThat(indexVersionCreated, is(NEEDS_UPGRADE_INDEX_VERSION.toString()));
        // we can't have NEEDS_UPGRADE_VERSION in settings anymore,
        // because those settings will be used in index rollover during data stream migration
        // instead we update settings here, kinda simulating upgrade to a new version and restart the cluster
        systemDataStreamDescriptor = createSystemDataStreamDescriptor(IndexVersion.current());

        internalCluster().fullRestart();
        ensureGreen();
    }

    private void createDataStream() throws InterruptedException, ExecutionException {
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            TEST_DATA_STREAM_NAME
        );
        AcknowledgedResponse createDSResponse = client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();
        assertTrue(createDSResponse.isAcknowledged());

        ensureGreen();
    }

    public static class DataStreamTestPlugin extends Plugin implements SystemIndexPlugin, ActionPlugin {
        @Override
        public String getFeatureName() {
            return DATA_STREAM_FEATURE;
        }

        @Override
        public String getFeatureDescription() {
            return "Feature to test system data streams migration";
        }

        @Override
        public Collection<SystemDataStreamDescriptor> getSystemDataStreamDescriptors() {
            return List.of(systemDataStreamDescriptor);
        }
    }
}
