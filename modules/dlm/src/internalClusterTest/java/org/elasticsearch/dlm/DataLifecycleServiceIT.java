/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.dlm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.datastreams.GetDataStreamAction;
import org.elasticsearch.action.datastreams.ModifyDataStreamsAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataLifecycle;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamAction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.dlm.action.PutDataLifecycleAction;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.test.transport.StubbableTransport;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentType;
import org.junit.After;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.backingIndexEqualTo;
import static org.elasticsearch.cluster.metadata.IndexMetadata.APIBlock.READ_ONLY;
import static org.elasticsearch.cluster.metadata.MetadataIndexTemplateService.DEFAULT_TIMESTAMP_FIELD;
import static org.elasticsearch.index.IndexSettings.LIFECYCLE_ORIGINATION_DATE;
import static org.elasticsearch.indices.ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class DataLifecycleServiceIT extends ESIntegTestCase {
    private static final Logger logger = LogManager.getLogger(DataLifecycleServiceIT.class);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(DataLifecyclePlugin.class, DataStreamsPlugin.class, MockTransportService.TestPlugin.class);
    }

    protected boolean ignoreExternalCluster() {
        return true;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder settings = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        settings.put(DataLifecycleService.DLM_POLL_INTERVAL, "1s");
        settings.put(DataLifecycle.CLUSTER_DLM_DEFAULT_ROLLOVER_SETTING.getKey(), "min_docs=1,max_docs=1");
        return settings.build();
    }

    @After
    public void cleanup() {
        // we change SETTING_CLUSTER_MAX_SHARDS_PER_NODE in a test so let's make sure we clean it up even when the test fails
        updateClusterSettings(Settings.builder().putNull("*"));
    }

    public void testRolloverLifecycle() throws Exception {
        // empty lifecycle contains the default rollover
        DataLifecycle lifecycle = new DataLifecycle();

        putComposableIndexTemplate("id1", null, List.of("metrics-foo*"), null, null, lifecycle);
        String dataStreamName = "metrics-foo";
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(dataStreamName);
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();

        indexDocs(dataStreamName, 1);

        assertBusy(() -> {
            GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(new String[] { dataStreamName });
            GetDataStreamAction.Response getDataStreamResponse = client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest)
                .actionGet();
            assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(1));
            assertThat(getDataStreamResponse.getDataStreams().get(0).getDataStream().getName(), equalTo(dataStreamName));
            List<Index> backingIndices = getDataStreamResponse.getDataStreams().get(0).getDataStream().getIndices();
            assertThat(backingIndices.size(), equalTo(2));
            String backingIndex = backingIndices.get(0).getName();
            assertThat(backingIndex, backingIndexEqualTo(dataStreamName, 1));
            String writeIndex = backingIndices.get(1).getName();
            assertThat(writeIndex, backingIndexEqualTo(dataStreamName, 2));
        });
    }

    public void testRolloverAndRetention() throws Exception {
        DataLifecycle lifecycle = new DataLifecycle(TimeValue.timeValueMillis(0));

        putComposableIndexTemplate("id1", null, List.of("metrics-foo*"), null, null, lifecycle);

        String dataStreamName = "metrics-foo";
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(dataStreamName);
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();

        indexDocs(dataStreamName, 1);

        assertBusy(() -> {
            GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(new String[] { dataStreamName });
            GetDataStreamAction.Response getDataStreamResponse = client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest)
                .actionGet();
            assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(1));
            assertThat(getDataStreamResponse.getDataStreams().get(0).getDataStream().getName(), equalTo(dataStreamName));
            List<Index> backingIndices = getDataStreamResponse.getDataStreams().get(0).getDataStream().getIndices();
            assertThat(backingIndices.size(), equalTo(1));
            // we expect the data stream to have only one backing index, the write one, with generation 2
            // as generation 1 would've been deleted by DLM given the lifecycle configuration
            String writeIndex = backingIndices.get(0).getName();
            assertThat(writeIndex, backingIndexEqualTo(dataStreamName, 2));
        });
    }

    public void testOriginationDate() throws Exception {
        /*
         * In this test, we set up a datastream with 7 day retention. Then we add two indices to it -- one with an origination date 365
         * days ago, and one with an origination date 1 day ago. After DLM runs, we expect the one with the old origination date to have
         * been deleted, and the one with the newer origination date to remain.
         */
        DataLifecycle lifecycle = new DataLifecycle(TimeValue.timeValueDays(7).millis());

        putComposableIndexTemplate("id1", null, List.of("metrics-foo*"), null, null, lifecycle);

        String dataStreamName = "metrics-foo";
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(dataStreamName);
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();

        indexDocs(dataStreamName, 1);

        String mapping = """
             {
                "properties":{
                    "@timestamp": {
                        "type": "date"
                    }
                }
            }""";
        PutComposableIndexTemplateAction.Request request = new PutComposableIndexTemplateAction.Request("id2");
        request.indexTemplate(
            new ComposableIndexTemplate(
                List.of("index_*"),
                new Template(null, CompressedXContent.fromJSON(mapping), null, null),
                null,
                null,
                null,
                null,
                null,
                null
            )
        );
        client().execute(PutComposableIndexTemplateAction.INSTANCE, request).actionGet();

        String indexWithOldOriginationDate = "index_old";
        long originTimeMillis = System.currentTimeMillis() - TimeValue.timeValueDays(365).millis();
        createIndex(indexWithOldOriginationDate, Settings.builder().put(LIFECYCLE_ORIGINATION_DATE, originTimeMillis).build());
        client().execute(
            ModifyDataStreamsAction.INSTANCE,
            new ModifyDataStreamsAction.Request(List.of(DataStreamAction.addBackingIndex(dataStreamName, indexWithOldOriginationDate)))
        ).get();

        String indexWithNewOriginationDate = "index_new";
        originTimeMillis = System.currentTimeMillis() - TimeValue.timeValueDays(1).millis();
        createIndex(indexWithNewOriginationDate, Settings.builder().put(LIFECYCLE_ORIGINATION_DATE, originTimeMillis).build());
        client().execute(
            ModifyDataStreamsAction.INSTANCE,
            new ModifyDataStreamsAction.Request(List.of(DataStreamAction.addBackingIndex(dataStreamName, indexWithNewOriginationDate)))
        ).get();

        assertBusy(() -> {
            GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(new String[] { dataStreamName });
            GetDataStreamAction.Response getDataStreamResponse = client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest)
                .actionGet();
            assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(1));
            assertThat(getDataStreamResponse.getDataStreams().get(0).getDataStream().getName(), equalTo(dataStreamName));
            List<Index> backingIndices = getDataStreamResponse.getDataStreams().get(0).getDataStream().getIndices();
            Set<String> indexNames = backingIndices.stream().map(index -> index.getName()).collect(Collectors.toSet());
            assertTrue(indexNames.contains("index_new"));
            assertFalse(indexNames.contains("index_old"));
        });
    }

    public void testUpdatingLifecycleAppliesToAllBackingIndices() throws Exception {
        DataLifecycle lifecycle = new DataLifecycle();

        putComposableIndexTemplate("id1", null, List.of("metrics-foo*"), null, null, lifecycle);

        String dataStreamName = "metrics-foo";
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(dataStreamName);
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();

        int finalGeneration = randomIntBetween(2, 20);
        for (int currentGeneration = 1; currentGeneration < finalGeneration; currentGeneration++) {
            indexDocs(dataStreamName, 1);
            int currentBackingIndexCount = currentGeneration;
            assertBusy(() -> {
                GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(new String[] { dataStreamName });
                GetDataStreamAction.Response getDataStreamResponse = client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest)
                    .actionGet();
                assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(1));
                DataStream dataStream = getDataStreamResponse.getDataStreams().get(0).getDataStream();
                assertThat(dataStream.getName(), equalTo(dataStreamName));
                List<Index> backingIndices = dataStream.getIndices();
                assertThat(backingIndices.size(), equalTo(currentBackingIndexCount + 1));
                String writeIndex = dataStream.getWriteIndex().getName();
                assertThat(writeIndex, backingIndexEqualTo(dataStreamName, currentBackingIndexCount + 1));
            });
        }
        // Update the lifecycle of the data stream
        updateLifecycle(dataStreamName, TimeValue.timeValueMillis(1));
        // Verify that the retention has changed for all backing indices
        assertBusy(() -> {
            GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(new String[] { dataStreamName });
            GetDataStreamAction.Response getDataStreamResponse = client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest)
                .actionGet();
            assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(1));
            DataStream dataStream = getDataStreamResponse.getDataStreams().get(0).getDataStream();
            assertThat(dataStream.getName(), equalTo(dataStreamName));
            List<Index> backingIndices = dataStream.getIndices();
            assertThat(backingIndices.size(), equalTo(1));
            String writeIndex = dataStream.getWriteIndex().getName();
            assertThat(writeIndex, backingIndexEqualTo(dataStreamName, finalGeneration));
        });
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/96084")
    public void testAutomaticForceMerge() throws Exception {
        /*
         * This test makes sure that (1) DLM does _not_ call forcemerge on an index in the same DLM pass when it rolls over the index and
         * that (2) it _does_ call forcemerge on an index that was rolled over in a previous DLM pass.
         * It's harder than you would think to detect through the REST API that forcemerge has been called. The reason is that segment
         * merging happens automatically during indexing, and when forcemerge is called it likely does nothing because all nececssary
         * merging has already happened automatically. So in order to detect whether forcemerge has been called, we use a
         * SendRequestBehavior in the MockTransportService to detect it.
         */
        DataLifecycle lifecycle = new DataLifecycle();
        disableDLM();
        String dataStreamName = "metrics-foo";
        putComposableIndexTemplate(
            "id1",
            null,
            List.of(dataStreamName + "*"),
            Settings.builder().put("index.number_of_replicas", 1).put("index.number_of_shards", 1).build(),
            null,
            lifecycle
        );
        // This is the set of all indices against which a ForceMergeAction has been run:
        final Set<String> forceMergedIndices = new HashSet<>();
        {
            // This creates a SendRequestBehavior to add the name of any index that has been forcemerged to the forceMergedIndices set.
            final StubbableTransport.SendRequestBehavior sendBehavior = (connection, requestId, action, request, options) -> {
                if (action.startsWith(ForceMergeAction.NAME)) {
                    String index = ((IndicesRequest) request).indices()[0];
                    forceMergedIndices.add(index);
                    logger.info("Force merging {}", index);
                }
                connection.sendRequest(requestId, action, request, options);
            };
            String masterNode = internalCluster().getMasterName();
            final MockTransportService targetTransportService = (MockTransportService) internalCluster().getInstance(
                TransportService.class,
                masterNode
            );

            for (DiscoveryNode node : internalCluster().getInstance(ClusterService.class, internalCluster().getMasterName())
                .state()
                .getNodes()) {
                if (node.canContainData() && node.getName().equals(masterNode) == false) {
                    final TransportService sourceTransportService = internalCluster().getInstance(TransportService.class, node.getName());
                    targetTransportService.addSendBehavior(sourceTransportService, sendBehavior);
                }
            }
        }

        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(dataStreamName);
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();
        int finalGeneration = randomIntBetween(2, 10);
        for (int currentGeneration = 1; currentGeneration < finalGeneration; currentGeneration++) {
            // This is currently the write index, but it will be rolled over as soon as DLM runs:
            final String toBeRolledOverIndex = DataStream.getDefaultBackingIndexName(dataStreamName, currentGeneration);
            for (int i = 0; i < randomIntBetween(10, 50); i++) {
                indexDocs(dataStreamName, randomIntBetween(1, 300));
                // Make sure the segments get written:
                FlushResponse flushResponse = client().admin().indices().flush(new FlushRequest(toBeRolledOverIndex)).actionGet();
                assertThat(flushResponse.getStatus(), equalTo(RestStatus.OK));
            }

            final String toBeForceMergedIndex;
            if (currentGeneration == 1) {
                toBeForceMergedIndex = null; // Not going to be used
            } else {
                toBeForceMergedIndex = DataStream.getDefaultBackingIndexName(dataStreamName, currentGeneration - 1);
            }
            int currentBackingIndexCount = currentGeneration;
            DataLifecycleService dataLifecycleService = internalCluster().getInstance(
                DataLifecycleService.class,
                internalCluster().getMasterName()
            );
            ClusterService clusterService = internalCluster().getInstance(ClusterService.class, internalCluster().getMasterName());
            // run DLM once
            dataLifecycleService.run(clusterService.state());
            assertBusy(() -> {
                GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(new String[] { dataStreamName });
                GetDataStreamAction.Response getDataStreamResponse = client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest)
                    .actionGet();
                assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(1));
                DataStream dataStream = getDataStreamResponse.getDataStreams().get(0).getDataStream();
                assertThat(dataStream.getName(), equalTo(dataStreamName));
                List<Index> backingIndices = dataStream.getIndices();
                assertThat(backingIndices.size(), equalTo(currentBackingIndexCount + 1));
                String writeIndex = dataStream.getWriteIndex().getName();
                assertThat(writeIndex, backingIndexEqualTo(dataStreamName, currentBackingIndexCount + 1));
                /*
                 * We only expect forcemerge to happen on the 2nd DLM run and later, since on the first there's only the single write
                 * index to be rolled over.
                 */
                if (currentBackingIndexCount > 1) {
                    assertThat(
                        "The segments for " + toBeForceMergedIndex + " were not merged",
                        forceMergedIndices.contains(toBeForceMergedIndex),
                        equalTo(true)
                    );
                }
                // We want to assert that when DLM rolls over the write index it, it doesn't forcemerge it on that iteration:
                assertThat(
                    "The segments for " + toBeRolledOverIndex + " were unexpectedly merged",
                    forceMergedIndices.contains(toBeRolledOverIndex),
                    equalTo(false)
                );
            });
        }
    }

    private static void disableDLM() {
        updateClusterSettings(Settings.builder().put(DataLifecycleService.DLM_POLL_INTERVAL, TimeValue.MAX_VALUE));
    }

    public void testErrorRecordingOnRollover() throws Exception {
        // empty lifecycle contains the default rollover
        DataLifecycle lifecycle = new DataLifecycle();
        /*
         * We set index.auto_expand_replicas to 0-1 so that if we get a single-node cluster it is not yellow. The cluster being yellow
         * could result in DLM's automatic forcemerge failing, which would result in an unexpected error in the error store.
         */
        putComposableIndexTemplate(
            "id1",
            null,
            List.of("metrics-foo*"),
            Settings.builder().put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1").build(),
            null,
            lifecycle
        );
        Iterable<DataLifecycleService> dataLifecycleServices = internalCluster().getInstances(DataLifecycleService.class);

        String dataStreamName = "metrics-foo";
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(dataStreamName);
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();

        indexDocs(dataStreamName, 1);

        // let's allow one rollover to go through
        assertBusy(() -> {
            GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(new String[] { dataStreamName });
            GetDataStreamAction.Response getDataStreamResponse = client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest)
                .actionGet();
            assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(1));
            assertThat(getDataStreamResponse.getDataStreams().get(0).getDataStream().getName(), equalTo(dataStreamName));
            List<Index> backingIndices = getDataStreamResponse.getDataStreams().get(0).getDataStream().getIndices();
            assertThat(backingIndices.size(), equalTo(2));
            String backingIndex = backingIndices.get(0).getName();
            assertThat(backingIndex, backingIndexEqualTo(dataStreamName, 1));
            String writeIndex = backingIndices.get(1).getName();
            assertThat(writeIndex, backingIndexEqualTo(dataStreamName, 2));
        });

        // prevent new indices from being created (ie. future rollovers)
        updateClusterSettings(Settings.builder().put(SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey(), 1));

        indexDocs(dataStreamName, 1);

        assertBusy(() -> {
            String writeIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, 2);
            String writeIndexRolloverError = null;
            Iterable<DataLifecycleService> lifecycleServices = internalCluster().getInstances(DataLifecycleService.class);

            for (DataLifecycleService lifecycleService : lifecycleServices) {
                writeIndexRolloverError = lifecycleService.getErrorStore().getError(writeIndexName);
                if (writeIndexRolloverError != null) {
                    break;
                }
            }

            assertThat(writeIndexRolloverError, is(notNullValue()));
            assertThat(writeIndexRolloverError, containsString("maximum normal shards open"));
        });

        // let's reset the cluster max shards per node limit to allow rollover to proceed and check the error store is empty
        updateClusterSettings(Settings.builder().putNull("*"));

        assertBusy(() -> {
            GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(new String[] { dataStreamName });
            GetDataStreamAction.Response getDataStreamResponse = client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest)
                .actionGet();
            assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(1));
            assertThat(getDataStreamResponse.getDataStreams().get(0).getDataStream().getName(), equalTo(dataStreamName));
            List<Index> backingIndices = getDataStreamResponse.getDataStreams().get(0).getDataStream().getIndices();
            assertThat(backingIndices.size(), equalTo(3));
            String writeIndex = backingIndices.get(2).getName();
            // rollover was successful and we got to generation 3
            assertThat(writeIndex, backingIndexEqualTo(dataStreamName, 3));

            // we recorded the error against the previous write index (generation 2)
            // let's check there's no error recorded against it anymore
            String previousWriteInddex = DataStream.getDefaultBackingIndexName(dataStreamName, 2);
            Iterable<DataLifecycleService> lifecycleServices = internalCluster().getInstances(DataLifecycleService.class);

            for (DataLifecycleService lifecycleService : lifecycleServices) {
                assertThat(lifecycleService.getErrorStore().getError(previousWriteInddex), nullValue());
            }
        });
    }

    public void testErrorRecordingOnRetention() throws Exception {
        // starting with a lifecycle without retention so we can rollover the data stream and manipulate the second generation index such
        // that its retention execution fails
        DataLifecycle lifecycle = new DataLifecycle();

        /*
         * We set index.auto_expand_replicas to 0-1 so that if we get a single-node cluster it is not yellow. The cluster being yellow
         * could result in DLM's automatic forcemerge failing, which would result in an unexpected error in the error store.
         */
        putComposableIndexTemplate(
            "id1",
            null,
            List.of("metrics-foo*"),
            Settings.builder().put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1").build(),
            null,
            lifecycle
        );
        Iterable<DataLifecycleService> dataLifecycleServices = internalCluster().getInstances(DataLifecycleService.class);

        String dataStreamName = "metrics-foo";
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(dataStreamName);
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();
        indexDocs(dataStreamName, 1);

        // let's allow one rollover to go through
        assertBusy(() -> {
            GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(new String[] { dataStreamName });
            GetDataStreamAction.Response getDataStreamResponse = client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest)
                .actionGet();
            assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(1));
            assertThat(getDataStreamResponse.getDataStreams().get(0).getDataStream().getName(), equalTo(dataStreamName));
            List<Index> backingIndices = getDataStreamResponse.getDataStreams().get(0).getDataStream().getIndices();
            assertThat(backingIndices.size(), equalTo(2));
            String backingIndex = backingIndices.get(0).getName();
            assertThat(backingIndex, backingIndexEqualTo(dataStreamName, 1));
            String writeIndex = backingIndices.get(1).getName();
            assertThat(writeIndex, backingIndexEqualTo(dataStreamName, 2));
        });

        String firstGenerationIndex = DataStream.getDefaultBackingIndexName(dataStreamName, 1L);

        // mark the first generation index as read-only so deletion fails when we enable the retention configuration
        updateIndexSettings(Settings.builder().put(READ_ONLY.settingName(), true), firstGenerationIndex);
        try {
            updateLifecycle(dataStreamName, TimeValue.timeValueSeconds(1));

            assertBusy(() -> {
                GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(new String[] { dataStreamName });
                GetDataStreamAction.Response getDataStreamResponse = client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest)
                    .actionGet();
                assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(1));
                assertThat(getDataStreamResponse.getDataStreams().get(0).getDataStream().getName(), equalTo(dataStreamName));
                List<Index> backingIndices = getDataStreamResponse.getDataStreams().get(0).getDataStream().getIndices();
                assertThat(backingIndices.size(), equalTo(2));
                String writeIndex = backingIndices.get(1).getName();
                assertThat(writeIndex, backingIndexEqualTo(dataStreamName, 2));

                String recordedRetentionExecutionError = null;
                Iterable<DataLifecycleService> lifecycleServices = internalCluster().getInstances(DataLifecycleService.class);

                for (DataLifecycleService lifecycleService : lifecycleServices) {
                    recordedRetentionExecutionError = lifecycleService.getErrorStore().getError(firstGenerationIndex);
                    if (recordedRetentionExecutionError != null) {
                        break;
                    }
                }

                assertThat(recordedRetentionExecutionError, is(notNullValue()));
                assertThat(recordedRetentionExecutionError, containsString("blocked by: [FORBIDDEN/5/index read-only (api)"));
            });

            // let's mark the index as writeable and make sure it's deleted and the error store is empty
            updateIndexSettings(Settings.builder().put(READ_ONLY.settingName(), false), firstGenerationIndex);

            assertBusy(() -> {
                GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(new String[] { dataStreamName });
                GetDataStreamAction.Response getDataStreamResponse = client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest)
                    .actionGet();
                assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(1));
                assertThat(getDataStreamResponse.getDataStreams().get(0).getDataStream().getName(), equalTo(dataStreamName));
                List<Index> backingIndices = getDataStreamResponse.getDataStreams().get(0).getDataStream().getIndices();
                // data stream only has one index now
                assertThat(backingIndices.size(), equalTo(1));

                // error stores don't contain anything for the first generation index anymore
                Iterable<DataLifecycleService> lifecycleServices = internalCluster().getInstances(DataLifecycleService.class);
                for (DataLifecycleService lifecycleService : lifecycleServices) {
                    assertThat(lifecycleService.getErrorStore().getError(firstGenerationIndex), nullValue());
                }
            });
        } finally {
            // when the test executes successfully this will not be needed however, otherwise we need to make sure the index is
            // "delete-able" for test cleanup
            try {
                updateIndexSettings(Settings.builder().put(READ_ONLY.settingName(), false), firstGenerationIndex);
            } catch (Exception e) {
                // index would be deleted if the test is successful
            }
        }
    }

    static void indexDocs(String dataStream, int numDocs) {
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < numDocs; i++) {
            String value = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(System.currentTimeMillis());
            bulkRequest.add(
                new IndexRequest(dataStream).opType(DocWriteRequest.OpType.CREATE)
                    .source(String.format(Locale.ROOT, "{\"%s\":\"%s\"}", DEFAULT_TIMESTAMP_FIELD, value), XContentType.JSON)
            );
        }
        BulkResponse bulkResponse = client().bulk(bulkRequest).actionGet();
        assertThat(bulkResponse.getItems().length, equalTo(numDocs));
        String backingIndexPrefix = DataStream.BACKING_INDEX_PREFIX + dataStream;
        for (BulkItemResponse itemResponse : bulkResponse) {
            assertThat(itemResponse.getFailureMessage(), nullValue());
            assertThat(itemResponse.status(), equalTo(RestStatus.CREATED));
            assertThat(itemResponse.getIndex(), startsWith(backingIndexPrefix));
        }
        client().admin().indices().refresh(new RefreshRequest(dataStream)).actionGet();
    }

    static void putComposableIndexTemplate(
        String id,
        @Nullable String mappings,
        List<String> patterns,
        @Nullable Settings settings,
        @Nullable Map<String, Object> metadata,
        @Nullable DataLifecycle lifecycle
    ) throws IOException {
        PutComposableIndexTemplateAction.Request request = new PutComposableIndexTemplateAction.Request(id);
        request.indexTemplate(
            new ComposableIndexTemplate(
                patterns,
                new Template(settings, mappings == null ? null : CompressedXContent.fromJSON(mappings), null, lifecycle),
                null,
                null,
                null,
                metadata,
                new ComposableIndexTemplate.DataStreamTemplate(),
                null
            )
        );
        client().execute(PutComposableIndexTemplateAction.INSTANCE, request).actionGet();
    }

    static void updateLifecycle(String dataStreamName, TimeValue dataRetention) {
        PutDataLifecycleAction.Request putDataLifecycleRequest = new PutDataLifecycleAction.Request(
            new String[] { dataStreamName },
            dataRetention
        );
        AcknowledgedResponse putDataLifecycleResponse = client().execute(PutDataLifecycleAction.INSTANCE, putDataLifecycleRequest)
            .actionGet();
        assertThat(putDataLifecycleResponse.isAcknowledged(), equalTo(true));
    }
}
