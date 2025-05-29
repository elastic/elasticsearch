/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.datastreams.lifecycle;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.rollover.Condition;
import org.elasticsearch.action.admin.indices.rollover.RolloverAction;
import org.elasticsearch.action.admin.indices.rollover.RolloverConditions;
import org.elasticsearch.action.admin.indices.rollover.RolloverConfiguration;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.datastreams.lifecycle.ExplainDataStreamLifecycleAction;
import org.elasticsearch.action.datastreams.lifecycle.ExplainIndexDataStreamLifecycle;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamFailureStore;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.DataStreamOptions;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xcontent.XContentType;
import org.junit.After;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.backingIndexEqualTo;
import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.dataStreamIndexEqualTo;
import static org.elasticsearch.cluster.metadata.MetadataIndexTemplateService.DEFAULT_TIMESTAMP_FIELD;
import static org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleServiceIT.TestSystemDataStreamPlugin.SYSTEM_DATA_STREAM_NAME;
import static org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleServiceIT.TestSystemDataStreamPlugin.SYSTEM_DATA_STREAM_RETENTION_DAYS;
import static org.elasticsearch.indices.ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class ExplainDataStreamLifecycleIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(
            DataStreamsPlugin.class,
            MockTransportService.TestPlugin.class,
            DataStreamLifecycleServiceIT.TestSystemDataStreamPlugin.class,
            MapperExtrasPlugin.class
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder settings = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        settings.put(DataStreamLifecycleService.DATA_STREAM_LIFECYCLE_POLL_INTERVAL, "1s");
        settings.put(DataStreamLifecycle.CLUSTER_LIFECYCLE_DEFAULT_ROLLOVER_SETTING.getKey(), "min_docs=1,max_docs=1");
        return settings.build();
    }

    @After
    public void cleanup() {
        // we change SETTING_CLUSTER_MAX_SHARDS_PER_NODE in a test so let's make sure we clean it up even when the test fails
        updateClusterSettings(Settings.builder().putNull("*"));
    }

    public void testExplainLifecycle() throws Exception {
        // empty lifecycle contains the default rollover
        DataStreamLifecycle.Template lifecycle = DataStreamLifecycle.Template.DATA_DEFAULT;

        putComposableIndexTemplate("id1", null, List.of("metrics-foo*"), null, null, lifecycle);
        String dataStreamName = "metrics-foo";
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            dataStreamName
        );
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();

        indexDocs(dataStreamName, 1);

        List<String> backingIndices = waitForDataStreamBackingIndices(dataStreamName, 2);
        String firstGenerationIndex = backingIndices.get(0);
        assertThat(firstGenerationIndex, backingIndexEqualTo(dataStreamName, 1));
        String secondGenerationIndex = backingIndices.get(1);
        assertThat(secondGenerationIndex, backingIndexEqualTo(dataStreamName, 2));

        {
            ExplainDataStreamLifecycleAction.Request explainIndicesRequest = new ExplainDataStreamLifecycleAction.Request(
                TEST_REQUEST_TIMEOUT,
                new String[] { firstGenerationIndex, secondGenerationIndex }
            );
            ExplainDataStreamLifecycleAction.Response response = client().execute(
                ExplainDataStreamLifecycleAction.INSTANCE,
                explainIndicesRequest
            ).actionGet();
            assertThat(response.getIndices().size(), is(2));
            // we requested the explain for indices with the default include_details=false
            assertThat(response.getRolloverConfiguration(), nullValue());
            for (ExplainIndexDataStreamLifecycle explainIndex : response.getIndices()) {
                assertThat(explainIndex.isManagedByLifecycle(), is(true));
                assertThat(explainIndex.getIndexCreationDate(), notNullValue());
                assertThat(explainIndex.getLifecycle(), notNullValue());
                assertThat(explainIndex.getLifecycle().dataRetention(), nullValue());
                if (internalCluster().numDataNodes() > 1) {
                    // If the number of nodes is 1 then the cluster will be yellow so forcemerge will report an error if it has run
                    assertThat(explainIndex.getError(), nullValue());
                }

                if (explainIndex.getIndex().equals(firstGenerationIndex)) {
                    // first generation index was rolled over
                    assertThat(explainIndex.getRolloverDate(), notNullValue());
                    assertThat(explainIndex.getTimeSinceRollover(System::currentTimeMillis), notNullValue());
                    assertThat(explainIndex.getGenerationTime(System::currentTimeMillis), notNullValue());
                } else {
                    // the write index has not been rolled over yet
                    assertThat(explainIndex.getRolloverDate(), nullValue());
                    assertThat(explainIndex.getTimeSinceRollover(System::currentTimeMillis), nullValue());
                    assertThat(explainIndex.getGenerationTime(System::currentTimeMillis), nullValue());
                }
            }
        }

        {
            // let's also explain with include_defaults=true
            ExplainDataStreamLifecycleAction.Request explainIndicesRequest = new ExplainDataStreamLifecycleAction.Request(
                TEST_REQUEST_TIMEOUT,
                new String[] { firstGenerationIndex, secondGenerationIndex },
                true
            );
            ExplainDataStreamLifecycleAction.Response response = client().execute(
                ExplainDataStreamLifecycleAction.INSTANCE,
                explainIndicesRequest
            ).actionGet();
            assertThat(response.getIndices().size(), is(2));
            RolloverConfiguration rolloverConfiguration = response.getRolloverConfiguration();
            assertThat(rolloverConfiguration, notNullValue());
            Map<String, Condition<?>> conditions = rolloverConfiguration.resolveRolloverConditions(null).getConditions();
            assertThat(conditions.size(), is(2));
            assertThat(conditions.get(RolloverConditions.MAX_DOCS_FIELD.getPreferredName()).value(), is(1L));
            assertThat(conditions.get(RolloverConditions.MIN_DOCS_FIELD.getPreferredName()).value(), is(1L));
        }

        {
            // Let's also explain using the data stream name
            ExplainDataStreamLifecycleAction.Request explainIndicesRequest = new ExplainDataStreamLifecycleAction.Request(
                TEST_REQUEST_TIMEOUT,
                new String[] { dataStreamName }
            );
            ExplainDataStreamLifecycleAction.Response response = client().execute(
                ExplainDataStreamLifecycleAction.INSTANCE,
                explainIndicesRequest
            ).actionGet();
            assertThat(response.getIndices().size(), is(2));
            for (ExplainIndexDataStreamLifecycle explainIndex : response.getIndices()) {
                assertThat(explainIndex.isManagedByLifecycle(), is(true));
                assertThat(explainIndex.getIndexCreationDate(), notNullValue());
                assertThat(explainIndex.getLifecycle(), notNullValue());
                assertThat(explainIndex.getLifecycle().dataRetention(), nullValue());

                if (explainIndex.getIndex().equals(firstGenerationIndex)) {
                    // first generation index was rolled over
                    assertThat(explainIndex.getRolloverDate(), notNullValue());
                    assertThat(explainIndex.getTimeSinceRollover(System::currentTimeMillis), notNullValue());
                    assertThat(explainIndex.getGenerationTime(System::currentTimeMillis), notNullValue());
                } else {
                    // the write index has not been rolled over yet
                    assertThat(explainIndex.getRolloverDate(), nullValue());
                    assertThat(explainIndex.getTimeSinceRollover(System::currentTimeMillis), nullValue());
                    assertThat(explainIndex.getGenerationTime(System::currentTimeMillis), nullValue());
                }
            }
        }
    }

    public void testSystemExplainLifecycle() throws Exception {
        /*
         * This test makes sure that for system data streams, we correctly ignore the global retention when calling
         * ExplainDataStreamLifecycle. It is very similar to testExplainLifecycle, but only focuses on the retention for a system index.
         */
        try {
            String dataStreamName = SYSTEM_DATA_STREAM_NAME;
            CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                dataStreamName
            );
            client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();

            indexDocs(dataStreamName, 1);

            List<String> backingIndices = waitForDataStreamBackingIndices(dataStreamName, 2);
            String firstGenerationIndex = backingIndices.get(0);
            assertThat(firstGenerationIndex, backingIndexEqualTo(dataStreamName, 1));
            String secondGenerationIndex = backingIndices.get(1);
            assertThat(secondGenerationIndex, backingIndexEqualTo(dataStreamName, 2));

            ExplainDataStreamLifecycleAction.Request explainIndicesRequest = new ExplainDataStreamLifecycleAction.Request(
                TEST_REQUEST_TIMEOUT,
                new String[] { firstGenerationIndex, secondGenerationIndex }
            );
            ExplainDataStreamLifecycleAction.Response response = client().execute(
                ExplainDataStreamLifecycleAction.INSTANCE,
                explainIndicesRequest
            ).actionGet();
            assertThat(response.getIndices().size(), is(2));
            // we requested the explain for indices with the default include_details=false
            assertThat(response.getRolloverConfiguration(), nullValue());
            for (ExplainIndexDataStreamLifecycle explainIndex : response.getIndices()) {
                assertThat(explainIndex.isManagedByLifecycle(), is(true));
                assertThat(explainIndex.getIndexCreationDate(), notNullValue());
                assertThat(explainIndex.getLifecycle(), notNullValue());
                assertThat(
                    explainIndex.getLifecycle().dataRetention(),
                    equalTo(TimeValue.timeValueDays(SYSTEM_DATA_STREAM_RETENTION_DAYS))
                );
            }
        } finally {
            // reset properties
        }
    }

    public void testExplainFailuresLifecycle() throws Exception {
        // Failure indices are always managed unless explicitly disabled.
        putComposableIndexTemplate(
            "id1",
            """
                {
                    "properties": {
                      "@timestamp" : {
                        "type": "date"
                      },
                      "count": {
                        "type": "long"
                      }
                    }
                }""",
            List.of("metrics-foo*"),
            null,
            null,
            null,
            new DataStreamOptions.Template(DataStreamFailureStore.builder().enabled(true).buildTemplate())
        );
        String dataStreamName = "metrics-foo";
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            dataStreamName
        );
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();

        indexFailedDocs(dataStreamName, 1);

        List<String> failureIndices = waitForDataStreamIndices(dataStreamName, 1, true);
        String firstGenerationIndex = failureIndices.get(0);
        assertThat(firstGenerationIndex, DataStreamTestHelper.dataStreamIndexEqualTo(dataStreamName, 2, true));

        indexFailedDocs(dataStreamName, 1);
        failureIndices = waitForDataStreamIndices(dataStreamName, 2, true);
        String secondGenerationIndex = failureIndices.get(1);
        assertThat(secondGenerationIndex, DataStreamTestHelper.dataStreamIndexEqualTo(dataStreamName, 3, true));

        {
            ExplainDataStreamLifecycleAction.Request explainIndicesRequest = new ExplainDataStreamLifecycleAction.Request(
                TEST_REQUEST_TIMEOUT,
                new String[] { firstGenerationIndex, secondGenerationIndex }
            );
            ExplainDataStreamLifecycleAction.Response response = client().execute(
                ExplainDataStreamLifecycleAction.INSTANCE,
                explainIndicesRequest
            ).actionGet();
            assertThat(response.getIndices().size(), is(2));
            // we requested the explain for indices with the default include_details=false
            assertThat(response.getRolloverConfiguration(), nullValue());
            for (ExplainIndexDataStreamLifecycle explainIndex : response.getIndices()) {
                assertThat(explainIndex.isManagedByLifecycle(), is(true));
                assertThat(explainIndex.getIndexCreationDate(), notNullValue());
                assertThat(explainIndex.getLifecycle(), notNullValue());
                assertThat(explainIndex.getLifecycle().dataRetention(), nullValue());
                if (internalCluster().numDataNodes() > 1) {
                    // If the number of nodes is 1 then the cluster will be yellow so forcemerge will report an error if it has run
                    assertThat(explainIndex.getError(), nullValue());
                }

                if (explainIndex.getIndex().equals(firstGenerationIndex)) {
                    // first generation index was rolled over
                    assertThat(explainIndex.getRolloverDate(), notNullValue());
                    assertThat(explainIndex.getTimeSinceRollover(System::currentTimeMillis), notNullValue());
                    assertThat(explainIndex.getGenerationTime(System::currentTimeMillis), notNullValue());
                } else {
                    // the write index has not been rolled over yet
                    assertThat(explainIndex.getRolloverDate(), nullValue());
                    assertThat(explainIndex.getTimeSinceRollover(System::currentTimeMillis), nullValue());
                    assertThat(explainIndex.getGenerationTime(System::currentTimeMillis), nullValue());
                }
            }
        }

        {
            // let's also explain with include_defaults=true
            ExplainDataStreamLifecycleAction.Request explainIndicesRequest = new ExplainDataStreamLifecycleAction.Request(
                TEST_REQUEST_TIMEOUT,
                new String[] { firstGenerationIndex },
                true
            );
            ExplainDataStreamLifecycleAction.Response response = client().execute(
                ExplainDataStreamLifecycleAction.INSTANCE,
                explainIndicesRequest
            ).actionGet();
            assertThat(response.getIndices().size(), is(1));
            RolloverConfiguration rolloverConfiguration = response.getRolloverConfiguration();
            assertThat(rolloverConfiguration, notNullValue());
            Map<String, Condition<?>> conditions = rolloverConfiguration.resolveRolloverConditions(null).getConditions();
            assertThat(conditions.size(), is(2));
            assertThat(conditions.get(RolloverConditions.MAX_DOCS_FIELD.getPreferredName()).value(), is(1L));
            assertThat(conditions.get(RolloverConditions.MIN_DOCS_FIELD.getPreferredName()).value(), is(1L));
        }

        {
            // Let's also explain using the data stream name
            ExplainDataStreamLifecycleAction.Request explainIndicesRequest = new ExplainDataStreamLifecycleAction.Request(
                TEST_REQUEST_TIMEOUT,
                new String[] { dataStreamName }
            );
            ExplainDataStreamLifecycleAction.Response response = client().execute(
                ExplainDataStreamLifecycleAction.INSTANCE,
                explainIndicesRequest
            ).actionGet();
            assertThat(response.getIndices().size(), is(1));
            for (ExplainIndexDataStreamLifecycle explainIndex : response.getIndices()) {
                if (explainIndex.getIndex().startsWith(DataStream.BACKING_INDEX_PREFIX)) {
                    assertThat(explainIndex.isManagedByLifecycle(), is(false));
                } else {
                    assertThat(explainIndex.isManagedByLifecycle(), is(true));
                    assertThat(explainIndex.getIndexCreationDate(), notNullValue());
                    assertThat(explainIndex.getLifecycle(), notNullValue());
                    assertThat(explainIndex.getLifecycle().dataRetention(), nullValue());

                    if (internalCluster().numDataNodes() > 1) {
                        // If the number of nodes is 1 then the cluster will be yellow so forcemerge will report an error if it has run
                        assertThat(explainIndex.getError(), nullValue());
                    }

                    if (explainIndex.getIndex().equals(firstGenerationIndex)) {
                        // first generation index was rolled over
                        assertThat(explainIndex.getRolloverDate(), notNullValue());
                        assertThat(explainIndex.getTimeSinceRollover(System::currentTimeMillis), notNullValue());
                        assertThat(explainIndex.getGenerationTime(System::currentTimeMillis), notNullValue());
                    } else {
                        // the write index has not been rolled over yet
                        assertThat(explainIndex.getRolloverDate(), nullValue());
                        assertThat(explainIndex.getTimeSinceRollover(System::currentTimeMillis), nullValue());
                        assertThat(explainIndex.getGenerationTime(System::currentTimeMillis), nullValue());
                    }
                }
            }
        }
    }

    public void testExplainLifecycleForIndicesWithErrors() throws Exception {
        // empty lifecycle contains the default rollover
        DataStreamLifecycle.Template lifecycle = DataStreamLifecycle.Template.DATA_DEFAULT;

        putComposableIndexTemplate(
            "id1",
            """
                {
                    "properties": {
                      "@timestamp" : {
                        "type": "date"
                      },
                      "count": {
                        "type": "long"
                      }
                    }
                }""",
            List.of("metrics-foo*"),
            null,
            null,
            lifecycle,
            new DataStreamOptions.Template(DataStreamFailureStore.builder().enabled(true).buildTemplate())
        );
        String dataStreamName = "metrics-foo";
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            dataStreamName
        );
        safeGet(client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest));
        safeGet(client().execute(RolloverAction.INSTANCE, new RolloverRequest(dataStreamName + "::failures", null)));

        indexDocs(dataStreamName, 1);

        // let's allow one rollover to go through
        List<String> backingIndices = waitForDataStreamBackingIndices(dataStreamName, 2);
        String firstGenerationIndex = backingIndices.get(0);
        assertThat(firstGenerationIndex, backingIndexEqualTo(dataStreamName, 1));
        String secondGenerationIndex = backingIndices.get(1);
        assertThat(secondGenerationIndex, backingIndexEqualTo(dataStreamName, 3));
        // let's ensure that the failure store is initialised
        List<String> failureIndices = waitForDataStreamIndices(dataStreamName, 1, true);
        String firstGenerationFailureIndex = failureIndices.get(0);
        assertThat(firstGenerationFailureIndex, dataStreamIndexEqualTo(dataStreamName, 2, true));

        // prevent new indices from being created (ie. future rollovers)
        updateClusterSettings(Settings.builder().put(SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey(), 1));

        indexDocs(dataStreamName, 1);
        indexFailedDocs(dataStreamName, 1);

        assertBusy(() -> {
            ExplainDataStreamLifecycleAction.Request explainIndicesRequest = new ExplainDataStreamLifecycleAction.Request(
                TEST_REQUEST_TIMEOUT,
                new String[] { secondGenerationIndex, firstGenerationFailureIndex }
            );
            ExplainDataStreamLifecycleAction.Response response = client().execute(
                ExplainDataStreamLifecycleAction.INSTANCE,
                explainIndicesRequest
            ).actionGet();
            assertThat(response.getIndices().size(), is(2));
            // we requested the explain for indices with the default include_details=false
            assertThat(response.getRolloverConfiguration(), nullValue());
            for (int i = 0; i < 2; i++) {
                ExplainIndexDataStreamLifecycle explainIndex = response.getIndices().get(i);
                if (i == 0) {
                    assertThat(explainIndex.getIndex(), is(secondGenerationIndex));
                } else {
                    assertThat(explainIndex.getIndex(), is(firstGenerationFailureIndex));
                }
                assertThat(explainIndex.getIndex(), explainIndex.isManagedByLifecycle(), is(true));
                assertThat(explainIndex.getIndex(), explainIndex.getIndexCreationDate(), notNullValue());
                assertThat(explainIndex.getIndex(), explainIndex.getLifecycle(), notNullValue());
                assertThat(explainIndex.getIndex(), explainIndex.getLifecycle().dataRetention(), nullValue());
                assertThat(explainIndex.getIndex(), explainIndex.getRolloverDate(), nullValue());
                assertThat(explainIndex.getIndex(), explainIndex.getTimeSinceRollover(System::currentTimeMillis), nullValue());
                // index has not been rolled over yet
                assertThat(explainIndex.getIndex(), explainIndex.getGenerationTime(System::currentTimeMillis), nullValue());

                assertThat(explainIndex.getIndex(), explainIndex.getError(), notNullValue());
                assertThat(explainIndex.getIndex(), explainIndex.getError().error(), containsString("maximum normal shards open"));
                assertThat(explainIndex.getIndex(), explainIndex.getError().retryCount(), greaterThanOrEqualTo(1));
            }
        });

        // let's reset the cluster max shards per node limit to allow rollover to proceed and check the reported error is null
        updateClusterSettings(Settings.builder().putNull("*"));

        assertBusy(() -> {
            ExplainDataStreamLifecycleAction.Request explainIndicesRequest = new ExplainDataStreamLifecycleAction.Request(
                TEST_REQUEST_TIMEOUT,
                new String[] { secondGenerationIndex, firstGenerationFailureIndex }
            );
            ExplainDataStreamLifecycleAction.Response response = client().execute(
                ExplainDataStreamLifecycleAction.INSTANCE,
                explainIndicesRequest
            ).actionGet();
            assertThat(response.getIndices().size(), is(2));
            if (internalCluster().numDataNodes() > 1) {
                assertThat(response.getIndices().get(0).getError(), is(nullValue()));
                assertThat(response.getIndices().get(1).getError(), is(nullValue()));
            } else {
                /*
                 * If there is only one node in the cluster then the replica shard will never be allocated. So forcemerge will never
                 * succeed, and there will always be an error in the error store. This behavior is subject to change in the future.
                 */
                assertThat(response.getIndices().get(0).getError(), is(notNullValue()));
                assertThat(response.getIndices().get(0).getError().error(), containsString("Force merge request "));
                assertThat(response.getIndices().get(1).getError(), is(notNullValue()));
                assertThat(response.getIndices().get(1).getError().error(), containsString("Force merge request "));
            }
        });
    }

    public void testExplainDataStreamLifecycleForUnmanagedIndices() throws Exception {
        String dataStreamName = "metrics-foo";
        putComposableIndexTemplate(
            "id1",
            null,
            List.of("metrics-foo*"),
            null,
            null,
            DataStreamLifecycle.dataLifecycleBuilder().enabled(false).buildTemplate()
        );
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            "metrics-foo"
        );
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();

        indexDocs(dataStreamName, 4);

        List<String> backingIndices = waitForDataStreamBackingIndices(dataStreamName, 1);
        String firstGenerationIndex = backingIndices.get(0);
        assertThat(firstGenerationIndex, backingIndexEqualTo(dataStreamName, 1));

        assertBusy(() -> {
            ExplainDataStreamLifecycleAction.Request explainIndicesRequest = new ExplainDataStreamLifecycleAction.Request(
                TEST_REQUEST_TIMEOUT,
                new String[] { firstGenerationIndex }
            );
            ExplainDataStreamLifecycleAction.Response response = client().execute(
                ExplainDataStreamLifecycleAction.INSTANCE,
                explainIndicesRequest
            ).actionGet();
            assertThat(response.getIndices().size(), is(1));
            assertThat(response.getRolloverConfiguration(), nullValue());
            for (ExplainIndexDataStreamLifecycle explainIndex : response.getIndices()) {
                assertThat(explainIndex.isManagedByLifecycle(), is(false));
                assertThat(explainIndex.getIndex(), is(firstGenerationIndex));
                assertThat(explainIndex.getIndexCreationDate(), nullValue());
                assertThat(explainIndex.getLifecycle(), nullValue());
                assertThat(explainIndex.getGenerationTime(System::currentTimeMillis), nullValue());
                assertThat(explainIndex.getRolloverDate(), nullValue());
                assertThat(explainIndex.getTimeSinceRollover(System::currentTimeMillis), nullValue());
                if (internalCluster().numDataNodes() > 1) {
                    // If the number of nodes is 1 then the cluster will be yellow so forcemerge will report an error if it has run
                    assertThat(explainIndex.getError(), nullValue());
                }
            }
        });
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
        indicesAdmin().refresh(new RefreshRequest(dataStream)).actionGet();
    }

    private void indexFailedDocs(String dataStream, int numDocs) {
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < numDocs; i++) {
            String value = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(System.currentTimeMillis());
            bulkRequest.add(
                new IndexRequest(dataStream).opType(DocWriteRequest.OpType.CREATE)
                    .source(
                        String.format(Locale.ROOT, "{\"%s\":\"%s\", \"count\":\"not-a-number\"}", DEFAULT_TIMESTAMP_FIELD, value),
                        XContentType.JSON
                    )
            );
        }
        BulkResponse bulkResponse = safeGet(client().bulk(bulkRequest));
        assertThat(bulkResponse.getItems().length, equalTo(numDocs));
        String failureIndexPrefix = DataStream.FAILURE_STORE_PREFIX + dataStream;
        for (BulkItemResponse itemResponse : bulkResponse) {
            assertThat(itemResponse.getFailureMessage(), nullValue());
            assertThat(itemResponse.status(), equalTo(RestStatus.CREATED));
            assertThat(itemResponse.getIndex(), startsWith(failureIndexPrefix));
        }
        safeGet(indicesAdmin().refresh(new RefreshRequest(dataStream)));
    }

    static void putComposableIndexTemplate(
        String id,
        @Nullable String mappings,
        List<String> patterns,
        @Nullable Settings settings,
        @Nullable Map<String, Object> metadata,
        @Nullable DataStreamLifecycle.Template lifecycle
    ) throws IOException {
        putComposableIndexTemplate(id, mappings, patterns, settings, metadata, lifecycle, null);
    }

    static void putComposableIndexTemplate(
        String id,
        @Nullable String mappings,
        List<String> patterns,
        @Nullable Settings settings,
        @Nullable Map<String, Object> metadata,
        @Nullable DataStreamLifecycle.Template lifecycle,
        @Nullable DataStreamOptions.Template options
    ) throws IOException {
        TransportPutComposableIndexTemplateAction.Request request = new TransportPutComposableIndexTemplateAction.Request(id);
        request.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(patterns)
                .template(
                    Template.builder()
                        .settings(settings)
                        .mappings(mappings == null ? null : CompressedXContent.fromJSON(mappings))
                        .lifecycle(lifecycle)
                        .dataStreamOptions(options)
                )
                .metadata(metadata)
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                .build()
        );
        client().execute(TransportPutComposableIndexTemplateAction.TYPE, request).actionGet();
    }

}
