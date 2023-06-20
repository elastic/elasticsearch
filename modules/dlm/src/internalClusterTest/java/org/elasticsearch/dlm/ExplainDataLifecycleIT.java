/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.dlm;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.rollover.Condition;
import org.elasticsearch.action.admin.indices.rollover.RolloverConditions;
import org.elasticsearch.action.admin.indices.rollover.RolloverConfiguration;
import org.elasticsearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.datastreams.GetDataStreamAction;
import org.elasticsearch.action.dlm.ExplainIndexDataLifecycle;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataLifecycle;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.dlm.action.ExplainDataLifecycleAction;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.DateFieldMapper;
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
import static org.elasticsearch.cluster.metadata.MetadataIndexTemplateService.DEFAULT_TIMESTAMP_FIELD;
import static org.elasticsearch.indices.ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class ExplainDataLifecycleIT extends ESIntegTestCase {

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
        settings.put(DataLifecycleService.DATA_STREAM_LIFECYCLE_POLL_INTERVAL, "1s");
        settings.put(DataLifecycle.CLUSTER_LIFECYCLE_DEFAULT_ROLLOVER_SETTING.getKey(), "min_docs=1,max_docs=1");
        return settings.build();
    }

    @After
    public void cleanup() {
        // we change SETTING_CLUSTER_MAX_SHARDS_PER_NODE in a test so let's make sure we clean it up even when the test fails
        updateClusterSettings(Settings.builder().putNull("*"));
    }

    public void testExplainLifecycle() throws Exception {
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

        {
            ExplainDataLifecycleAction.Request explainIndicesRequest = new ExplainDataLifecycleAction.Request(
                new String[] {
                    DataStream.getDefaultBackingIndexName(dataStreamName, 1),
                    DataStream.getDefaultBackingIndexName(dataStreamName, 2) }
            );
            ExplainDataLifecycleAction.Response response = client().execute(ExplainDataLifecycleAction.INSTANCE, explainIndicesRequest)
                .actionGet();
            assertThat(response.getIndices().size(), is(2));
            // we requested the explain for indices with the default include_details=false
            assertThat(response.getRolloverConfiguration(), nullValue());
            for (ExplainIndexDataLifecycle explainIndex : response.getIndices()) {
                assertThat(explainIndex.isManagedByDLM(), is(true));
                assertThat(explainIndex.getIndexCreationDate(), notNullValue());
                assertThat(explainIndex.getLifecycle(), notNullValue());
                assertThat(explainIndex.getLifecycle().getEffectiveDataRetention(), nullValue());
                if (internalCluster().numDataNodes() > 1) {
                    // If the number of nodes is 1 then the cluster will be yellow so forcemerge will report an error if it has run
                    assertThat(explainIndex.getError(), nullValue());
                }

                if (explainIndex.getIndex().equals(DataStream.getDefaultBackingIndexName(dataStreamName, 1))) {
                    // first generation index was rolled over
                    assertThat(explainIndex.getIndex(), is(DataStream.getDefaultBackingIndexName(dataStreamName, 1)));
                    assertThat(explainIndex.getRolloverDate(), notNullValue());
                    assertThat(explainIndex.getTimeSinceRollover(System::currentTimeMillis), notNullValue());
                    assertThat(explainIndex.getGenerationTime(System::currentTimeMillis), notNullValue());
                } else {
                    // the write index has not been rolled over yet
                    assertThat(explainIndex.getGenerationTime(System::currentTimeMillis), nullValue());
                    assertThat(explainIndex.getIndex(), is(DataStream.getDefaultBackingIndexName(dataStreamName, 2)));
                    assertThat(explainIndex.getRolloverDate(), nullValue());
                    assertThat(explainIndex.getTimeSinceRollover(System::currentTimeMillis), nullValue());
                }
            }
        }

        {
            // let's also explain with include_defaults=true
            ExplainDataLifecycleAction.Request explainIndicesRequest = new ExplainDataLifecycleAction.Request(
                new String[] {
                    DataStream.getDefaultBackingIndexName(dataStreamName, 1),
                    DataStream.getDefaultBackingIndexName(dataStreamName, 2) },
                true
            );
            ExplainDataLifecycleAction.Response response = client().execute(ExplainDataLifecycleAction.INSTANCE, explainIndicesRequest)
                .actionGet();
            assertThat(response.getIndices().size(), is(2));
            RolloverConfiguration rolloverConfiguration = response.getRolloverConfiguration();
            assertThat(rolloverConfiguration, notNullValue());
            Map<String, Condition<?>> conditions = rolloverConfiguration.resolveRolloverConditions(null).getConditions();
            assertThat(conditions.size(), is(2));
            assertThat(conditions.get(RolloverConditions.MAX_DOCS_FIELD.getPreferredName()).value(), is(1L));
            assertThat(conditions.get(RolloverConditions.MIN_DOCS_FIELD.getPreferredName()).value(), is(1L));
        }
    }

    public void testExplainLifecycleForIndicesWithErrors() throws Exception {
        // empty lifecycle contains the default rollover
        DataLifecycle lifecycle = new DataLifecycle();

        putComposableIndexTemplate("id1", null, List.of("metrics-foo*"), null, null, lifecycle);

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

        String writeIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, 2);
        assertBusy(() -> {
            ExplainDataLifecycleAction.Request explainIndicesRequest = new ExplainDataLifecycleAction.Request(
                new String[] { writeIndexName }
            );
            ExplainDataLifecycleAction.Response response = client().execute(ExplainDataLifecycleAction.INSTANCE, explainIndicesRequest)
                .actionGet();
            assertThat(response.getIndices().size(), is(1));
            // we requested the explain for indices with the default include_details=false
            assertThat(response.getRolloverConfiguration(), nullValue());
            for (ExplainIndexDataLifecycle explainIndex : response.getIndices()) {
                assertThat(explainIndex.getIndex(), is(writeIndexName));
                assertThat(explainIndex.isManagedByDLM(), is(true));
                assertThat(explainIndex.getIndexCreationDate(), notNullValue());
                assertThat(explainIndex.getLifecycle(), notNullValue());
                assertThat(explainIndex.getLifecycle().getEffectiveDataRetention(), nullValue());
                assertThat(explainIndex.getRolloverDate(), nullValue());
                assertThat(explainIndex.getTimeSinceRollover(System::currentTimeMillis), nullValue());
                // index has not been rolled over yet
                assertThat(explainIndex.getGenerationTime(System::currentTimeMillis), nullValue());

                assertThat(explainIndex.getError(), containsString("maximum normal shards open"));
            }
        });

        // let's reset the cluster max shards per node limit to allow rollover to proceed and check the reported error is null
        updateClusterSettings(Settings.builder().putNull("*"));

        assertBusy(() -> {
            ExplainDataLifecycleAction.Request explainIndicesRequest = new ExplainDataLifecycleAction.Request(
                new String[] { writeIndexName }
            );
            ExplainDataLifecycleAction.Response response = client().execute(ExplainDataLifecycleAction.INSTANCE, explainIndicesRequest)
                .actionGet();
            assertThat(response.getIndices().size(), is(1));
            if (internalCluster().numDataNodes() > 1) {
                assertThat(response.getIndices().get(0).getError(), is(nullValue()));
            } else {
                /*
                 * If there is only one node in the cluster then the replica shard will never be allocated. So forcemerge will never
                 * succeed, and there will always be an error in the error store. This behavior is subject to change in the future.
                 */
                assertThat(response.getIndices().get(0).getError(), is(notNullValue()));
            }
        });
    }

    public void testExplainDLMForUnmanagedIndices() throws Exception {
        String dataStreamName = "metrics-foo";
        putComposableIndexTemplate("id1", null, List.of("metrics-foo*"), null, null, null);
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request("metrics-foo");
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();

        indexDocs(dataStreamName, 4);

        String writeIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, 1);
        assertBusy(() -> {
            ExplainDataLifecycleAction.Request explainIndicesRequest = new ExplainDataLifecycleAction.Request(
                new String[] { writeIndexName }
            );
            ExplainDataLifecycleAction.Response response = client().execute(ExplainDataLifecycleAction.INSTANCE, explainIndicesRequest)
                .actionGet();
            assertThat(response.getIndices().size(), is(1));
            assertThat(response.getRolloverConfiguration(), nullValue());
            for (ExplainIndexDataLifecycle explainIndex : response.getIndices()) {
                assertThat(explainIndex.isManagedByDLM(), is(false));
                assertThat(explainIndex.getIndex(), is(writeIndexName));
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

}
