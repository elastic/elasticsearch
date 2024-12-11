/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless;

import co.elastic.elasticsearch.stateless.commits.HollowShardsService;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.datastreams.GetDataStreamAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicReference;

import static co.elastic.elasticsearch.stateless.commits.HollowShardsService.SETTING_HOLLOW_INGESTION_DS_NON_WRITE_TTL;
import static co.elastic.elasticsearch.stateless.commits.HollowShardsService.SETTING_HOLLOW_INGESTION_TTL;
import static co.elastic.elasticsearch.stateless.commits.HollowShardsService.STATELESS_HOLLOW_INDEX_SHARDS_ENABLED;
import static org.elasticsearch.cluster.metadata.MetadataIndexTemplateService.DEFAULT_TIMESTAMP_FIELD;
import static org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService.DATA_STREAM_LIFECYCLE_POLL_INTERVAL;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class StatelessHollowIndexShardsIT extends AbstractStatelessIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(DataStreamsPlugin.class);
        return plugins;
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(STATELESS_HOLLOW_INDEX_SHARDS_ENABLED.getKey(), true);
    }

    public void testHollowIndexShardsEnabledSetting() {
        boolean hollowShardsEnabled = randomBoolean();
        final var indexingNode = startMasterAndIndexNode(
            Settings.builder().put(STATELESS_HOLLOW_INDEX_SHARDS_ENABLED.getKey(), hollowShardsEnabled).build()
        );

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);

        var hollowShardsService = internalCluster().getInstance(HollowShardsService.class, indexingNode);
        assertThat(hollowShardsService.isFeatureEnabled(), equalTo(hollowShardsEnabled));
    }

    public void testIsHollowableRegularIndex() throws Exception {
        boolean lowTtl = randomBoolean();
        final var indexingNode = startMasterAndIndexNode(
            lowTtl ? Settings.builder().put(SETTING_HOLLOW_INGESTION_TTL.getKey(), TimeValue.timeValueMillis(1)).build() : Settings.EMPTY
        );

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);

        indexDocs(indexName, randomIntBetween(1, 10));
        final var hollowShardsService = internalCluster().getInstance(HollowShardsService.class, indexingNode);
        final var indexShard = findIndexShard(indexName);
        if (lowTtl) {
            assertBusy(() -> assertThat(hollowShardsService.isHollowableIndexShard(indexShard), equalTo(true)));
        } else {
            assertThat(hollowShardsService.isHollowableIndexShard(indexShard), equalTo(false));
        }
    }

    public void testIsHollowableDataStreamWriteIndex() throws Exception {
        boolean lowTtl = randomBoolean();
        final var settings = Settings.builder()
            .put(DATA_STREAM_LIFECYCLE_POLL_INTERVAL, TimeValue.timeValueSeconds(1))
            .put(DataStreamLifecycle.CLUSTER_LIFECYCLE_DEFAULT_ROLLOVER_SETTING.getKey(), "min_docs=1,max_docs=1");
        if (lowTtl) {
            settings.put(SETTING_HOLLOW_INGESTION_TTL.getKey(), TimeValue.timeValueMillis(1));
        }
        final var indexingNode = startMasterAndIndexNode(settings.build());
        startSearchNode();
        ensureStableCluster(2);

        final var writeIndex = createDataStream("my-data-stream", randomIntBetween(1, 10)).getLast();
        final var hollowShardsService = internalCluster().getInstance(HollowShardsService.class, indexingNode);
        final var indexShard = findIndexShard(writeIndex.getName());
        if (lowTtl) {
            assertBusy(() -> assertThat(hollowShardsService.isHollowableIndexShard(indexShard), equalTo(true)));
        } else {
            assertThat(hollowShardsService.isHollowableIndexShard(indexShard), equalTo(false));
        }
    }

    public void testIsHollowableDataStreamNonWriteIndex() throws Exception {
        boolean lowTtl = randomBoolean();
        final var settings = Settings.builder()
            .put(DATA_STREAM_LIFECYCLE_POLL_INTERVAL, TimeValue.timeValueSeconds(1))
            .put(DataStreamLifecycle.CLUSTER_LIFECYCLE_DEFAULT_ROLLOVER_SETTING.getKey(), "min_docs=1,max_docs=1");
        if (lowTtl) {
            settings.put(SETTING_HOLLOW_INGESTION_DS_NON_WRITE_TTL.getKey(), TimeValue.timeValueMillis(1));
        }
        final var indexingNode = startMasterAndIndexNode(settings.build());
        startSearchNode();
        ensureStableCluster(2);

        final var nonWriteIndex = createDataStream("my-data-stream", randomIntBetween(1, 10)).getFirst();
        final var hollowShardsService = internalCluster().getInstance(HollowShardsService.class, indexingNode);
        final var indexShard = findIndexShard(nonWriteIndex.getName());
        if (lowTtl) {
            assertBusy(() -> assertThat(hollowShardsService.isHollowableIndexShard(indexShard), equalTo(true)));
        } else {
            assertThat(hollowShardsService.isHollowableIndexShard(indexShard), equalTo(false));
        }
    }

    protected List<Index> createDataStream(String dataStream, int numDocs) throws Exception {
        TransportPutComposableIndexTemplateAction.Request request = new TransportPutComposableIndexTemplateAction.Request("id1");
        request.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(List.of(dataStream + "*"))
                .template(Template.builder().lifecycle(DataStreamLifecycle.newBuilder().dataRetention(TimeValue.timeValueDays(1)).build()))
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                .build()
        );
        client().execute(TransportPutComposableIndexTemplateAction.TYPE, request).actionGet();

        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            dataStream
        );
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();

        indexDocsToDataStream(dataStream, numDocs);

        AtomicReference<List<Index>> backingIndices = new AtomicReference<>();
        assertBusy(() -> {
            GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(
                TEST_REQUEST_TIMEOUT,
                new String[] { dataStream }
            );
            GetDataStreamAction.Response getDataStreamResponse = client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest)
                .actionGet();
            assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(1));
            assertThat(getDataStreamResponse.getDataStreams().get(0).getDataStream().getName(), equalTo(dataStream));
            backingIndices.set(getDataStreamResponse.getDataStreams().get(0).getDataStream().getIndices());
        });
        ensureGreen();
        return backingIndices.get();
    }

    protected void indexDocsToDataStream(String dataStream, int numDocs) {
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

}
