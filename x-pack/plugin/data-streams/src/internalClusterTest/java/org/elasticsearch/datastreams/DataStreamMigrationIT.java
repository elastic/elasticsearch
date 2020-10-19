/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.datastreams;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.resolve.ResolveIndexAction;
import org.elasticsearch.action.admin.indices.template.delete.DeleteComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.MigrateToDataStreamAction;
import org.elasticsearch.xpack.core.action.DeleteDataStreamAction;
import org.elasticsearch.xpack.datastreams.DataStreamsPlugin;
import org.junit.After;

import java.util.Collection;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.cluster.metadata.MetadataIndexTemplateService.DEFAULT_TIMESTAMP_FIELD;
import static org.elasticsearch.datastreams.DataStreamIT.putComposableIndexTemplate;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class DataStreamMigrationIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(DataStreamsPlugin.class);
    }

    @After
    public void cleanup() {
        AcknowledgedResponse response = client().execute(
            DeleteDataStreamAction.INSTANCE,
            new DeleteDataStreamAction.Request(new String[] { "*" })
        ).actionGet();
        assertAcked(response);

        DeleteDataStreamAction.Request deleteDSRequest = new DeleteDataStreamAction.Request(new String[] { "*" });
        client().execute(DeleteDataStreamAction.INSTANCE, deleteDSRequest).actionGet();
        DeleteComposableIndexTemplateAction.Request deleteTemplateRequest = new DeleteComposableIndexTemplateAction.Request("*");
        client().execute(DeleteComposableIndexTemplateAction.INSTANCE, deleteTemplateRequest).actionGet();
    }

    public void testBasicMigration() throws Exception {
        putComposableIndexTemplate("id1", List.of("migrate*"));

        admin().indices().create(new CreateIndexRequest("index1")).get();
        admin().indices().create(new CreateIndexRequest("index2")).get();

        int numDocs = randomIntBetween(2, 16);
        indexDocs("index1", numDocs);
        numDocs = randomIntBetween(2, 16);
        indexDocs("index2", numDocs);

        String alias = "migrate-to-data-stream";
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAliasAction(IndicesAliasesRequest.AliasActions.add().index("index1").alias(alias).writeIndex(true));
        request.addAliasAction(IndicesAliasesRequest.AliasActions.add().index("index2").alias(alias).writeIndex(false));
        assertAcked(admin().indices().aliases(request).get());

        ResolveIndexAction.Response resolveResponse =
            admin().indices().resolveIndex(new ResolveIndexAction.Request(new String[]{"*"})).get();
        assertThat(resolveResponse.getAliases().size(), equalTo(1));
        assertThat(resolveResponse.getDataStreams().size(), equalTo(0));
        //assertThat(resolveResponse.getIndices().toArray(new ResolveIndexAction.ResolvedIndex[0]), arrayContaining());


        client().execute(MigrateToDataStreamAction.INSTANCE, new MigrateToDataStreamAction.Request(alias)).get();



        /*
        putComposableIndexTemplate("id2", List.of("metrics-bar*"));
        createDataStreamRequest = new CreateDataStreamAction.Request("metrics-bar");
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();

        GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(new String[] { "*" });
        GetDataStreamAction.Response getDataStreamResponse = client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest)
            .actionGet();
        getDataStreamResponse.getDataStreams().sort(Comparator.comparing(dataStreamInfo -> dataStreamInfo.getDataStream().getName()));
        assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(2));
        DataStream firstDataStream = getDataStreamResponse.getDataStreams().get(0).getDataStream();
        assertThat(firstDataStream.getName(), equalTo("metrics-bar"));
        assertThat(firstDataStream.getTimeStampField().getName(), equalTo("@timestamp"));
        assertThat(firstDataStream.getIndices().size(), equalTo(1));
        assertThat(firstDataStream.getIndices().get(0).getName(), equalTo(DataStream.getDefaultBackingIndexName("metrics-bar", 1)));
        DataStream dataStream = getDataStreamResponse.getDataStreams().get(1).getDataStream();
        assertThat(dataStream.getName(), equalTo("metrics-foo"));
        assertThat(dataStream.getTimeStampField().getName(), equalTo("@timestamp"));
        assertThat(dataStream.getIndices().size(), equalTo(1));
        assertThat(dataStream.getIndices().get(0).getName(), equalTo(DataStream.getDefaultBackingIndexName("metrics-foo", 1)));

        String backingIndex = DataStream.getDefaultBackingIndexName("metrics-bar", 1);
        GetIndexResponse getIndexResponse = client().admin().indices().getIndex(new GetIndexRequest().indices(backingIndex)).actionGet();
        assertThat(getIndexResponse.getSettings().get(backingIndex), notNullValue());
        assertThat(getIndexResponse.getSettings().get(backingIndex).getAsBoolean("index.hidden", null), is(true));
        Map<?, ?> mappings = getIndexResponse.getMappings().get(backingIndex).getSourceAsMap();
        assertThat(ObjectPath.eval("properties.@timestamp.type", mappings), is("date"));

        backingIndex = DataStream.getDefaultBackingIndexName("metrics-foo", 1);
        getIndexResponse = client().admin().indices().getIndex(new GetIndexRequest().indices(backingIndex)).actionGet();
        assertThat(getIndexResponse.getSettings().get(backingIndex), notNullValue());
        assertThat(getIndexResponse.getSettings().get(backingIndex).getAsBoolean("index.hidden", null), is(true));
        mappings = getIndexResponse.getMappings().get(backingIndex).getSourceAsMap();
        assertThat(ObjectPath.eval("properties.@timestamp.type", mappings), is("date"));

        int numDocsFoo = randomIntBetween(2, 16);
        indexDocs("metrics-foo", numDocsFoo);

        verifyDocs("metrics-bar", numDocsBar, 1, 1);
        verifyDocs("metrics-foo", numDocsFoo, 1, 1);

        RolloverResponse rolloverResponse = client().admin().indices().rolloverIndex(new RolloverRequest("metrics-foo", null)).get();
        assertThat(rolloverResponse.getNewIndex(), equalTo(DataStream.getDefaultBackingIndexName("metrics-foo", 2)));
        assertTrue(rolloverResponse.isRolledOver());

        rolloverResponse = client().admin().indices().rolloverIndex(new RolloverRequest("metrics-bar", null)).get();
        assertThat(rolloverResponse.getNewIndex(), equalTo(DataStream.getDefaultBackingIndexName("metrics-bar", 2)));
        assertTrue(rolloverResponse.isRolledOver());

        backingIndex = DataStream.getDefaultBackingIndexName("metrics-foo", 2);
        getIndexResponse = client().admin().indices().getIndex(new GetIndexRequest().indices(backingIndex)).actionGet();
        assertThat(getIndexResponse.getSettings().get(backingIndex), notNullValue());
        assertThat(getIndexResponse.getSettings().get(backingIndex).getAsBoolean("index.hidden", null), is(true));
        mappings = getIndexResponse.getMappings().get(backingIndex).getSourceAsMap();
        assertThat(ObjectPath.eval("properties.@timestamp.type", mappings), is("date"));

        backingIndex = DataStream.getDefaultBackingIndexName("metrics-bar", 2);
        getIndexResponse = client().admin().indices().getIndex(new GetIndexRequest().indices(backingIndex)).actionGet();
        assertThat(getIndexResponse.getSettings().get(backingIndex), notNullValue());
        assertThat(getIndexResponse.getSettings().get(backingIndex).getAsBoolean("index.hidden", null), is(true));
        mappings = getIndexResponse.getMappings().get(backingIndex).getSourceAsMap();
        assertThat(ObjectPath.eval("properties.@timestamp.type", mappings), is("date"));

        int numDocsBar2 = randomIntBetween(2, 16);
        indexDocs("metrics-bar", numDocsBar2);
        int numDocsFoo2 = randomIntBetween(2, 16);
        indexDocs("metrics-foo", numDocsFoo2);

        verifyDocs("metrics-bar", numDocsBar + numDocsBar2, 1, 2);
        verifyDocs("metrics-foo", numDocsFoo + numDocsFoo2, 1, 2);

        DeleteDataStreamAction.Request deleteDataStreamRequest = new DeleteDataStreamAction.Request(new String[] { "metrics-*" });
        client().execute(DeleteDataStreamAction.INSTANCE, deleteDataStreamRequest).actionGet();
        getDataStreamResponse = client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest).actionGet();
        assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(0));

        expectThrows(
            IndexNotFoundException.class,
            () -> client().admin()
                .indices()
                .getIndex(new GetIndexRequest().indices(DataStream.getDefaultBackingIndexName("metrics-bar", 1)))
                .actionGet()
        );
        expectThrows(
            IndexNotFoundException.class,
            () -> client().admin()
                .indices()
                .getIndex(new GetIndexRequest().indices(DataStream.getDefaultBackingIndexName("metrics-bar", 2)))
                .actionGet()
        );
        expectThrows(
            IndexNotFoundException.class,
            () -> client().admin()
                .indices()
                .getIndex(new GetIndexRequest().indices(DataStream.getDefaultBackingIndexName("metrics-foo", 1)))
                .actionGet()
        );
        expectThrows(
            IndexNotFoundException.class,
            () -> client().admin()
                .indices()
                .getIndex(new GetIndexRequest().indices(DataStream.getDefaultBackingIndexName("metrics-foo", 2)))
                .actionGet()
        );

         */
    }

    static void indexDocs(String index, int numDocs) {
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < numDocs; i++) {
            String value = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(System.currentTimeMillis());
            bulkRequest.add(
                new IndexRequest(index).opType(DocWriteRequest.OpType.CREATE)
                    .source(String.format(Locale.ROOT, "{\"%s\":\"%s\"}", DEFAULT_TIMESTAMP_FIELD, value), XContentType.JSON)
            );
        }
        BulkResponse bulkResponse = client().bulk(bulkRequest).actionGet();
        assertThat(bulkResponse.getItems().length, equalTo(numDocs));
        for (BulkItemResponse itemResponse : bulkResponse) {
            assertThat(itemResponse.getFailureMessage(), nullValue());
            assertThat(itemResponse.status(), equalTo(RestStatus.CREATED));
            //assertThat(itemResponse.getIndex(), startsWith(index));
        }
        client().admin().indices().refresh(new RefreshRequest(index)).actionGet();
    }

}
