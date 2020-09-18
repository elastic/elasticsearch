/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.datastreams;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.template.delete.DeleteComposableIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryRequestBuilder;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.MultiSearchRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ObjectPath;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.action.CreateDataStreamAction;
import org.elasticsearch.xpack.core.action.DeleteDataStreamAction;
import org.elasticsearch.xpack.core.action.GetDataStreamAction;
import org.elasticsearch.xpack.datastreams.DataStreamsPlugin;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static org.elasticsearch.action.DocWriteRequest.OpType.CREATE;
import static org.elasticsearch.cluster.DataStreamTestHelper.generateMapping;
import static org.elasticsearch.cluster.metadata.MetadataIndexTemplateService.DEFAULT_TIMESTAMP_FIELD;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItemInArray;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class DataStreamIT extends ESIntegTestCase {

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

    public void testBasicScenario() throws Exception {
        putComposableIndexTemplate("id1", List.of("metrics-foo*"));
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request("metrics-foo");
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();

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

        int numDocsBar = randomIntBetween(2, 16);
        indexDocs("metrics-bar", numDocsBar);
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
    }

    public void testOtherWriteOps() throws Exception {
        putComposableIndexTemplate("id", List.of("metrics-foobar*"));
        String dataStreamName = "metrics-foobar";
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(dataStreamName);
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();

        {
            BulkRequest bulkRequest = new BulkRequest().add(
                new IndexRequest(dataStreamName).source("{\"@timestamp1\": \"2020-12-12\"}", XContentType.JSON)
            );
            BulkResponse bulkResponse = client().bulk(bulkRequest).actionGet();
            assertThat(bulkResponse.getItems(), arrayWithSize(1));
            assertThat(
                bulkResponse.getItems()[0].getFailure().getMessage(),
                containsString("only write ops with an op_type of create are allowed in data streams")
            );
        }
        {
            BulkRequest bulkRequest = new BulkRequest().add(new DeleteRequest(dataStreamName, "_id"));
            BulkResponse bulkResponse = client().bulk(bulkRequest).actionGet();
            assertThat(bulkResponse.getItems(), arrayWithSize(1));
            assertThat(
                bulkResponse.getItems()[0].getFailure().getMessage(),
                containsString("only write ops with an op_type of create are allowed in data streams")
            );
        }
        {
            BulkRequest bulkRequest = new BulkRequest().add(
                new UpdateRequest(dataStreamName, "_id").doc("{\"@timestamp1\": \"2020-12-12\"}", XContentType.JSON)
            );
            BulkResponse bulkResponse = client().bulk(bulkRequest).actionGet();
            assertThat(bulkResponse.getItems(), arrayWithSize(1));
            assertThat(
                bulkResponse.getItems()[0].getFailure().getMessage(),
                containsString("only write ops with an op_type of create are allowed in data streams")
            );
        }
        {
            IndexRequest indexRequest = new IndexRequest(dataStreamName).source("{\"@timestamp\": \"2020-12-12\"}", XContentType.JSON);
            Exception e = expectThrows(IllegalArgumentException.class, () -> client().index(indexRequest).actionGet());
            assertThat(e.getMessage(), equalTo("only write ops with an op_type of create are allowed in data streams"));
        }
        {
            UpdateRequest updateRequest = new UpdateRequest(dataStreamName, "_id").doc("{}", XContentType.JSON);
            Exception e = expectThrows(IllegalArgumentException.class, () -> client().update(updateRequest).actionGet());
            assertThat(e.getMessage(), equalTo("only write ops with an op_type of create are allowed in data streams"));
        }
        {
            DeleteRequest deleteRequest = new DeleteRequest(dataStreamName, "_id");
            Exception e = expectThrows(IllegalArgumentException.class, () -> client().delete(deleteRequest).actionGet());
            assertThat(e.getMessage(), equalTo("only write ops with an op_type of create are allowed in data streams"));
        }
        {
            IndexRequest indexRequest = new IndexRequest(dataStreamName).source("{\"@timestamp\": \"2020-12-12\"}", XContentType.JSON)
                .opType(DocWriteRequest.OpType.CREATE);
            IndexResponse indexResponse = client().index(indexRequest).actionGet();
            assertThat(indexResponse.getIndex(), equalTo(DataStream.getDefaultBackingIndexName(dataStreamName, 1)));
        }
        {
            BulkRequest bulkRequest = new BulkRequest().add(
                new IndexRequest(dataStreamName).source("{\"@timestamp\": \"2020-12-12\"}", XContentType.JSON)
                    .opType(DocWriteRequest.OpType.CREATE)
            );
            BulkResponse bulkItemResponses = client().bulk(bulkRequest).actionGet();
            assertThat(bulkItemResponses.getItems()[0].getIndex(), equalTo(DataStream.getDefaultBackingIndexName(dataStreamName, 1)));
        }
    }

    /**
     * The composable template that matches with the data stream name should always be used for backing indices.
     * It is possible that a backing index doesn't match with a template or a different template, but in order
     * to avoid confusion, the template matching with the corresponding data stream name should be used.
     */
    public void testComposableTemplateOnlyMatchingWithDataStreamName() throws Exception {
        String dataStreamName = "logs-foobar";

        String mapping = "{\n"
            + "      \"properties\": {\n"
            + "        \"baz_field\": {\n"
            + "          \"type\": \"keyword\"\n"
            + "        },\n"
            + "        \"@timestamp\": {\n"
            + "          \"type\": \"date\"\n"
            + "        }\n"
            + "      }\n"
            + "    }";
        PutComposableIndexTemplateAction.Request request = new PutComposableIndexTemplateAction.Request("id_1");
        request.indexTemplate(
            new ComposableIndexTemplate(
                List.of(dataStreamName), // use no wildcard, so that backing indices don't match just by name
                new Template(null, new CompressedXContent(mapping), null),
                null,
                null,
                null,
                null,
                new ComposableIndexTemplate.DataStreamTemplate()
            )
        );
        client().execute(PutComposableIndexTemplateAction.INSTANCE, request).actionGet();

        int numDocs = randomIntBetween(2, 16);
        indexDocs(dataStreamName, numDocs);
        verifyDocs(dataStreamName, numDocs, 1, 1);

        String backingIndex = DataStream.getDefaultBackingIndexName(dataStreamName, 1);
        GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(new String[] { "*" });
        GetDataStreamAction.Response getDataStreamResponse = client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest)
            .actionGet();
        assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(1));
        assertThat(getDataStreamResponse.getDataStreams().get(0).getDataStream().getName(), equalTo(dataStreamName));
        assertThat(getDataStreamResponse.getDataStreams().get(0).getDataStream().getTimeStampField().getName(), equalTo("@timestamp"));
        assertThat(getDataStreamResponse.getDataStreams().get(0).getDataStream().getIndices().size(), equalTo(1));
        assertThat(getDataStreamResponse.getDataStreams().get(0).getDataStream().getIndices().get(0).getName(), equalTo(backingIndex));

        GetIndexResponse getIndexResponse = client().admin().indices().getIndex(new GetIndexRequest().indices(dataStreamName)).actionGet();
        assertThat(getIndexResponse.getSettings().get(backingIndex), notNullValue());
        assertThat(getIndexResponse.getSettings().get(backingIndex).getAsBoolean("index.hidden", null), is(true));
        assertThat(
            ObjectPath.eval("properties.baz_field.type", getIndexResponse.mappings().get(backingIndex).getSourceAsMap()),
            equalTo("keyword")
        );

        backingIndex = DataStream.getDefaultBackingIndexName(dataStreamName, 2);
        RolloverResponse rolloverResponse = client().admin().indices().rolloverIndex(new RolloverRequest(dataStreamName, null)).get();
        assertThat(rolloverResponse.getNewIndex(), equalTo(backingIndex));
        assertTrue(rolloverResponse.isRolledOver());

        getIndexResponse = client().admin().indices().getIndex(new GetIndexRequest().indices(backingIndex)).actionGet();
        assertThat(getIndexResponse.getSettings().get(backingIndex), notNullValue());
        assertThat(getIndexResponse.getSettings().get(backingIndex).getAsBoolean("index.hidden", null), is(true));
        assertThat(
            ObjectPath.eval("properties.baz_field.type", getIndexResponse.mappings().get(backingIndex).getSourceAsMap()),
            equalTo("keyword")
        );

        int numDocs2 = randomIntBetween(2, 16);
        indexDocs(dataStreamName, numDocs2);
        verifyDocs(dataStreamName, numDocs + numDocs2, 1, 2);

        DeleteDataStreamAction.Request deleteDataStreamRequest = new DeleteDataStreamAction.Request(new String[] { dataStreamName });
        client().execute(DeleteDataStreamAction.INSTANCE, deleteDataStreamRequest).actionGet();
        getDataStreamResponse = client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest).actionGet();
        assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(0));

        expectThrows(
            IndexNotFoundException.class,
            () -> client().admin()
                .indices()
                .getIndex(new GetIndexRequest().indices(DataStream.getDefaultBackingIndexName(dataStreamName, 1)))
                .actionGet()
        );
        expectThrows(
            IndexNotFoundException.class,
            () -> client().admin()
                .indices()
                .getIndex(new GetIndexRequest().indices(DataStream.getDefaultBackingIndexName(dataStreamName, 2)))
                .actionGet()
        );
    }

    public void testTimeStampValidationInvalidFieldMapping() throws Exception {
        // Adding a template with an invalid mapping for timestamp field and expect template creation to fail.
        String mapping = "{\n"
            + "      \"properties\": {\n"
            + "        \"@timestamp\": {\n"
            + "          \"type\": \"keyword\"\n"
            + "        }\n"
            + "      }\n"
            + "    }";
        PutComposableIndexTemplateAction.Request createTemplateRequest = new PutComposableIndexTemplateAction.Request("logs-foo");
        createTemplateRequest.indexTemplate(
            new ComposableIndexTemplate(
                List.of("logs-*"),
                new Template(null, new CompressedXContent(mapping), null),
                null,
                null,
                null,
                null,
                new ComposableIndexTemplate.DataStreamTemplate()
            )
        );

        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> client().execute(PutComposableIndexTemplateAction.INSTANCE, createTemplateRequest).actionGet()
        );
        assertThat(
            e.getCause().getCause().getMessage(),
            equalTo("data stream timestamp field [@timestamp] is of type [keyword], but [date,date_nanos] is expected")
        );
    }

    public void testResolvabilityOfDataStreamsInAPIs() throws Exception {
        putComposableIndexTemplate("id", List.of("logs-*"));
        String dataStreamName = "logs-foobar";
        CreateDataStreamAction.Request request = new CreateDataStreamAction.Request(dataStreamName);
        client().execute(CreateDataStreamAction.INSTANCE, request).actionGet();

        verifyResolvability(
            dataStreamName,
            client().prepareIndex(dataStreamName)
                .setSource("{\"@timestamp\": \"2020-12-12\"}", XContentType.JSON)
                .setOpType(DocWriteRequest.OpType.CREATE),
            false
        );
        verifyResolvability(dataStreamName, client().admin().indices().prepareRefresh(dataStreamName), false);
        verifyResolvability(dataStreamName, client().prepareSearch(dataStreamName), false, 1);
        verifyResolvability(
            dataStreamName,
            client().prepareMultiSearch().add(client().prepareSearch(dataStreamName).setQuery(matchAllQuery())),
            false
        );
        verifyResolvability(dataStreamName, client().admin().indices().prepareClearCache(dataStreamName), false);
        verifyResolvability(dataStreamName, client().admin().indices().prepareFlush(dataStreamName), false);
        verifyResolvability(dataStreamName, client().admin().indices().prepareSegments(dataStreamName), false);
        verifyResolvability(dataStreamName, client().admin().indices().prepareStats(dataStreamName), false);
        verifyResolvability(dataStreamName, client().admin().indices().prepareForceMerge(dataStreamName), false);
        verifyResolvability(dataStreamName, client().admin().indices().prepareValidateQuery(dataStreamName), false);
        verifyResolvability(dataStreamName, client().admin().indices().prepareUpgrade(dataStreamName), false);
        verifyResolvability(dataStreamName, client().admin().indices().prepareRecoveries(dataStreamName), false);
        verifyResolvability(dataStreamName, client().admin().indices().prepareUpgradeStatus(dataStreamName), false);
        verifyResolvability(dataStreamName, client().admin().indices().prepareGetAliases("dummy").addIndices(dataStreamName), true);
        verifyResolvability(dataStreamName, client().admin().indices().prepareGetFieldMappings(dataStreamName), false);
        verifyResolvability(
            dataStreamName,
            client().admin()
                .indices()
                .preparePutMapping(dataStreamName)
                .setSource("{\"_doc\":{\"properties\": {\"my_field\":{\"type\":\"keyword\"}}}}", XContentType.JSON),
            false
        );
        verifyResolvability(dataStreamName, client().admin().indices().prepareGetMappings(dataStreamName), false);
        verifyResolvability(
            dataStreamName,
            client().admin()
                .indices()
                .prepareUpdateSettings(dataStreamName)
                .setSettings(Settings.builder().put("index.number_of_replicas", 0)),
            false
        );
        verifyResolvability(dataStreamName, client().admin().indices().prepareGetSettings(dataStreamName), false);
        verifyResolvability(dataStreamName, client().admin().cluster().prepareHealth(dataStreamName), false);
        verifyResolvability(dataStreamName, client().admin().cluster().prepareState().setIndices(dataStreamName), false);
        verifyResolvability(dataStreamName, client().prepareFieldCaps(dataStreamName).setFields("*"), false);
        verifyResolvability(dataStreamName, client().admin().indices().prepareGetIndex().addIndices(dataStreamName), false);
        verifyResolvability(dataStreamName, client().admin().indices().prepareOpen(dataStreamName), false);
        verifyResolvability(dataStreamName, client().admin().cluster().prepareSearchShards(dataStreamName), false);
        verifyResolvability(dataStreamName, client().admin().indices().prepareShardStores(dataStreamName), false);

        request = new CreateDataStreamAction.Request("logs-barbaz");
        client().execute(CreateDataStreamAction.INSTANCE, request).actionGet();
        verifyResolvability(
            "logs-barbaz",
            client().prepareIndex("logs-barbaz")
                .setSource("{\"@timestamp\": \"2020-12-12\"}", XContentType.JSON)
                .setOpType(DocWriteRequest.OpType.CREATE),
            false
        );

        String wildcardExpression = "logs*";
        verifyResolvability(wildcardExpression, client().admin().indices().prepareRefresh(wildcardExpression), false);
        verifyResolvability(wildcardExpression, client().prepareSearch(wildcardExpression), false, 2);
        verifyResolvability(
            wildcardExpression,
            client().prepareMultiSearch().add(client().prepareSearch(wildcardExpression).setQuery(matchAllQuery())),
            false
        );
        verifyResolvability(wildcardExpression, client().admin().indices().prepareClearCache(wildcardExpression), false);
        verifyResolvability(wildcardExpression, client().admin().indices().prepareFlush(wildcardExpression), false);
        verifyResolvability(wildcardExpression, client().admin().indices().prepareSegments(wildcardExpression), false);
        verifyResolvability(wildcardExpression, client().admin().indices().prepareStats(wildcardExpression), false);
        verifyResolvability(wildcardExpression, client().admin().indices().prepareForceMerge(wildcardExpression), false);
        verifyResolvability(wildcardExpression, client().admin().indices().prepareValidateQuery(wildcardExpression), false);
        verifyResolvability(wildcardExpression, client().admin().indices().prepareUpgrade(wildcardExpression), false);
        verifyResolvability(wildcardExpression, client().admin().indices().prepareRecoveries(wildcardExpression), false);
        verifyResolvability(wildcardExpression, client().admin().indices().prepareUpgradeStatus(wildcardExpression), false);
        verifyResolvability(wildcardExpression, client().admin().indices().prepareGetAliases(wildcardExpression), false);
        verifyResolvability(wildcardExpression, client().admin().indices().prepareGetFieldMappings(wildcardExpression), false);
        verifyResolvability(
            wildcardExpression,
            client().admin()
                .indices()
                .preparePutMapping(wildcardExpression)
                .setSource("{\"_doc\":{\"properties\": {\"my_field\":{\"type\":\"keyword\"}}}}", XContentType.JSON),
            false
        );
        verifyResolvability(wildcardExpression, client().admin().indices().prepareGetMappings(wildcardExpression), false);
        verifyResolvability(wildcardExpression, client().admin().indices().prepareGetSettings(wildcardExpression), false);
        verifyResolvability(
            wildcardExpression,
            client().admin()
                .indices()
                .prepareUpdateSettings(wildcardExpression)
                .setSettings(Settings.builder().put("index.number_of_replicas", 0)),
            false
        );
        verifyResolvability(wildcardExpression, client().admin().cluster().prepareHealth(wildcardExpression), false);
        verifyResolvability(wildcardExpression, client().admin().cluster().prepareState().setIndices(wildcardExpression), false);
        verifyResolvability(wildcardExpression, client().prepareFieldCaps(wildcardExpression).setFields("*"), false);
        verifyResolvability(wildcardExpression, client().admin().indices().prepareGetIndex().addIndices(wildcardExpression), false);
        verifyResolvability(wildcardExpression, client().admin().indices().prepareOpen(wildcardExpression), false);
        verifyResolvability(wildcardExpression, client().admin().cluster().prepareSearchShards(wildcardExpression), false);
        verifyResolvability(wildcardExpression, client().admin().indices().prepareShardStores(wildcardExpression), false);
    }

    public void testCannotDeleteComposableTemplateUsedByDataStream() throws Exception {
        putComposableIndexTemplate("id", List.of("metrics-foobar*"));
        String dataStreamName = "metrics-foobar-baz";
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(dataStreamName);
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();
        createDataStreamRequest = new CreateDataStreamAction.Request(dataStreamName + "-eggplant");
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();

        DeleteComposableIndexTemplateAction.Request req = new DeleteComposableIndexTemplateAction.Request("id");
        Exception e = expectThrows(Exception.class, () -> client().execute(DeleteComposableIndexTemplateAction.INSTANCE, req).get());
        Optional<Exception> maybeE = ExceptionsHelper.unwrapCausesAndSuppressed(
            e,
            err -> err.getMessage()
                .contains(
                    "unable to remove composable templates [id] "
                        + "as they are in use by a data streams [metrics-foobar-baz, metrics-foobar-baz-eggplant]"
                )
        );
        assertTrue(maybeE.isPresent());

        DeleteComposableIndexTemplateAction.Request req2 = new DeleteComposableIndexTemplateAction.Request("i*");
        Exception e2 = expectThrows(Exception.class, () -> client().execute(DeleteComposableIndexTemplateAction.INSTANCE, req2).get());
        maybeE = ExceptionsHelper.unwrapCausesAndSuppressed(
            e2,
            err -> err.getMessage()
                .contains(
                    "unable to remove composable templates [id] "
                        + "as they are in use by a data streams [metrics-foobar-baz, metrics-foobar-baz-eggplant]"
                )
        );
        assertTrue(maybeE.isPresent());
    }

    public void testAliasActionsFailOnDataStreams() throws Exception {
        putComposableIndexTemplate("id1", List.of("metrics-foo*"));
        String dataStreamName = "metrics-foo";
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(dataStreamName);
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();

        IndicesAliasesRequest.AliasActions addAction = new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD)
            .index(dataStreamName)
            .aliases("foo");
        IndicesAliasesRequest aliasesAddRequest = new IndicesAliasesRequest();
        aliasesAddRequest.addAliasAction(addAction);
        Exception e = expectThrows(IndexNotFoundException.class, () -> client().admin().indices().aliases(aliasesAddRequest).actionGet());
        assertThat(e.getMessage(), equalTo("no such index [" + dataStreamName + "]"));
    }

    public void testAliasActionsFailOnDataStreamBackingIndices() throws Exception {
        putComposableIndexTemplate("id1", List.of("metrics-foo*"));
        String dataStreamName = "metrics-foo";
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(dataStreamName);
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();

        String backingIndex = DataStream.getDefaultBackingIndexName(dataStreamName, 1);
        IndicesAliasesRequest.AliasActions addAction = new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD)
            .index(backingIndex)
            .aliases("first_gen");
        IndicesAliasesRequest aliasesAddRequest = new IndicesAliasesRequest();
        aliasesAddRequest.addAliasAction(addAction);
        Exception e = expectThrows(IllegalArgumentException.class, () -> client().admin().indices().aliases(aliasesAddRequest).actionGet());
        assertThat(
            e.getMessage(),
            equalTo(
                "The provided expressions ["
                    + backingIndex
                    + "] match a backing index belonging to data stream ["
                    + dataStreamName
                    + "]. Data streams and their backing indices don't "
                    + "support aliases."
            )
        );
    }

    public void testTimestampFieldCustomAttributes() throws Exception {
        String mapping = "{\n"
            + "      \"properties\": {\n"
            + "        \"@timestamp\": {\n"
            + "          \"type\": \"date\",\n"
            + "          \"format\": \"yyyy-MM\",\n"
            + "          \"meta\": {\n"
            + "            \"x\": \"y\"\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    }";
        putComposableIndexTemplate("id1", mapping, List.of("logs-foo*"), null);

        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request("logs-foobar");
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();
        GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(new String[] { "logs-foobar" });
        GetDataStreamAction.Response getDataStreamResponse = client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest)
            .actionGet();
        assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(1));
        assertThat(getDataStreamResponse.getDataStreams().get(0).getDataStream().getName(), equalTo("logs-foobar"));
        assertThat(getDataStreamResponse.getDataStreams().get(0).getDataStream().getTimeStampField().getName(), equalTo("@timestamp"));
        Map<?, ?> expectedTimestampMapping = Map.of("type", "date", "format", "yyyy-MM", "meta", Map.of("x", "y"));
        assertBackingIndex(DataStream.getDefaultBackingIndexName("logs-foobar", 1), "properties.@timestamp", expectedTimestampMapping);
    }

    public void testUpdateMappingViaDataStream() throws Exception {
        putComposableIndexTemplate("id1", List.of("logs-*"));
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request("logs-foobar");
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).actionGet();

        String backingIndex1 = DataStream.getDefaultBackingIndexName("logs-foobar", 1);
        String backingIndex2 = DataStream.getDefaultBackingIndexName("logs-foobar", 2);

        RolloverResponse rolloverResponse = client().admin().indices().rolloverIndex(new RolloverRequest("logs-foobar", null)).get();
        assertThat(rolloverResponse.getNewIndex(), equalTo(backingIndex2));
        assertTrue(rolloverResponse.isRolledOver());

        Map<?, ?> expectedMapping = Map.of(
            "properties",
            Map.of("@timestamp", Map.of("type", "date")),
            "_data_stream_timestamp",
            Map.of("enabled", true)
        );
        GetMappingsResponse getMappingsResponse = client().admin().indices().prepareGetMappings("logs-foobar").get();
        assertThat(getMappingsResponse.getMappings().size(), equalTo(2));
        assertThat(getMappingsResponse.getMappings().get(backingIndex1).getSourceAsMap(), equalTo(expectedMapping));
        assertThat(getMappingsResponse.getMappings().get(backingIndex2).getSourceAsMap(), equalTo(expectedMapping));

        expectedMapping = Map.of(
            "properties",
            Map.of("@timestamp", Map.of("type", "date"), "my_field", Map.of("type", "keyword")),
            "_data_stream_timestamp",
            Map.of("enabled", true)
        );
        client().admin()
            .indices()
            .preparePutMapping("logs-foobar")
            .setSource("{\"properties\":{\"my_field\":{\"type\":\"keyword\"}}}", XContentType.JSON)
            .get();
        // The mappings of all backing indices should be updated:
        getMappingsResponse = client().admin().indices().prepareGetMappings("logs-foobar").get();
        assertThat(getMappingsResponse.getMappings().size(), equalTo(2));
        assertThat(getMappingsResponse.getMappings().get(backingIndex1).getSourceAsMap(), equalTo(expectedMapping));
        assertThat(getMappingsResponse.getMappings().get(backingIndex2).getSourceAsMap(), equalTo(expectedMapping));
    }

    public void testUpdateIndexSettingsViaDataStream() throws Exception {
        putComposableIndexTemplate("id1", List.of("logs-*"));
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request("logs-foobar");
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).actionGet();

        String backingIndex1 = DataStream.getDefaultBackingIndexName("logs-foobar", 1);
        String backingIndex2 = DataStream.getDefaultBackingIndexName("logs-foobar", 2);

        RolloverResponse rolloverResponse = client().admin().indices().rolloverIndex(new RolloverRequest("logs-foobar", null)).get();
        assertThat(rolloverResponse.getNewIndex(), equalTo(backingIndex2));
        assertTrue(rolloverResponse.isRolledOver());

        // The index settings of all backing indices should be updated:
        GetSettingsResponse getSettingsResponse = client().admin().indices().prepareGetSettings("logs-foobar").get();
        assertThat(getSettingsResponse.getIndexToSettings().size(), equalTo(2));
        assertThat(getSettingsResponse.getSetting(backingIndex1, "index.number_of_replicas"), equalTo("1"));
        assertThat(getSettingsResponse.getSetting(backingIndex2, "index.number_of_replicas"), equalTo("1"));

        client().admin()
            .indices()
            .prepareUpdateSettings("logs-foobar")
            .setSettings(Settings.builder().put("index.number_of_replicas", 0))
            .get();
        getSettingsResponse = client().admin().indices().prepareGetSettings("logs-foobar").get();
        assertThat(getSettingsResponse.getIndexToSettings().size(), equalTo(2));
        assertThat(getSettingsResponse.getSetting(backingIndex1, "index.number_of_replicas"), equalTo("0"));
        assertThat(getSettingsResponse.getSetting(backingIndex2, "index.number_of_replicas"), equalTo("0"));
    }

    public void testIndexDocsWithCustomRoutingTargetingDataStreamIsNotAllowed() throws Exception {
        putComposableIndexTemplate("id1", List.of("logs-foo*"));

        // Index doc that triggers creation of a data stream
        String dataStream = "logs-foobar";
        IndexRequest indexRequest = new IndexRequest(dataStream).source("{\"@timestamp\": \"2020-12-12\"}", XContentType.JSON)
            .opType(DocWriteRequest.OpType.CREATE);
        IndexResponse indexResponse = client().index(indexRequest).actionGet();
        assertThat(indexResponse.getIndex(), equalTo(DataStream.getDefaultBackingIndexName(dataStream, 1)));

        // Index doc with custom routing that targets the data stream
        IndexRequest indexRequestWithRouting = new IndexRequest(dataStream).source("@timestamp", System.currentTimeMillis())
            .opType(DocWriteRequest.OpType.CREATE)
            .routing("custom");
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> client().index(indexRequestWithRouting).actionGet()
        );
        assertThat(
            exception.getMessage(),
            is(
                "index request targeting data stream [logs-foobar] specifies a custom routing. target the "
                    + "backing indices directly or remove the custom routing."
            )
        );

        // Bulk indexing with custom routing targeting the data stream is also prohibited
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < 10; i++) {
            bulkRequest.add(
                new IndexRequest(dataStream).opType(DocWriteRequest.OpType.CREATE)
                    .routing("bulk-request-routing")
                    .source("{}", XContentType.JSON)
            );
        }

        BulkResponse bulkResponse = client().bulk(bulkRequest).actionGet();
        for (BulkItemResponse responseItem : bulkResponse.getItems()) {
            assertThat(responseItem.getFailure(), notNullValue());
            assertThat(
                responseItem.getFailureMessage(),
                is(
                    "java.lang.IllegalArgumentException: index request targeting data stream "
                        + "[logs-foobar] specifies a custom routing. target the backing indices directly or remove the custom routing."
                )
            );
        }
    }

    public void testIndexDocsWithCustomRoutingTargetingBackingIndex() throws Exception {
        putComposableIndexTemplate("id1", List.of("logs-foo*"));

        // Index doc that triggers creation of a data stream
        IndexRequest indexRequest = new IndexRequest("logs-foobar").source("{\"@timestamp\": \"2020-12-12\"}", XContentType.JSON)
            .opType(DocWriteRequest.OpType.CREATE);
        IndexResponse indexResponse = client().index(indexRequest).actionGet();
        assertThat(indexResponse.getIndex(), equalTo(DataStream.getDefaultBackingIndexName("logs-foobar", 1)));

        // Index doc with custom routing that targets the backing index
        IndexRequest indexRequestWithRouting = new IndexRequest(DataStream.getDefaultBackingIndexName("logs-foobar", 1L)).source(
            "@timestamp",
            System.currentTimeMillis()
        )
            .opType(DocWriteRequest.OpType.INDEX)
            .routing("custom")
            .id(indexResponse.getId())
            .setIfPrimaryTerm(indexResponse.getPrimaryTerm())
            .setIfSeqNo(indexResponse.getSeqNo());
        IndexResponse response = client().index(indexRequestWithRouting).actionGet();
        assertThat(response.getIndex(), equalTo(DataStream.getDefaultBackingIndexName("logs-foobar", 1)));
    }

    public void testSearchAllResolvesDataStreams() throws Exception {
        putComposableIndexTemplate("id1", List.of("metrics-foo*"));
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request("metrics-foo");
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();

        putComposableIndexTemplate("id2", List.of("metrics-bar*"));
        createDataStreamRequest = new CreateDataStreamAction.Request("metrics-bar");
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();

        int numDocsBar = randomIntBetween(2, 16);
        indexDocs("metrics-bar", numDocsBar);
        int numDocsFoo = randomIntBetween(2, 16);
        indexDocs("metrics-foo", numDocsFoo);

        RolloverResponse rolloverResponse = client().admin().indices().rolloverIndex(new RolloverRequest("metrics-foo", null)).get();
        assertThat(rolloverResponse.getNewIndex(), equalTo(DataStream.getDefaultBackingIndexName("metrics-foo", 2)));

        // ingest some more data in the rolled data stream
        int numDocsRolledFoo = randomIntBetween(2, 16);
        indexDocs("metrics-foo", numDocsRolledFoo);

        SearchRequest searchRequest = new SearchRequest("*");
        SearchResponse searchResponse = client().search(searchRequest).actionGet();
        assertThat(searchResponse.getHits().getTotalHits().value, is((long) numDocsBar + numDocsFoo + numDocsRolledFoo));
    }

    public void testGetDataStream() throws Exception {
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, maximumNumberOfReplicas() + 2).build();
        putComposableIndexTemplate("template_for_foo", null, List.of("metrics-foo*"), settings);

        int numDocsFoo = randomIntBetween(2, 16);
        indexDocs("metrics-foo", numDocsFoo);

        GetDataStreamAction.Response response = client().execute(
            GetDataStreamAction.INSTANCE,
            new GetDataStreamAction.Request(new String[] { "metrics-foo" })
        ).actionGet();
        assertThat(response.getDataStreams().size(), is(1));
        GetDataStreamAction.Response.DataStreamInfo metricsFooDataStream = response.getDataStreams().get(0);
        assertThat(metricsFooDataStream.getDataStream().getName(), is("metrics-foo"));
        assertThat(metricsFooDataStream.getDataStreamStatus(), is(ClusterHealthStatus.YELLOW));
        assertThat(metricsFooDataStream.getIndexTemplate(), is("template_for_foo"));
        assertThat(metricsFooDataStream.getIlmPolicy(), is(nullValue()));
    }

    private static void assertBackingIndex(String backingIndex, String timestampFieldPathInMapping) {
        assertBackingIndex(backingIndex, timestampFieldPathInMapping, Map.of("type", "date"));
    }

    private static void assertBackingIndex(String backingIndex, String timestampFieldPathInMapping, Map<?, ?> expectedMapping) {
        GetIndexResponse getIndexResponse = client().admin().indices().getIndex(new GetIndexRequest().indices(backingIndex)).actionGet();
        assertThat(getIndexResponse.getSettings().get(backingIndex), notNullValue());
        assertThat(getIndexResponse.getSettings().get(backingIndex).getAsBoolean("index.hidden", null), is(true));
        Map<?, ?> mappings = getIndexResponse.getMappings().get(backingIndex).getSourceAsMap();
        assertThat(ObjectPath.eval(timestampFieldPathInMapping, mappings), is(expectedMapping));
    }

    public void testNoTimestampInDocument() throws Exception {
        putComposableIndexTemplate("id", List.of("logs-foobar*"));
        String dataStreamName = "logs-foobar";
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(dataStreamName);
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();

        IndexRequest indexRequest = new IndexRequest(dataStreamName).opType("create").source("{}", XContentType.JSON);
        Exception e = expectThrows(MapperParsingException.class, () -> client().index(indexRequest).actionGet());
        assertThat(e.getCause().getMessage(), equalTo("data stream timestamp field [@timestamp] is missing"));
    }

    public void testMultipleTimestampValuesInDocument() throws Exception {
        putComposableIndexTemplate("id", List.of("logs-foobar*"));
        String dataStreamName = "logs-foobar";
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(dataStreamName);
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();

        IndexRequest indexRequest = new IndexRequest(dataStreamName).opType("create")
            .source("{\"@timestamp\": [\"2020-12-12\",\"2022-12-12\"]}", XContentType.JSON);
        Exception e = expectThrows(MapperParsingException.class, () -> client().index(indexRequest).actionGet());
        assertThat(e.getCause().getMessage(), equalTo("data stream timestamp field [@timestamp] encountered multiple values"));
    }

    public void testMixedAutoCreate() throws Exception {
        PutComposableIndexTemplateAction.Request createTemplateRequest = new PutComposableIndexTemplateAction.Request("logs-foo");
        createTemplateRequest.indexTemplate(
            new ComposableIndexTemplate(
                List.of("logs-foo*"),
                new Template(null, new CompressedXContent(generateMapping("@timestamp")), null),
                null,
                null,
                null,
                null,
                new ComposableIndexTemplate.DataStreamTemplate()
            )
        );
        client().execute(PutComposableIndexTemplateAction.INSTANCE, createTemplateRequest).actionGet();

        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest("logs-foobar").opType(CREATE).source("{\"@timestamp\": \"2020-12-12\"}", XContentType.JSON));
        bulkRequest.add(new IndexRequest("logs-foobaz").opType(CREATE).source("{\"@timestamp\": \"2020-12-12\"}", XContentType.JSON));
        bulkRequest.add(new IndexRequest("logs-barbaz").opType(CREATE).source("{\"@timestamp\": \"2020-12-12\"}", XContentType.JSON));
        bulkRequest.add(new IndexRequest("logs-barfoo").opType(CREATE).source("{\"@timestamp\": \"2020-12-12\"}", XContentType.JSON));
        BulkResponse bulkResponse = client().bulk(bulkRequest).actionGet();
        assertThat("bulk failures: " + Strings.toString(bulkResponse), bulkResponse.hasFailures(), is(false));

        bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest("logs-foobar").opType(CREATE).source("{\"@timestamp\": \"2020-12-12\"}", XContentType.JSON));
        bulkRequest.add(new IndexRequest("logs-foobaz2").opType(CREATE).source("{\"@timestamp\": \"2020-12-12\"}", XContentType.JSON));
        bulkRequest.add(new IndexRequest("logs-barbaz").opType(CREATE).source("{\"@timestamp\": \"2020-12-12\"}", XContentType.JSON));
        bulkRequest.add(new IndexRequest("logs-barfoo2").opType(CREATE).source("{\"@timestamp\": \"2020-12-12\"}", XContentType.JSON));
        bulkResponse = client().bulk(bulkRequest).actionGet();
        assertThat("bulk failures: " + Strings.toString(bulkResponse), bulkResponse.hasFailures(), is(false));

        bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest("logs-foobar").opType(CREATE).source("{\"@timestamp\": \"2020-12-12\"}", XContentType.JSON));
        bulkRequest.add(new IndexRequest("logs-foobaz2").opType(CREATE).source("{\"@timestamp\": \"2020-12-12\"}", XContentType.JSON));
        bulkRequest.add(new IndexRequest("logs-foobaz3").opType(CREATE).source("{\"@timestamp\": \"2020-12-12\"}", XContentType.JSON));
        bulkRequest.add(new IndexRequest("logs-barbaz").opType(CREATE).source("{\"@timestamp\": \"2020-12-12\"}", XContentType.JSON));
        bulkRequest.add(new IndexRequest("logs-barfoo2").opType(CREATE).source("{\"@timestamp\": \"2020-12-12\"}", XContentType.JSON));
        bulkRequest.add(new IndexRequest("logs-barfoo3").opType(CREATE).source("{\"@timestamp\": \"2020-12-12\"}", XContentType.JSON));
        bulkResponse = client().bulk(bulkRequest).actionGet();
        assertThat("bulk failures: " + Strings.toString(bulkResponse), bulkResponse.hasFailures(), is(false));

        GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(new String[] { "*" });
        GetDataStreamAction.Response getDataStreamsResponse = client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest)
            .actionGet();
        assertThat(getDataStreamsResponse.getDataStreams(), hasSize(4));
        getDataStreamsResponse.getDataStreams().sort(Comparator.comparing(dataStreamInfo -> dataStreamInfo.getDataStream().getName()));
        assertThat(getDataStreamsResponse.getDataStreams().get(0).getDataStream().getName(), equalTo("logs-foobar"));
        assertThat(getDataStreamsResponse.getDataStreams().get(1).getDataStream().getName(), equalTo("logs-foobaz"));
        assertThat(getDataStreamsResponse.getDataStreams().get(2).getDataStream().getName(), equalTo("logs-foobaz2"));
        assertThat(getDataStreamsResponse.getDataStreams().get(3).getDataStream().getName(), equalTo("logs-foobaz3"));

        GetIndexResponse getIndexResponse = client().admin().indices().getIndex(new GetIndexRequest().indices("logs-bar*")).actionGet();
        assertThat(getIndexResponse.getIndices(), arrayWithSize(4));
        assertThat(getIndexResponse.getIndices(), hasItemInArray("logs-barbaz"));
        assertThat(getIndexResponse.getIndices(), hasItemInArray("logs-barfoo"));
        assertThat(getIndexResponse.getIndices(), hasItemInArray("logs-barfoo2"));
        assertThat(getIndexResponse.getIndices(), hasItemInArray("logs-barfoo3"));

        DeleteDataStreamAction.Request deleteDSReq = new DeleteDataStreamAction.Request(new String[] { "*" });
        client().execute(DeleteDataStreamAction.INSTANCE, deleteDSReq).actionGet();
        DeleteComposableIndexTemplateAction.Request deleteTemplateRequest = new DeleteComposableIndexTemplateAction.Request("*");
        client().execute(DeleteComposableIndexTemplateAction.INSTANCE, deleteTemplateRequest).actionGet();
    }

    public void testAutoCreateV1TemplateNoDataStream() {
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build();

        PutIndexTemplateRequest v1Request = new PutIndexTemplateRequest("logs-foo");
        v1Request.patterns(List.of("logs-foo*"));
        v1Request.settings(settings);
        v1Request.order(Integer.MAX_VALUE); // in order to avoid number_of_replicas being overwritten by random_template
        client().admin().indices().putTemplate(v1Request).actionGet();

        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest("logs-foobar").opType(CREATE).source("{}", XContentType.JSON));
        BulkResponse bulkResponse = client().bulk(bulkRequest).actionGet();
        assertThat("bulk failures: " + Strings.toString(bulkResponse), bulkResponse.hasFailures(), is(false));

        GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(new String[] { "*" });
        GetDataStreamAction.Response getDataStreamsResponse = client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest)
            .actionGet();
        assertThat(getDataStreamsResponse.getDataStreams(), hasSize(0));

        GetIndexResponse getIndexResponse = client().admin().indices().getIndex(new GetIndexRequest().indices("logs-foobar")).actionGet();
        assertThat(getIndexResponse.getIndices(), arrayWithSize(1));
        assertThat(getIndexResponse.getIndices(), hasItemInArray("logs-foobar"));
        assertThat(getIndexResponse.getSettings().get("logs-foobar").get(IndexMetadata.SETTING_NUMBER_OF_REPLICAS), equalTo("0"));
    }

    private static void verifyResolvability(String dataStream, ActionRequestBuilder<?, ?> requestBuilder, boolean fail) {
        verifyResolvability(dataStream, requestBuilder, fail, 0);
    }

    private static void verifyResolvability(
        String dataStream,
        ActionRequestBuilder<?, ?> requestBuilder,
        boolean fail,
        long expectedCount
    ) {
        if (fail) {
            String expectedErrorMessage = "no such index [" + dataStream + "]";
            if (requestBuilder instanceof MultiSearchRequestBuilder) {
                MultiSearchResponse multiSearchResponse = ((MultiSearchRequestBuilder) requestBuilder).get();
                assertThat(multiSearchResponse.getResponses().length, equalTo(1));
                assertThat(multiSearchResponse.getResponses()[0].isFailure(), is(true));
                assertThat(multiSearchResponse.getResponses()[0].getFailure(), instanceOf(IllegalArgumentException.class));
                assertThat(multiSearchResponse.getResponses()[0].getFailure().getMessage(), equalTo(expectedErrorMessage));
            } else if (requestBuilder instanceof ValidateQueryRequestBuilder) {
                Exception e = expectThrows(IndexNotFoundException.class, requestBuilder::get);
                assertThat(e.getMessage(), equalTo(expectedErrorMessage));
            } else {
                Exception e = expectThrows(IndexNotFoundException.class, requestBuilder::get);
                assertThat(e.getMessage(), equalTo(expectedErrorMessage));
            }
        } else {
            if (requestBuilder instanceof SearchRequestBuilder) {
                SearchRequestBuilder searchRequestBuilder = (SearchRequestBuilder) requestBuilder;
                assertHitCount(searchRequestBuilder.get(), expectedCount);
            } else if (requestBuilder instanceof MultiSearchRequestBuilder) {
                MultiSearchResponse multiSearchResponse = ((MultiSearchRequestBuilder) requestBuilder).get();
                assertThat(multiSearchResponse.getResponses()[0].isFailure(), is(false));
            } else {
                requestBuilder.get();
            }
        }
    }

    private static void indexDocs(String dataStream, int numDocs) {
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
        String backingIndex = DataStream.getDefaultBackingIndexName(dataStream, 1);
        String backingIndexPrefix = backingIndex.substring(0, backingIndex.length() - 3);
        for (BulkItemResponse itemResponse : bulkResponse) {
            assertThat(itemResponse.getFailureMessage(), nullValue());
            assertThat(itemResponse.status(), equalTo(RestStatus.CREATED));
            assertThat(itemResponse.getIndex(), startsWith(backingIndexPrefix));
        }
        client().admin().indices().refresh(new RefreshRequest(dataStream)).actionGet();
    }

    private static void verifyDocs(String dataStream, long expectedNumHits, long minGeneration, long maxGeneration) {
        SearchRequest searchRequest = new SearchRequest(dataStream);
        searchRequest.source().size((int) expectedNumHits);
        SearchResponse searchResponse = client().search(searchRequest).actionGet();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(expectedNumHits));

        List<String> expectedIndices = new ArrayList<>();
        for (long k = minGeneration; k <= maxGeneration; k++) {
            expectedIndices.add(DataStream.getDefaultBackingIndexName(dataStream, k));
        }
        Arrays.stream(searchResponse.getHits().getHits()).forEach(hit -> { assertTrue(expectedIndices.contains(hit.getIndex())); });
    }

    public static void putComposableIndexTemplate(String id, List<String> patterns) throws IOException {
        putComposableIndexTemplate(id, null, patterns, null);
    }

    static void putComposableIndexTemplate(String id, @Nullable String mappings, List<String> patterns, @Nullable Settings settings)
        throws IOException {
        PutComposableIndexTemplateAction.Request request = new PutComposableIndexTemplateAction.Request(id);
        request.indexTemplate(
            new ComposableIndexTemplate(
                patterns,
                new Template(settings, mappings == null ? null : new CompressedXContent(mappings), null),
                null,
                null,
                null,
                null,
                new ComposableIndexTemplate.DataStreamTemplate()
            )
        );
        client().execute(PutComposableIndexTemplateAction.INSTANCE, request).actionGet();
    }

}
