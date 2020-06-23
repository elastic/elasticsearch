/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.indices;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.datastream.CreateDataStreamAction;
import org.elasticsearch.action.admin.indices.datastream.DeleteDataStreamAction;
import org.elasticsearch.action.admin.indices.datastream.GetDataStreamAction;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverResponse;
import org.elasticsearch.action.admin.indices.template.delete.DeleteComposableIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryRequestBuilder;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryResponse;
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
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.MetadataCreateDataStreamServiceTests;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.ObjectPath;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Map;

import static org.elasticsearch.indices.IndicesOptionsIntegrationIT._flush;
import static org.elasticsearch.indices.IndicesOptionsIntegrationIT.clearCache;
import static org.elasticsearch.indices.IndicesOptionsIntegrationIT.getAliases;
import static org.elasticsearch.indices.IndicesOptionsIntegrationIT.getFieldMapping;
import static org.elasticsearch.indices.IndicesOptionsIntegrationIT.getMapping;
import static org.elasticsearch.indices.IndicesOptionsIntegrationIT.getSettings;
import static org.elasticsearch.indices.IndicesOptionsIntegrationIT.health;
import static org.elasticsearch.indices.IndicesOptionsIntegrationIT.indicesStats;
import static org.elasticsearch.indices.IndicesOptionsIntegrationIT.msearch;
import static org.elasticsearch.indices.IndicesOptionsIntegrationIT.refreshBuilder;
import static org.elasticsearch.indices.IndicesOptionsIntegrationIT.search;
import static org.elasticsearch.indices.IndicesOptionsIntegrationIT.segments;
import static org.elasticsearch.indices.IndicesOptionsIntegrationIT.validateQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class DataStreamIT extends ESIntegTestCase {

    @After
    public void deleteAllComposableTemplates() {
        DeleteDataStreamAction.Request deleteDSRequest = new DeleteDataStreamAction.Request("*");
        client().execute(DeleteDataStreamAction.INSTANCE, deleteDSRequest).actionGet();
        DeleteComposableIndexTemplateAction.Request deleteTemplateRequest = new DeleteComposableIndexTemplateAction.Request("*");
        client().execute(DeleteComposableIndexTemplateAction.INSTANCE, deleteTemplateRequest).actionGet();
    }

    public void testBasicScenario() throws Exception {
        putComposableIndexTemplate("id1", "@timestamp1", List.of("metrics-foo*"));
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request("metrics-foo");
        client().admin().indices().createDataStream(createDataStreamRequest).get();

        putComposableIndexTemplate("id2", "@timestamp2", List.of("metrics-bar*"));
        createDataStreamRequest = new CreateDataStreamAction.Request("metrics-bar");
        client().admin().indices().createDataStream(createDataStreamRequest).get();

        GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request("*");
        GetDataStreamAction.Response getDataStreamResponse = client().admin().indices().getDataStreams(getDataStreamRequest).actionGet();
        getDataStreamResponse.getDataStreams().sort(Comparator.comparing(DataStream::getName));
        assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(2));
        assertThat(getDataStreamResponse.getDataStreams().get(0).getName(), equalTo("metrics-bar"));
        assertThat(getDataStreamResponse.getDataStreams().get(0).getTimeStampField().getName(), equalTo("@timestamp2"));
        assertThat(getDataStreamResponse.getDataStreams().get(0).getTimeStampField().getFieldMapping(), equalTo(Map.of("type", "date")));
        assertThat(getDataStreamResponse.getDataStreams().get(0).getIndices().size(), equalTo(1));
        assertThat(getDataStreamResponse.getDataStreams().get(0).getIndices().get(0).getName(),
            equalTo(DataStream.getDefaultBackingIndexName("metrics-bar", 1)));
        assertThat(getDataStreamResponse.getDataStreams().get(1).getName(), equalTo("metrics-foo"));
        assertThat(getDataStreamResponse.getDataStreams().get(1).getTimeStampField().getName(), equalTo("@timestamp1"));
        assertThat(getDataStreamResponse.getDataStreams().get(1).getTimeStampField().getFieldMapping(), equalTo(Map.of("type", "date")));
        assertThat(getDataStreamResponse.getDataStreams().get(1).getIndices().size(), equalTo(1));
        assertThat(getDataStreamResponse.getDataStreams().get(1).getIndices().get(0).getName(),
            equalTo(DataStream.getDefaultBackingIndexName("metrics-foo", 1)));

        String backingIndex = DataStream.getDefaultBackingIndexName("metrics-bar", 1);
        GetIndexResponse getIndexResponse =
            client().admin().indices().getIndex(new GetIndexRequest().indices(backingIndex)).actionGet();
        assertThat(getIndexResponse.getSettings().get(backingIndex), notNullValue());
        assertThat(getIndexResponse.getSettings().get(backingIndex).getAsBoolean("index.hidden", null), is(true));
        Map<?, ?> mappings = getIndexResponse.getMappings().get(backingIndex).getSourceAsMap();
        assertThat(ObjectPath.eval("properties.@timestamp2.type", mappings), is("date"));

        backingIndex = DataStream.getDefaultBackingIndexName("metrics-foo", 1);
        getIndexResponse = client().admin().indices().getIndex(new GetIndexRequest().indices(backingIndex)).actionGet();
        assertThat(getIndexResponse.getSettings().get(backingIndex), notNullValue());
        assertThat(getIndexResponse.getSettings().get(backingIndex).getAsBoolean("index.hidden", null), is(true));
        mappings = getIndexResponse.getMappings().get(backingIndex).getSourceAsMap();
        assertThat(ObjectPath.eval("properties.@timestamp1.type", mappings), is("date"));

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
        assertThat(ObjectPath.eval("properties.@timestamp1.type", mappings), is("date"));

        backingIndex = DataStream.getDefaultBackingIndexName("metrics-bar", 2);
        getIndexResponse = client().admin().indices().getIndex(new GetIndexRequest().indices(backingIndex)).actionGet();
        assertThat(getIndexResponse.getSettings().get(backingIndex), notNullValue());
        assertThat(getIndexResponse.getSettings().get(backingIndex).getAsBoolean("index.hidden", null), is(true));
        mappings = getIndexResponse.getMappings().get(backingIndex).getSourceAsMap();
        assertThat(ObjectPath.eval("properties.@timestamp2.type", mappings), is("date"));

        int numDocsBar2 = randomIntBetween(2, 16);
        indexDocs("metrics-bar", numDocsBar2);
        int numDocsFoo2 = randomIntBetween(2, 16);
        indexDocs("metrics-foo", numDocsFoo2);

        verifyDocs("metrics-bar", numDocsBar + numDocsBar2, 1, 2);
        verifyDocs("metrics-foo", numDocsFoo + numDocsFoo2, 1, 2);

        DeleteDataStreamAction.Request deleteDataStreamRequest = new DeleteDataStreamAction.Request("metrics-*");
        client().admin().indices().deleteDataStream(deleteDataStreamRequest).actionGet();
        getDataStreamResponse = client().admin().indices().getDataStreams(getDataStreamRequest).actionGet();
        assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(0));

        expectThrows(IndexNotFoundException.class,
            () -> client().admin().indices().getIndex(new GetIndexRequest().indices(
                DataStream.getDefaultBackingIndexName("metrics-bar", 1))).actionGet());
        expectThrows(IndexNotFoundException.class,
            () -> client().admin().indices().getIndex(new GetIndexRequest().indices(
                DataStream.getDefaultBackingIndexName("metrics-bar", 2))).actionGet());
        expectThrows(IndexNotFoundException.class,
            () -> client().admin().indices().getIndex(new GetIndexRequest().indices(
                DataStream.getDefaultBackingIndexName("metrics-foo", 1))).actionGet());
        expectThrows(IndexNotFoundException.class,
            () -> client().admin().indices().getIndex(new GetIndexRequest().indices(
                DataStream.getDefaultBackingIndexName("metrics-foo", 2))).actionGet());
    }

    public void testOtherWriteOps() throws Exception {
        putComposableIndexTemplate("id", "@timestamp1", List.of("metrics-foobar*"));
        String dataStreamName = "metrics-foobar";
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(dataStreamName);
        client().admin().indices().createDataStream(createDataStreamRequest).get();

        {
            BulkRequest bulkRequest = new BulkRequest()
                .add(new IndexRequest(dataStreamName).source("{}", XContentType.JSON));
            expectFailure(dataStreamName, () -> client().bulk(bulkRequest).actionGet());
        }
        {
            BulkRequest bulkRequest = new BulkRequest()
                .add(new DeleteRequest(dataStreamName, "_id"));
            expectFailure(dataStreamName, () -> client().bulk(bulkRequest).actionGet());
        }
        {
            BulkRequest bulkRequest = new BulkRequest()
                .add(new UpdateRequest(dataStreamName, "_id").doc("{}", XContentType.JSON));
            expectFailure(dataStreamName, () -> client().bulk(bulkRequest).actionGet());
        }
        {
            IndexRequest indexRequest = new IndexRequest(dataStreamName).source("{}", XContentType.JSON);
            expectFailure(dataStreamName, () -> client().index(indexRequest).actionGet());
        }
        {
            UpdateRequest updateRequest = new UpdateRequest(dataStreamName, "_id")
                .doc("{}", XContentType.JSON);
            expectFailure(dataStreamName, () -> client().update(updateRequest).actionGet());
        }
        {
            DeleteRequest deleteRequest = new DeleteRequest(dataStreamName, "_id");
            expectFailure(dataStreamName, () -> client().delete(deleteRequest).actionGet());
        }
        {
            IndexRequest indexRequest = new IndexRequest(dataStreamName).source("{}", XContentType.JSON)
                .opType(DocWriteRequest.OpType.CREATE);
            IndexResponse indexResponse = client().index(indexRequest).actionGet();
            assertThat(indexResponse.getIndex(), equalTo(DataStream.getDefaultBackingIndexName(dataStreamName, 1)));
        }
        {
            BulkRequest bulkRequest = new BulkRequest()
                .add(new IndexRequest(dataStreamName).source("{}", XContentType.JSON)
                    .opType(DocWriteRequest.OpType.CREATE));
            BulkResponse bulkItemResponses  = client().bulk(bulkRequest).actionGet();
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

        String mapping = "{\n" +
            "      \"properties\": {\n" +
            "        \"baz_field\": {\n" +
            "          \"type\": \"keyword\"\n" +
            "        },\n" +
            "        \"@timestamp\": {\n" +
            "          \"type\": \"date\"\n" +
            "        }\n" +
            "      }\n" +
            "    }";
        PutComposableIndexTemplateAction.Request request = new PutComposableIndexTemplateAction.Request("id_1");
        request.indexTemplate(
            new ComposableIndexTemplate(
                List.of(dataStreamName), // use no wildcard, so that backing indices don't match just by name
                new Template(null,
                    new CompressedXContent(mapping), null),
                null, null, null, null,
                new ComposableIndexTemplate.DataStreamTemplate("@timestamp"))
        );
        client().execute(PutComposableIndexTemplateAction.INSTANCE, request).actionGet();

        int numDocs = randomIntBetween(2, 16);
        indexDocs(dataStreamName, numDocs);
        verifyDocs(dataStreamName, numDocs, 1, 1);

        String backingIndex = DataStream.getDefaultBackingIndexName(dataStreamName, 1);
        GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request("*");
        GetDataStreamAction.Response getDataStreamResponse = client().admin().indices().getDataStreams(getDataStreamRequest).actionGet();
        assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(1));
        assertThat(getDataStreamResponse.getDataStreams().get(0).getName(), equalTo(dataStreamName));
        assertThat(getDataStreamResponse.getDataStreams().get(0).getTimeStampField().getName(), equalTo("@timestamp"));
        assertThat(getDataStreamResponse.getDataStreams().get(0).getIndices().size(), equalTo(1));
        assertThat(getDataStreamResponse.getDataStreams().get(0).getIndices().get(0).getName(), equalTo(backingIndex));

        GetIndexResponse getIndexResponse =
            client().admin().indices().getIndex(new GetIndexRequest().indices(dataStreamName)).actionGet();
        assertThat(getIndexResponse.getSettings().get(backingIndex), notNullValue());
        assertThat(getIndexResponse.getSettings().get(backingIndex).getAsBoolean("index.hidden", null), is(true));
        assertThat(ObjectPath.eval("properties.baz_field.type",
            getIndexResponse.mappings().get(backingIndex).getSourceAsMap()), equalTo("keyword"));

        backingIndex = DataStream.getDefaultBackingIndexName(dataStreamName, 2);
        RolloverResponse rolloverResponse = client().admin().indices().rolloverIndex(new RolloverRequest(dataStreamName, null)).get();
        assertThat(rolloverResponse.getNewIndex(), equalTo(backingIndex));
        assertTrue(rolloverResponse.isRolledOver());

        getIndexResponse = client().admin().indices().getIndex(new GetIndexRequest().indices(backingIndex)).actionGet();
        assertThat(getIndexResponse.getSettings().get(backingIndex), notNullValue());
        assertThat(getIndexResponse.getSettings().get(backingIndex).getAsBoolean("index.hidden", null), is(true));
        assertThat(ObjectPath.eval("properties.baz_field.type",
            getIndexResponse.mappings().get(backingIndex).getSourceAsMap()), equalTo("keyword"));

        int numDocs2 = randomIntBetween(2, 16);
        indexDocs(dataStreamName, numDocs2);
        verifyDocs(dataStreamName, numDocs + numDocs2, 1, 2);

        DeleteDataStreamAction.Request deleteDataStreamRequest = new DeleteDataStreamAction.Request(dataStreamName);
        client().admin().indices().deleteDataStream(deleteDataStreamRequest).actionGet();
        getDataStreamResponse = client().admin().indices().getDataStreams(getDataStreamRequest).actionGet();
        assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(0));

        expectThrows(IndexNotFoundException.class,
            () -> client().admin().indices().getIndex(new GetIndexRequest().indices(
                DataStream.getDefaultBackingIndexName(dataStreamName, 1))).actionGet());
        expectThrows(IndexNotFoundException.class,
            () -> client().admin().indices().getIndex(new GetIndexRequest().indices(
                DataStream.getDefaultBackingIndexName(dataStreamName, 2))).actionGet());
    }

    public void testTimeStampValidationNoFieldMapping() throws Exception {
        // Adding a template without a mapping for timestamp field and expect template creation to fail.
        PutComposableIndexTemplateAction.Request createTemplateRequest = new PutComposableIndexTemplateAction.Request("logs-foo");
        createTemplateRequest.indexTemplate(
            new ComposableIndexTemplate(
                List.of("logs-*"),
                new Template(null, new CompressedXContent("{}"), null),
                null, null, null, null,
                new ComposableIndexTemplate.DataStreamTemplate("@timestamp"))
        );

        Exception e = expectThrows(IllegalArgumentException.class,
            () -> client().execute(PutComposableIndexTemplateAction.INSTANCE, createTemplateRequest).actionGet());
        assertThat(e.getCause().getCause().getMessage(), equalTo("expected timestamp field [@timestamp], but found no timestamp field"));
    }

    public void testTimeStampValidationInvalidFieldMapping() throws Exception {
        // Adding a template with an invalid mapping for timestamp field and expect template creation to fail.
        String mapping = "{\n" +
            "      \"properties\": {\n" +
            "        \"@timestamp\": {\n" +
            "          \"type\": \"keyword\"\n" +
            "        }\n" +
            "      }\n" +
            "    }";
        PutComposableIndexTemplateAction.Request createTemplateRequest = new PutComposableIndexTemplateAction.Request("logs-foo");
        createTemplateRequest.indexTemplate(
            new ComposableIndexTemplate(
                List.of("logs-*"),
                new Template(null, new CompressedXContent(mapping), null),
                null, null, null, null,
                new ComposableIndexTemplate.DataStreamTemplate("@timestamp"))
        );

        Exception e = expectThrows(IllegalArgumentException.class,
            () -> client().execute(PutComposableIndexTemplateAction.INSTANCE, createTemplateRequest).actionGet());
        assertThat(e.getCause().getCause().getMessage(), equalTo("expected timestamp field [@timestamp] to be of types " +
            "[date, date_nanos], but instead found type [keyword]"));
    }

    public void testResolvabilityOfDataStreamsInAPIs() throws Exception {
        putComposableIndexTemplate("id", "ts", List.of("logs-*"));
        String dataStreamName = "logs-foobar";
        CreateDataStreamAction.Request request = new CreateDataStreamAction.Request(dataStreamName);
        client().admin().indices().createDataStream(request).actionGet();

        verifyResolvability(dataStreamName, client().prepareIndex(dataStreamName)
                .setSource("{}", XContentType.JSON)
                .setOpType(DocWriteRequest.OpType.CREATE),
            false);
        verifyResolvability(dataStreamName, refreshBuilder(dataStreamName), false);
        verifyResolvability(dataStreamName, search(dataStreamName), false, 1);
        verifyResolvability(dataStreamName, msearch(null, dataStreamName), false);
        verifyResolvability(dataStreamName, clearCache(dataStreamName), false);
        verifyResolvability(dataStreamName, _flush(dataStreamName),false);
        verifyResolvability(dataStreamName, segments(dataStreamName), false);
        verifyResolvability(dataStreamName, indicesStats(dataStreamName), false);
        verifyResolvability(dataStreamName, IndicesOptionsIntegrationIT.forceMerge(dataStreamName), false);
        verifyResolvability(dataStreamName, validateQuery(dataStreamName), false);
        verifyResolvability(dataStreamName, client().admin().indices().prepareUpgrade(dataStreamName), false);
        verifyResolvability(dataStreamName, client().admin().indices().prepareRecoveries(dataStreamName), false);
        verifyResolvability(dataStreamName, client().admin().indices().prepareUpgradeStatus(dataStreamName), false);
        verifyResolvability(dataStreamName, getAliases(dataStreamName), true);
        verifyResolvability(dataStreamName, getFieldMapping(dataStreamName), true);
        verifyResolvability(dataStreamName, getMapping(dataStreamName), false);
        verifyResolvability(dataStreamName, getSettings(dataStreamName), false);
        verifyResolvability(dataStreamName, health(dataStreamName), false);
        verifyResolvability(dataStreamName, client().admin().cluster().prepareState().setIndices(dataStreamName), false);
        verifyResolvability(dataStreamName, client().prepareFieldCaps(dataStreamName).setFields("*"), false);
        verifyResolvability(dataStreamName, client().admin().indices().prepareGetIndex().addIndices(dataStreamName), false);

        request = new CreateDataStreamAction.Request("logs-barbaz");
        client().admin().indices().createDataStream(request).actionGet();
        verifyResolvability("logs-barbaz", client().prepareIndex("logs-barbaz")
                .setSource("{}", XContentType.JSON)
                .setOpType(DocWriteRequest.OpType.CREATE),
            false);

        String wildcardExpression = "logs*";
        verifyResolvability(wildcardExpression, refreshBuilder(wildcardExpression), false);
        verifyResolvability(wildcardExpression, search(wildcardExpression), false, 2);
        verifyResolvability(wildcardExpression, msearch(null, wildcardExpression), false);
        verifyResolvability(wildcardExpression, clearCache(wildcardExpression), false);
        verifyResolvability(wildcardExpression, _flush(wildcardExpression),false);
        verifyResolvability(wildcardExpression, segments(wildcardExpression), false);
        verifyResolvability(wildcardExpression, indicesStats(wildcardExpression), false);
        verifyResolvability(wildcardExpression, IndicesOptionsIntegrationIT.forceMerge(wildcardExpression), false);
        verifyResolvability(wildcardExpression, validateQuery(wildcardExpression), false);
        verifyResolvability(wildcardExpression, client().admin().indices().prepareUpgrade(wildcardExpression), false);
        verifyResolvability(wildcardExpression, client().admin().indices().prepareRecoveries(wildcardExpression), false);
        verifyResolvability(wildcardExpression, client().admin().indices().prepareUpgradeStatus(wildcardExpression), false);
        verifyResolvability(wildcardExpression, getAliases(wildcardExpression), true);
        verifyResolvability(wildcardExpression, getFieldMapping(wildcardExpression), true);
        verifyResolvability(wildcardExpression, getMapping(wildcardExpression), false);
        verifyResolvability(wildcardExpression, getSettings(wildcardExpression), false);
        verifyResolvability(wildcardExpression, health(wildcardExpression), false);
        verifyResolvability(wildcardExpression, client().admin().cluster().prepareState().setIndices(wildcardExpression), false);
        verifyResolvability(wildcardExpression, client().prepareFieldCaps(wildcardExpression).setFields("*"), false);
        verifyResolvability(wildcardExpression, client().admin().indices().prepareGetIndex().addIndices(wildcardExpression), false);
    }

    public void testCannotDeleteComposableTemplateUsedByDataStream() throws Exception {
        putComposableIndexTemplate("id", "@timestamp1", List.of("metrics-foobar*"));
        String dataStreamName = "metrics-foobar-baz";
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(dataStreamName);
        client().admin().indices().createDataStream(createDataStreamRequest).get();
        createDataStreamRequest = new CreateDataStreamAction.Request(dataStreamName + "-eggplant");
        client().admin().indices().createDataStream(createDataStreamRequest).get();

        DeleteComposableIndexTemplateAction.Request req = new DeleteComposableIndexTemplateAction.Request("id");
        Exception e = expectThrows(Exception.class, () -> client().execute(DeleteComposableIndexTemplateAction.INSTANCE, req).get());
        Optional<Exception> maybeE = ExceptionsHelper.unwrapCausesAndSuppressed(e, err ->
                err.getMessage().contains("unable to remove composable templates [id] " +
                    "as they are in use by a data streams [metrics-foobar-baz, metrics-foobar-baz-eggplant]"));
        assertTrue(maybeE.isPresent());

        DeleteComposableIndexTemplateAction.Request req2 = new DeleteComposableIndexTemplateAction.Request("i*");
        Exception e2 = expectThrows(Exception.class, () -> client().execute(DeleteComposableIndexTemplateAction.INSTANCE, req2).get());
        maybeE = ExceptionsHelper.unwrapCausesAndSuppressed(e2, err ->
            err.getMessage().contains("unable to remove composable templates [id] " +
                "as they are in use by a data streams [metrics-foobar-baz, metrics-foobar-baz-eggplant]"));
        assertTrue(maybeE.isPresent());
    }

    public void testAliasActionsFailOnDataStreams() throws Exception {
        putComposableIndexTemplate("id1", "@timestamp1", List.of("metrics-foo*"));
        String dataStreamName = "metrics-foo";
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(dataStreamName);
        client().admin().indices().createDataStream(createDataStreamRequest).get();

        IndicesAliasesRequest.AliasActions addAction = new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD)
            .index(dataStreamName).aliases("foo");
        IndicesAliasesRequest aliasesAddRequest = new IndicesAliasesRequest();
        aliasesAddRequest.addAliasAction(addAction);
        expectFailure(dataStreamName, () -> client().admin().indices().aliases(aliasesAddRequest).actionGet());
    }

    public void testAliasActionsFailOnDataStreamBackingIndices() throws Exception {
        putComposableIndexTemplate("id1", "@timestamp1", List.of("metrics-foo*"));
        String dataStreamName = "metrics-foo";
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(dataStreamName);
        client().admin().indices().createDataStream(createDataStreamRequest).get();

        String backingIndex = DataStream.getDefaultBackingIndexName(dataStreamName, 1);
        IndicesAliasesRequest.AliasActions addAction = new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD)
            .index(backingIndex).aliases("first_gen");
        IndicesAliasesRequest aliasesAddRequest = new IndicesAliasesRequest();
        aliasesAddRequest.addAliasAction(addAction);
        Exception e = expectThrows(IllegalArgumentException.class, () -> client().admin().indices().aliases(aliasesAddRequest).actionGet());
        assertThat(e.getMessage(), equalTo("The provided expressions [" + backingIndex
            + "] match a backing index belonging to data stream [" + dataStreamName + "]. Data streams and their backing indices don't " +
            "support aliases."));
    }

    public void testChangeTimestampFieldInComposableTemplatePriorToRollOver() throws Exception {
        putComposableIndexTemplate("id1", "@timestamp", List.of("logs-foo*"));

        // Index doc that triggers creation of a data stream
        IndexRequest indexRequest = new IndexRequest("logs-foobar").source("{}", XContentType.JSON).opType("create");
        IndexResponse indexResponse = client().index(indexRequest).actionGet();
        assertThat(indexResponse.getIndex(), equalTo(DataStream.getDefaultBackingIndexName("logs-foobar", 1)));
        assertBackingIndex(DataStream.getDefaultBackingIndexName("logs-foobar", 1), "properties.@timestamp");

        // Rollover data stream
        RolloverResponse rolloverResponse = client().admin().indices().rolloverIndex(new RolloverRequest("logs-foobar", null)).get();
        assertThat(rolloverResponse.getNewIndex(), equalTo(DataStream.getDefaultBackingIndexName("logs-foobar", 2)));
        assertTrue(rolloverResponse.isRolledOver());
        assertBackingIndex(DataStream.getDefaultBackingIndexName("logs-foobar", 2), "properties.@timestamp");

        // Index another doc into a data stream
        indexRequest = new IndexRequest("logs-foobar").source("{}", XContentType.JSON).opType("create");
        indexResponse = client().index(indexRequest).actionGet();
        assertThat(indexResponse.getIndex(), equalTo(DataStream.getDefaultBackingIndexName("logs-foobar", 2)));

        // Change the template to have a different timestamp field
        putComposableIndexTemplate("id1", "@timestamp2", List.of("logs-foo*"));

        // Rollover again, eventhough there is no mapping in the template, the timestamp field mapping in data stream
        // should be applied in the new backing index
        rolloverResponse = client().admin().indices().rolloverIndex(new RolloverRequest("logs-foobar", null)).get();
        assertThat(rolloverResponse.getNewIndex(), equalTo(DataStream.getDefaultBackingIndexName("logs-foobar", 3)));
        assertTrue(rolloverResponse.isRolledOver());
        assertBackingIndex(DataStream.getDefaultBackingIndexName("logs-foobar", 3), "properties.@timestamp");

        // Index another doc into a data stream
        indexRequest = new IndexRequest("logs-foobar").source("{}", XContentType.JSON).opType("create");
        indexResponse = client().index(indexRequest).actionGet();
        assertThat(indexResponse.getIndex(), equalTo(DataStream.getDefaultBackingIndexName("logs-foobar", 3)));

        DeleteDataStreamAction.Request deleteDataStreamRequest = new DeleteDataStreamAction.Request("logs-foobar");
        client().admin().indices().deleteDataStream(deleteDataStreamRequest).actionGet();
    }

    public void testNestedTimestampField() throws Exception {
        String mapping = "{\n" +
            "      \"properties\": {\n" +
            "        \"event\": {\n" +
            "          \"properties\": {\n" +
            "            \"@timestamp\": {\n" +
            "              \"type\": \"date\"" +
            "            }\n" +
            "          }\n" +
            "        }\n" +
            "      }\n" +
            "    }";;
        putComposableIndexTemplate("id1", "event.@timestamp", mapping, List.of("logs-foo*"));

        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request("logs-foobar");
        client().admin().indices().createDataStream(createDataStreamRequest).get();
        GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request("logs-foobar");
        GetDataStreamAction.Response getDataStreamResponse = client().admin().indices().getDataStreams(getDataStreamRequest).actionGet();
        assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(1));
        assertThat(getDataStreamResponse.getDataStreams().get(0).getName(), equalTo("logs-foobar"));
        assertThat(getDataStreamResponse.getDataStreams().get(0).getTimeStampField().getName(), equalTo("event.@timestamp"));
        assertThat(getDataStreamResponse.getDataStreams().get(0).getTimeStampField().getFieldMapping(), equalTo(Map.of("type", "date")));
        assertBackingIndex(DataStream.getDefaultBackingIndexName("logs-foobar", 1), "properties.event.properties.@timestamp");

        // Change the template to have a different timestamp field
        putComposableIndexTemplate("id1", "@timestamp2", List.of("logs-foo*"));

        RolloverResponse rolloverResponse = client().admin().indices().rolloverIndex(new RolloverRequest("logs-foobar", null)).actionGet();
        assertThat(rolloverResponse.getNewIndex(), equalTo(DataStream.getDefaultBackingIndexName("logs-foobar", 2)));
        assertTrue(rolloverResponse.isRolledOver());
        assertBackingIndex(DataStream.getDefaultBackingIndexName("logs-foobar", 2), "properties.event.properties.@timestamp");

        DeleteDataStreamAction.Request deleteDataStreamRequest = new DeleteDataStreamAction.Request("logs-foobar");
        client().admin().indices().deleteDataStream(deleteDataStreamRequest).actionGet();
    }

    public void testTimestampFieldCustomAttributes() throws Exception {
        String mapping = "{\n" +
            "      \"properties\": {\n" +
            "        \"@timestamp\": {\n" +
            "          \"type\": \"date\",\n" +
            "          \"format\": \"yyyy-MM\",\n" +
            "          \"meta\": {\n" +
            "            \"x\": \"y\"\n" +
            "          },\n" +
            "          \"store\": true\n" +
            "        }\n" +
            "      }\n" +
            "    }";
        putComposableIndexTemplate("id1", "@timestamp", mapping, List.of("logs-foo*"));

        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request("logs-foobar");
        client().admin().indices().createDataStream(createDataStreamRequest).get();
        GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request("logs-foobar");
        GetDataStreamAction.Response getDataStreamResponse = client().admin().indices().getDataStreams(getDataStreamRequest).actionGet();
        assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(1));
        assertThat(getDataStreamResponse.getDataStreams().get(0).getName(), equalTo("logs-foobar"));
        assertThat(getDataStreamResponse.getDataStreams().get(0).getTimeStampField().getName(), equalTo("@timestamp"));
        Map<?, ?> expectedTimestampMapping = Map.of("type", "date", "format", "yyyy-MM", "meta", Map.of("x", "y"), "store", true);
        assertThat(getDataStreamResponse.getDataStreams().get(0).getTimeStampField().getFieldMapping(), equalTo(expectedTimestampMapping));
        assertBackingIndex(DataStream.getDefaultBackingIndexName("logs-foobar", 1), "properties.@timestamp", expectedTimestampMapping);

        // Change the template to have a different timestamp field
        putComposableIndexTemplate("id1", "@timestamp2", List.of("logs-foo*"));

        RolloverResponse rolloverResponse = client().admin().indices().rolloverIndex(new RolloverRequest("logs-foobar", null)).actionGet();
        assertThat(rolloverResponse.getNewIndex(), equalTo(DataStream.getDefaultBackingIndexName("logs-foobar", 2)));
        assertTrue(rolloverResponse.isRolledOver());
        assertBackingIndex(DataStream.getDefaultBackingIndexName("logs-foobar", 2), "properties.@timestamp", expectedTimestampMapping);

        DeleteDataStreamAction.Request deleteDataStreamRequest = new DeleteDataStreamAction.Request("logs-foobar");
        client().admin().indices().deleteDataStream(deleteDataStreamRequest).actionGet();
    }

    private static void assertBackingIndex(String backingIndex, String timestampFieldPathInMapping) {
        assertBackingIndex(backingIndex, timestampFieldPathInMapping, Map.of("type", "date"));
    }

    private static void assertBackingIndex(String backingIndex, String timestampFieldPathInMapping, Map<?, ?> expectedMapping) {
        GetIndexResponse getIndexResponse =
            client().admin().indices().getIndex(new GetIndexRequest().indices(backingIndex)).actionGet();
        assertThat(getIndexResponse.getSettings().get(backingIndex), notNullValue());
        assertThat(getIndexResponse.getSettings().get(backingIndex).getAsBoolean("index.hidden", null), is(true));
        Map<?, ?> mappings = getIndexResponse.getMappings().get(backingIndex).getSourceAsMap();
        assertThat(ObjectPath.eval(timestampFieldPathInMapping, mappings), is(expectedMapping));
    }

    private static void verifyResolvability(String dataStream, ActionRequestBuilder requestBuilder, boolean fail) {
        verifyResolvability(dataStream, requestBuilder, fail, 0);
    }

    private static void verifyResolvability(String dataStream, ActionRequestBuilder requestBuilder, boolean fail, long expectedCount) {
        if (fail) {
            String expectedErrorMessage = "The provided expression [" + dataStream +
                "] matches a data stream, specify the corresponding concrete indices instead.";
            if (requestBuilder instanceof MultiSearchRequestBuilder) {
                MultiSearchResponse multiSearchResponse = ((MultiSearchRequestBuilder) requestBuilder).get();
                assertThat(multiSearchResponse.getResponses().length, equalTo(1));
                assertThat(multiSearchResponse.getResponses()[0].isFailure(), is(true));
                assertThat(multiSearchResponse.getResponses()[0].getFailure(), instanceOf(IllegalArgumentException.class));
                assertThat(multiSearchResponse.getResponses()[0].getFailure().getMessage(), equalTo(expectedErrorMessage));
            } else if (requestBuilder instanceof ValidateQueryRequestBuilder) {
                ValidateQueryResponse response = (ValidateQueryResponse) requestBuilder.get();
                assertThat(response.getQueryExplanation().get(0).getError(), equalTo(expectedErrorMessage));
            } else {
                Exception e = expectThrows(IllegalArgumentException.class, requestBuilder::get);
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
            bulkRequest.add(new IndexRequest(dataStream)
                .opType(DocWriteRequest.OpType.CREATE)
                .source("{}", XContentType.JSON));
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
        Arrays.stream(searchResponse.getHits().getHits()).forEach(hit -> {
            assertTrue(expectedIndices.contains(hit.getIndex()));
        });
    }

    private static void expectFailure(String dataStreamName, ThrowingRunnable runnable) {
        Exception e = expectThrows(IllegalArgumentException.class, runnable);
        assertThat(e.getMessage(), equalTo("The provided expression [" + dataStreamName +
            "] matches a data stream, specify the corresponding concrete indices instead."));
    }

    public static void putComposableIndexTemplate(String id, String timestampFieldName, List<String> patterns) throws IOException {
        String mapping = MetadataCreateDataStreamServiceTests.generateMapping(timestampFieldName);
        putComposableIndexTemplate(id, timestampFieldName, mapping, patterns);
    }

    static void putComposableIndexTemplate(String id, String timestampFieldName, String mapping, List<String> patterns) throws IOException {
        PutComposableIndexTemplateAction.Request request = new PutComposableIndexTemplateAction.Request(id);
        request.indexTemplate(
            new ComposableIndexTemplate(
                patterns,
                new Template(null, new CompressedXContent(mapping), null),
                null, null, null, null,
                new ComposableIndexTemplate.DataStreamTemplate(timestampFieldName))
        );
        client().execute(PutComposableIndexTemplateAction.INSTANCE, request).actionGet();
    }

}
