/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.datastreams;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.template.delete.DeleteComposableIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.get.GetComposableIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryRequestBuilder;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.datastreams.ModifyDataStreamsAction;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.MultiSearchRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStream.TimestampField;
import org.elasticsearch.cluster.metadata.DataStreamAction;
import org.elasticsearch.cluster.metadata.DataStreamAlias;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.List;
import org.elasticsearch.core.Map;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.indices.InvalidAliasNameException;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FieldAndFormat;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.ObjectPath;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.action.CreateDataStreamAction;
import org.elasticsearch.xpack.core.action.DeleteDataStreamAction;
import org.elasticsearch.xpack.core.action.GetDataStreamAction;
import org.elasticsearch.xpack.core.action.GetDataStreamAction.Response.DataStreamInfo;
import org.elasticsearch.xpack.datastreams.DataStreamsPlugin;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.action.DocWriteRequest.OpType.CREATE;
import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.backingIndexEqualTo;
import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.generateMapping;
import static org.elasticsearch.cluster.metadata.MetadataIndexTemplateService.DEFAULT_TIMESTAMP_FIELD;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItemInArray;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

@ESIntegTestCase.ClusterScope(transportClientRatio = 0)
public class DataStreamIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(DataStreamsPlugin.class);
    }

    @After
    public void cleanup() {
        DeleteDataStreamAction.Request deleteDataStreamsRequest = new DeleteDataStreamAction.Request("*");
        assertAcked(client().execute(DeleteDataStreamAction.INSTANCE, deleteDataStreamsRequest).actionGet());

        DeleteComposableIndexTemplateAction.Request deleteTemplateRequest = new DeleteComposableIndexTemplateAction.Request("*");
        assertAcked(client().execute(DeleteComposableIndexTemplateAction.INSTANCE, deleteTemplateRequest).actionGet());
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
        assertThat(firstDataStream.getIndices().get(0).getName(), backingIndexEqualTo("metrics-bar", 1));
        DataStream dataStream = getDataStreamResponse.getDataStreams().get(1).getDataStream();
        assertThat(dataStream.getName(), equalTo("metrics-foo"));
        assertThat(dataStream.getTimeStampField().getName(), equalTo("@timestamp"));
        assertThat(dataStream.getIndices().size(), equalTo(1));
        assertThat(dataStream.getIndices().get(0).getName(), backingIndexEqualTo("metrics-foo", 1));

        String backingIndex = DataStream.getDefaultBackingIndexName("metrics-bar", 1);
        GetIndexResponse getIndexResponse = client().admin().indices().getIndex(new GetIndexRequest().indices(backingIndex)).actionGet();
        assertThat(getIndexResponse.getSettings().get(backingIndex), notNullValue());
        assertThat(getIndexResponse.getSettings().get(backingIndex).getAsBoolean("index.hidden", null), is(true));
        java.util.Map<?, ?> mappings = getIndexResponse.getMappings().get(backingIndex).get("_doc").getSourceAsMap();
        assertThat(ObjectPath.eval("properties.@timestamp.type", mappings), is("date"));

        backingIndex = DataStream.getDefaultBackingIndexName("metrics-foo", 1);
        getIndexResponse = client().admin().indices().getIndex(new GetIndexRequest().indices(backingIndex)).actionGet();
        assertThat(getIndexResponse.getSettings().get(backingIndex), notNullValue());
        assertThat(getIndexResponse.getSettings().get(backingIndex).getAsBoolean("index.hidden", null), is(true));
        mappings = getIndexResponse.getMappings().get(backingIndex).get("_doc").getSourceAsMap();
        assertThat(ObjectPath.eval("properties.@timestamp.type", mappings), is("date"));

        int numDocsBar = randomIntBetween(2, 16);
        indexDocs("metrics-bar", numDocsBar);
        int numDocsFoo = randomIntBetween(2, 16);
        indexDocs("metrics-foo", numDocsFoo);

        verifyDocs("metrics-bar", numDocsBar, 1, 1);
        verifyDocs("metrics-foo", numDocsFoo, 1, 1);

        RolloverResponse rolloverResponse = client().admin().indices().rolloverIndex(new RolloverRequest("metrics-foo", null)).get();
        assertThat(rolloverResponse.getNewIndex(), backingIndexEqualTo("metrics-foo", 2));
        assertTrue(rolloverResponse.isRolledOver());

        rolloverResponse = client().admin().indices().rolloverIndex(new RolloverRequest("metrics-bar", null)).get();
        assertThat(rolloverResponse.getNewIndex(), backingIndexEqualTo("metrics-bar", 2));
        assertTrue(rolloverResponse.isRolledOver());

        backingIndex = DataStream.getDefaultBackingIndexName("metrics-foo", 2);
        getIndexResponse = client().admin().indices().getIndex(new GetIndexRequest().indices(backingIndex)).actionGet();
        assertThat(getIndexResponse.getSettings().get(backingIndex), notNullValue());
        assertThat(getIndexResponse.getSettings().get(backingIndex).getAsBoolean("index.hidden", null), is(true));
        mappings = getIndexResponse.getMappings().get(backingIndex).get("_doc").getSourceAsMap();
        assertThat(ObjectPath.eval("properties.@timestamp.type", mappings), is("date"));

        backingIndex = DataStream.getDefaultBackingIndexName("metrics-bar", 2);
        getIndexResponse = client().admin().indices().getIndex(new GetIndexRequest().indices(backingIndex)).actionGet();
        assertThat(getIndexResponse.getSettings().get(backingIndex), notNullValue());
        assertThat(getIndexResponse.getSettings().get(backingIndex).getAsBoolean("index.hidden", null), is(true));
        mappings = getIndexResponse.getMappings().get(backingIndex).get("_doc").getSourceAsMap();
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
            assertThat(indexResponse.getIndex(), backingIndexEqualTo(dataStreamName, 1));
        }
        {
            BulkRequest bulkRequest = new BulkRequest().add(
                new IndexRequest(dataStreamName).source("{\"@timestamp\": \"2020-12-12\"}", XContentType.JSON)
                    .opType(DocWriteRequest.OpType.CREATE)
            );
            BulkResponse bulkItemResponses = client().bulk(bulkRequest).actionGet();
            assertThat(bulkItemResponses.getItems()[0].getIndex(), backingIndexEqualTo(dataStreamName, 1));
        }

        {
            // TODO: remove when fixing the bug when an index matching a backing index name is created before the data stream is created
            createDataStreamRequest = new CreateDataStreamAction.Request(dataStreamName + "-baz");
            client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();

            BulkRequest bulkRequest = new BulkRequest().add(
                new IndexRequest(dataStreamName).source("{\"@timestamp\": \"2020-12-12\"}", XContentType.JSON),
                new IndexRequest(dataStreamName).source("{\"@timestamp\": \"2020-12-12\"}", XContentType.JSON).create(true),
                new IndexRequest(dataStreamName).source("{\"@timestamp\": \"2020-12-12\"}", XContentType.JSON),
                new UpdateRequest(dataStreamName, "_id").doc("{\"@timestamp1\": \"2020-12-12\"}", XContentType.JSON),
                new DeleteRequest(dataStreamName, "_id"),
                new IndexRequest(dataStreamName + "-baz").source("{\"@timestamp\": \"2020-12-12\"}", XContentType.JSON).create(true),
                new DeleteRequest(dataStreamName + "-baz", "_id"),
                new IndexRequest(dataStreamName + "-baz").source("{\"@timestamp\": \"2020-12-12\"}", XContentType.JSON),
                new IndexRequest(dataStreamName + "-baz").source("{\"@timestamp\": \"2020-12-12\"}", XContentType.JSON).create(true),
                // Non create ops directly against backing indices are allowed:
                new DeleteRequest(DataStream.getDefaultBackingIndexName(dataStreamName + "-baz", 1), "_id"),
                new IndexRequest(DataStream.getDefaultBackingIndexName(dataStreamName + "-baz", 1)).source(
                    "{\"@timestamp\": \"2020-12-12\"}",
                    XContentType.JSON
                ).id("_id").setIfSeqNo(1).setIfPrimaryTerm(1)
            );
            BulkResponse bulkResponse = client().bulk(bulkRequest).actionGet();
            assertThat(bulkResponse.getItems(), arrayWithSize(11));
            {
                assertThat(bulkResponse.getItems()[0].getFailure(), notNullValue());
                assertThat(bulkResponse.getItems()[0].getResponse(), nullValue());
                assertThat(
                    bulkResponse.getItems()[0].getFailure().getMessage(),
                    containsString("only write ops with an op_type of create are allowed in data streams")
                );
            }
            {
                assertThat(bulkResponse.getItems()[1].getFailure(), nullValue());
                assertThat(bulkResponse.getItems()[1].getResponse(), notNullValue());
                assertThat(bulkResponse.getItems()[1].getIndex(), backingIndexEqualTo(dataStreamName, 1));
            }
            {
                assertThat(bulkResponse.getItems()[2].getFailure(), notNullValue());
                assertThat(bulkResponse.getItems()[2].getResponse(), nullValue());
                assertThat(
                    bulkResponse.getItems()[2].getFailure().getMessage(),
                    containsString("only write ops with an op_type of create are allowed in data streams")
                );
            }
            {
                assertThat(bulkResponse.getItems()[3].getFailure(), notNullValue());
                assertThat(bulkResponse.getItems()[3].getResponse(), nullValue());
                assertThat(
                    bulkResponse.getItems()[3].getFailure().getMessage(),
                    containsString("only write ops with an op_type of create are allowed in data streams")
                );
            }
            {
                assertThat(bulkResponse.getItems()[4].getFailure(), notNullValue());
                assertThat(bulkResponse.getItems()[4].getResponse(), nullValue());
                assertThat(
                    bulkResponse.getItems()[4].getFailure().getMessage(),
                    containsString("only write ops with an op_type of create are allowed in data streams")
                );
            }
            {
                assertThat(bulkResponse.getItems()[5].getFailure(), nullValue());
                assertThat(bulkResponse.getItems()[5].getResponse(), notNullValue());
                assertThat(bulkResponse.getItems()[5].getIndex(), backingIndexEqualTo(dataStreamName + "-baz", 1));
            }
            {
                assertThat(bulkResponse.getItems()[6].getFailure(), notNullValue());
                assertThat(bulkResponse.getItems()[6].getResponse(), nullValue());
                assertThat(
                    bulkResponse.getItems()[6].getFailure().getMessage(),
                    containsString("only write ops with an op_type of create are allowed in data streams")
                );
            }
            {
                assertThat(bulkResponse.getItems()[7].getFailure(), notNullValue());
                assertThat(bulkResponse.getItems()[7].getResponse(), nullValue());
                assertThat(
                    bulkResponse.getItems()[7].getFailure().getMessage(),
                    containsString("only write ops with an op_type of create are allowed in data streams")
                );
            }
            {
                assertThat(bulkResponse.getItems()[8].getFailure(), nullValue());
                assertThat(bulkResponse.getItems()[8].getResponse(), notNullValue());
                assertThat(bulkResponse.getItems()[8].getIndex(), backingIndexEqualTo(dataStreamName + "-baz", 1));
            }
            {
                assertThat(bulkResponse.getItems()[9].getFailure(), nullValue());
                assertThat(bulkResponse.getItems()[9].getResponse(), notNullValue());
                assertThat(bulkResponse.getItems()[9].getIndex(), backingIndexEqualTo(dataStreamName + "-baz", 1));
            }
            {
                assertThat(bulkResponse.getItems()[10].getResponse(), nullValue());
                assertThat(bulkResponse.getItems()[10].getFailure(), notNullValue());
                assertThat(bulkResponse.getItems()[10].status(), equalTo(RestStatus.CONFLICT));
                assertThat(bulkResponse.getItems()[10].getIndex(), backingIndexEqualTo(dataStreamName + "-baz", 1));
            }
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
                Collections.singletonList(dataStreamName), // use no wildcard, so that backing indices don't match just by name
                new Template(null, new CompressedXContent(mapping), null),
                null,
                null,
                null,
                null,
                new ComposableIndexTemplate.DataStreamTemplate(),
                null
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
            ObjectPath.eval("properties.baz_field.type", getIndexResponse.mappings().get(backingIndex).get("_doc").getSourceAsMap()),
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
            ObjectPath.eval("properties.baz_field.type", getIndexResponse.mappings().get(backingIndex).get("_doc").getSourceAsMap()),
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
                Collections.singletonList("logs-*"),
                new Template(null, new CompressedXContent(mapping), null),
                null,
                null,
                null,
                null,
                new ComposableIndexTemplate.DataStreamTemplate(),
                null
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
        IndicesAliasesRequest aliasesRequest = new IndicesAliasesRequest();
        String aliasToDataStream = "logs";
        aliasesRequest.addAliasAction(new AliasActions(AliasActions.Type.ADD).alias(aliasToDataStream).index("logs-foobar"));
        assertAcked(client().admin().indices().aliases(aliasesRequest).actionGet());

        verifyResolvability(
            dataStreamName,
            client().prepareIndex(dataStreamName, "_doc")
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
        verifyResolvability(dataStreamName, client().admin().indices().prepareGetAliases("dummy").addIndices(dataStreamName), false);
        verifyResolvability(dataStreamName, client().admin().indices().prepareGetFieldMappings(dataStreamName), false);
        verifyResolvability(
            dataStreamName,
            client().admin()
                .indices()
                .preparePutMapping(dataStreamName)
                .setType("_doc")
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
        verifyResolvability(dataStreamName, client().admin().indices().prepareClose(dataStreamName), true);
        verifyResolvability(aliasToDataStream, client().admin().indices().prepareClose(aliasToDataStream), true);
        verifyResolvability(dataStreamName, client().admin().cluster().prepareSearchShards(dataStreamName), false);
        verifyResolvability(dataStreamName, client().admin().indices().prepareShardStores(dataStreamName), false);

        request = new CreateDataStreamAction.Request("logs-barbaz");
        client().execute(CreateDataStreamAction.INSTANCE, request).actionGet();
        verifyResolvability(
            "logs-barbaz",
            client().prepareIndex("logs-barbaz", "_doc")
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
                .setType("_doc")
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
        verifyResolvability(wildcardExpression, client().admin().indices().prepareClose(wildcardExpression), false);
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

        // Now replace it with a higher-priority template and delete the old one
        PutComposableIndexTemplateAction.Request request = new PutComposableIndexTemplateAction.Request("id2");
        request.indexTemplate(
            new ComposableIndexTemplate(
                Collections.singletonList("metrics-foobar*"), // Match the other data stream with a slightly different pattern
                new Template(null, null, null),
                null,
                2L, // Higher priority than the other composable template
                null,
                null,
                new ComposableIndexTemplate.DataStreamTemplate(),
                null
            )
        );
        client().execute(PutComposableIndexTemplateAction.INSTANCE, request).actionGet();

        DeleteComposableIndexTemplateAction.Request deleteRequest = new DeleteComposableIndexTemplateAction.Request("id");
        client().execute(DeleteComposableIndexTemplateAction.INSTANCE, deleteRequest).get();

        GetComposableIndexTemplateAction.Request getReq = new GetComposableIndexTemplateAction.Request("id");
        Exception e3 = expectThrows(Exception.class, () -> client().execute(GetComposableIndexTemplateAction.INSTANCE, getReq).get());
        maybeE = ExceptionsHelper.unwrapCausesAndSuppressed(e3, err -> err.getMessage().contains("index template matching [id] not found"));
        assertTrue(maybeE.isPresent());
    }

    public void testAliasActionsOnDataStreams() throws Exception {
        putComposableIndexTemplate("id1", List.of("metrics-foo*"));
        String dataStreamName = "metrics-foo";
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(dataStreamName);
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();

        AliasActions addAction = new AliasActions(AliasActions.Type.ADD).index(dataStreamName).aliases("foo");
        IndicesAliasesRequest aliasesAddRequest = new IndicesAliasesRequest();
        aliasesAddRequest.addAliasAction(addAction);
        assertAcked(client().admin().indices().aliases(aliasesAddRequest).actionGet());
        GetAliasesResponse response = client().admin().indices().getAliases(new GetAliasesRequest()).actionGet();
        assertThat(
            response.getDataStreamAliases(),
            equalTo(Map.of("metrics-foo", List.of(new DataStreamAlias("foo", List.of("metrics-foo"), null, null))))
        );
    }

    public void testDataSteamAliasWithFilter() throws Exception {
        putComposableIndexTemplate("id1", List.of("logs-*"));
        String dataStreamName = "logs-foobar";
        client().prepareIndex(dataStreamName, "_doc")
            .setId("1")
            .setSource("{\"@timestamp\": \"2022-12-12\", \"type\": \"x\"}", XContentType.JSON)
            .setOpType(DocWriteRequest.OpType.CREATE)
            .get();
        client().prepareIndex(dataStreamName, "_doc")
            .setId("2")
            .setSource("{\"@timestamp\": \"2022-12-12\", \"type\": \"y\"}", XContentType.JSON)
            .setOpType(DocWriteRequest.OpType.CREATE)
            .get();
        refresh(dataStreamName);

        AliasActions addAction = new AliasActions(AliasActions.Type.ADD).index(dataStreamName)
            .aliases("foo")
            .filter(Map.of("term", Map.of("type", Map.of("value", "y"))));
        IndicesAliasesRequest aliasesAddRequest = new IndicesAliasesRequest();
        aliasesAddRequest.addAliasAction(addAction);
        assertAcked(client().admin().indices().aliases(aliasesAddRequest).actionGet());
        GetAliasesResponse response = client().admin().indices().getAliases(new GetAliasesRequest()).actionGet();
        assertThat(
            response.getDataStreamAliases(),
            equalTo(
                Map.of(
                    "logs-foobar",
                    List.of(new DataStreamAlias("foo", List.of("logs-foobar"), null, Map.of("term", Map.of("type", Map.of("value", "y")))))
                )
            )
        );

        // Searching the data stream directly should return all hits:
        SearchResponse searchResponse = client().prepareSearch("logs-foobar").get();
        assertSearchHits(searchResponse, "1", "2");
        // Search the alias should only return document 2, because it matches with the defined filter in the alias:
        searchResponse = client().prepareSearch("foo").get();
        assertSearchHits(searchResponse, "2");

        // Update alias:
        addAction = new AliasActions(AliasActions.Type.ADD).index(dataStreamName)
            .aliases("foo")
            .filter(Map.of("term", Map.of("type", Map.of("value", "x"))));
        aliasesAddRequest = new IndicesAliasesRequest();
        aliasesAddRequest.addAliasAction(addAction);
        assertAcked(client().admin().indices().aliases(aliasesAddRequest).actionGet());
        response = client().admin().indices().getAliases(new GetAliasesRequest()).actionGet();
        assertThat(
            response.getDataStreamAliases(),
            equalTo(
                Map.of(
                    "logs-foobar",
                    List.of(new DataStreamAlias("foo", List.of("logs-foobar"), null, Map.of("term", Map.of("type", Map.of("value", "x")))))
                )
            )
        );

        // Searching the data stream directly should return all hits:
        searchResponse = client().prepareSearch("logs-foobar").get();
        assertSearchHits(searchResponse, "1", "2");
        // Search the alias should only return document 1, because it matches with the defined filter in the alias:
        searchResponse = client().prepareSearch("foo").get();
        assertSearchHits(searchResponse, "1");
    }

    public void testRandomDataSteamAliasesUpdate() throws Exception {
        putComposableIndexTemplate("id1", List.of("log-*"));

        String alias = randomAlphaOfLength(4);
        String[] dataStreams = Arrays.stream(generateRandomStringArray(16, 4, false, false))
            .map(s -> "log-" + s.toLowerCase(Locale.ROOT))
            .distinct()
            .toArray(String[]::new);
        for (String dataStream : dataStreams) {
            CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(dataStream);
            client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();
        }
        AliasActions addAction = new AliasActions(AliasActions.Type.ADD).aliases(alias)
            .indices(dataStreams)
            .filter(Map.of("term", Map.of("type", Map.of("value", "y"))));
        assertAcked(client().admin().indices().aliases(new IndicesAliasesRequest().addAliasAction(addAction)).actionGet());

        addAction = new AliasActions(AliasActions.Type.ADD).aliases(alias).indices(dataStreams[0]).writeIndex(true);
        assertAcked(client().admin().indices().aliases(new IndicesAliasesRequest().addAliasAction(addAction)).actionGet());

        GetAliasesResponse response = client().admin().indices().getAliases(new GetAliasesRequest()).actionGet();
        assertThat(response.getDataStreamAliases().size(), equalTo(dataStreams.length));
        java.util.List<DataStreamAlias> result = response.getDataStreamAliases()
            .values()
            .stream()
            .flatMap(Collection::stream)
            .distinct()
            .collect(Collectors.toList());
        assertThat(result, hasSize(1));
        assertThat(result.get(0).getName(), equalTo(alias));
        assertThat(result.get(0).getDataStreams(), containsInAnyOrder(dataStreams));
        assertThat(result.get(0).getWriteDataStream(), equalTo(dataStreams[0]));
        assertThat(result.get(0).getFilter().string(), equalTo("{\"term\":{\"type\":{\"value\":\"y\"}}}"));
    }

    public void testDataSteamAliasWithMalformedFilter() throws Exception {
        putComposableIndexTemplate("id1", List.of("log-*"));

        String alias = randomAlphaOfLength(4);
        String dataStream = "log-" + randomAlphaOfLength(4).toLowerCase(Locale.ROOT);
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(dataStream);
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();

        AliasActions addAction = new AliasActions(AliasActions.Type.ADD).aliases(alias).indices(dataStream);
        if (randomBoolean()) {
            // non existing attribute:
            addAction.filter(Map.of("term", Map.of("foo", Map.of("value", "bar", "x", "y"))));
        } else {
            // Unknown query:
            addAction.filter(Map.of("my_query", Map.of("x", "y")));
        }
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin().indices().aliases(new IndicesAliasesRequest().addAliasAction(addAction)).actionGet()
        );
        assertThat(e.getMessage(), equalTo("failed to parse filter for alias [" + alias + "]"));
        GetAliasesResponse response = client().admin().indices().getAliases(new GetAliasesRequest()).actionGet();
        assertThat(response.getDataStreamAliases(), anEmptyMap());
    }

    public void testAliasActionsFailOnDataStreamBackingIndices() throws Exception {
        putComposableIndexTemplate("id1", List.of("metrics-foo*"));
        String dataStreamName = "metrics-foo";
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(dataStreamName);
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();

        String backingIndex = DataStream.getDefaultBackingIndexName(dataStreamName, 1);
        AliasActions addAction = new AliasActions(AliasActions.Type.ADD).index(backingIndex).aliases("first_gen");
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
                    + "]. Data stream backing indices don't "
                    + "support aliases."
            )
        );
    }

    public void testAddDataStreamAliasesMixedExpressionValidation() throws Exception {
        createIndex("metrics-myindex");
        putComposableIndexTemplate("id1", List.of("metrics-*"));
        String dataStreamName = "metrics-foo";
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(dataStreamName);
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();

        AliasActions addAction = new AliasActions(AliasActions.Type.ADD).index("metrics-*").aliases("my-alias");
        IndicesAliasesRequest aliasesAddRequest = new IndicesAliasesRequest();
        aliasesAddRequest.addAliasAction(addAction);
        Exception e = expectThrows(IllegalArgumentException.class, () -> client().admin().indices().aliases(aliasesAddRequest).actionGet());
        assertThat(e.getMessage(), equalTo("expressions [metrics-*] that match with both data streams and regular indices are disallowed"));
    }

    public void testRemoveDataStreamAliasesMixedExpression() throws Exception {
        createIndex("metrics-myindex");
        putComposableIndexTemplate("id1", List.of("metrics-*"));
        String dataStreamName = "metrics-foo";
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(dataStreamName);
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();

        IndicesAliasesRequest aliasesAddRequest = new IndicesAliasesRequest();
        aliasesAddRequest.addAliasAction(new AliasActions(AliasActions.Type.ADD).index("metrics-foo").aliases("my-alias1"));
        aliasesAddRequest.addAliasAction(new AliasActions(AliasActions.Type.ADD).index("metrics-myindex").aliases("my-alias2"));
        assertAcked(client().admin().indices().aliases(aliasesAddRequest).actionGet());
        GetAliasesResponse response = client().admin().indices().getAliases(new GetAliasesRequest()).actionGet();
        assertThat(
            response.getDataStreamAliases(),
            equalTo(Map.of("metrics-foo", List.of(new DataStreamAlias("my-alias1", List.of("metrics-foo"), null, null))))
        );
        assertThat(response.getAliases().get("metrics-myindex"), equalTo(List.of(new AliasMetadata.Builder("my-alias2").build())));

        aliasesAddRequest = new IndicesAliasesRequest();
        if (randomBoolean()) {
            aliasesAddRequest.addAliasAction(new AliasActions(AliasActions.Type.REMOVE).index("_all").aliases("my-alias1"));
            aliasesAddRequest.addAliasAction(new AliasActions(AliasActions.Type.REMOVE).index("_all").aliases("my-alias2"));
        } else {
            aliasesAddRequest.addAliasAction(new AliasActions(AliasActions.Type.REMOVE).index("_all").aliases("my-*"));
        }
        assertAcked(client().admin().indices().aliases(aliasesAddRequest).actionGet());
        response = client().admin().indices().getAliases(new GetAliasesRequest()).actionGet();
        assertThat(response.getDataStreamAliases(), anEmptyMap());
        assertThat(response.getAliases().get("metrics-myindex").size(), equalTo(0));
        assertThat(response.getAliases().size(), equalTo(1));
    }

    public void testUpdateDataStreamsWithWildcards() throws Exception {
        putComposableIndexTemplate("id1", List.of("metrics-*"));
        String dataStreamName = "metrics-foo";
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(dataStreamName);
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();
        {
            IndicesAliasesRequest aliasesAddRequest = new IndicesAliasesRequest();
            aliasesAddRequest.addAliasAction(
                new AliasActions(AliasActions.Type.ADD).index("metrics-foo").aliases("my-alias1", "my-alias2")
            );
            assertAcked(client().admin().indices().aliases(aliasesAddRequest).actionGet());
            GetAliasesResponse response = client().admin().indices().getAliases(new GetAliasesRequest()).actionGet();
            assertThat(response.getDataStreamAliases().keySet(), containsInAnyOrder("metrics-foo"));
            assertThat(
                response.getDataStreamAliases().get("metrics-foo"),
                containsInAnyOrder(
                    new DataStreamAlias("my-alias1", List.of("metrics-foo"), null, null),
                    new DataStreamAlias("my-alias2", List.of("metrics-foo"), null, null)
                )
            );
            assertThat(response.getAliases().size(), equalTo(0));
        }
        // ADD doesn't resolve wildcards:
        {
            IndicesAliasesRequest aliasesAddRequest = new IndicesAliasesRequest();
            aliasesAddRequest.addAliasAction(new AliasActions(AliasActions.Type.ADD).index("metrics-foo").aliases("my-alias*"));
            expectThrows(InvalidAliasNameException.class, () -> client().admin().indices().aliases(aliasesAddRequest).actionGet());
        }
        // REMOVE does resolve wildcards:
        {
            IndicesAliasesRequest aliasesAddRequest = new IndicesAliasesRequest();
            if (randomBoolean()) {
                aliasesAddRequest.addAliasAction(new AliasActions(AliasActions.Type.REMOVE).index("metrics-*").aliases("my-*"));
            } else {
                aliasesAddRequest.addAliasAction(new AliasActions(AliasActions.Type.REMOVE).index("_all").aliases("_all"));
            }
            assertAcked(client().admin().indices().aliases(aliasesAddRequest).actionGet());
            GetAliasesResponse response = client().admin().indices().getAliases(new GetAliasesRequest()).actionGet();
            assertThat(response.getDataStreamAliases(), anEmptyMap());
            assertThat(response.getAliases().size(), equalTo(0));
        }
    }

    public void testDataStreamAliasesUnsupportedParametersValidation() throws Exception {
        putComposableIndexTemplate("id1", List.of("metrics-*"));
        String dataStreamName = "metrics-foo";
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(dataStreamName);
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();
        {
            AliasActions addAction = new AliasActions(AliasActions.Type.ADD).index("metrics-*").aliases("my-alias").routing("[routing]");
            IndicesAliasesRequest aliasesAddRequest = new IndicesAliasesRequest();
            aliasesAddRequest.addAliasAction(addAction);
            Exception e = expectThrows(
                IllegalArgumentException.class,
                () -> client().admin().indices().aliases(aliasesAddRequest).actionGet()
            );
            assertThat(e.getMessage(), equalTo("aliases that point to data streams don't support routing"));
        }
        {
            AliasActions addAction = new AliasActions(AliasActions.Type.ADD).index("metrics-*")
                .aliases("my-alias")
                .indexRouting("[index_routing]");
            IndicesAliasesRequest aliasesAddRequest = new IndicesAliasesRequest();
            aliasesAddRequest.addAliasAction(addAction);
            Exception e = expectThrows(
                IllegalArgumentException.class,
                () -> client().admin().indices().aliases(aliasesAddRequest).actionGet()
            );
            assertThat(e.getMessage(), equalTo("aliases that point to data streams don't support index_routing"));
        }
        {
            AliasActions addAction = new AliasActions(AliasActions.Type.ADD).index("metrics-*")
                .aliases("my-alias")
                .searchRouting("[search_routing]");
            IndicesAliasesRequest aliasesAddRequest = new IndicesAliasesRequest();
            aliasesAddRequest.addAliasAction(addAction);
            Exception e = expectThrows(
                IllegalArgumentException.class,
                () -> client().admin().indices().aliases(aliasesAddRequest).actionGet()
            );
            assertThat(e.getMessage(), equalTo("aliases that point to data streams don't support search_routing"));
        }
        {
            AliasActions addAction = new AliasActions(AliasActions.Type.ADD).index("metrics-*")
                .aliases("my-alias")
                .isHidden(randomBoolean());
            IndicesAliasesRequest aliasesAddRequest = new IndicesAliasesRequest();
            aliasesAddRequest.addAliasAction(addAction);
            Exception e = expectThrows(
                IllegalArgumentException.class,
                () -> client().admin().indices().aliases(aliasesAddRequest).actionGet()
            );
            assertThat(e.getMessage(), equalTo("aliases that point to data streams don't support is_hidden"));
        }
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
        putComposableIndexTemplate("id1", mapping, List.of("logs-foo*"), null, null);

        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request("logs-foobar");
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();
        GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(new String[] { "logs-foobar" });
        GetDataStreamAction.Response getDataStreamResponse = client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest)
            .actionGet();
        assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(1));
        assertThat(getDataStreamResponse.getDataStreams().get(0).getDataStream().getName(), equalTo("logs-foobar"));
        assertThat(getDataStreamResponse.getDataStreams().get(0).getDataStream().getTimeStampField().getName(), equalTo("@timestamp"));
        java.util.Map<?, ?> expectedTimestampMapping = Map.of("type", "date", "format", "yyyy-MM", "meta", Map.of("x", "y"));
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

        java.util.Map<?, ?> expectedMapping = Map.of(
            "properties",
            Map.of("@timestamp", Map.of("type", "date")),
            "_data_stream_timestamp",
            Map.of("enabled", true)
        );
        GetMappingsResponse getMappingsResponse = client().admin().indices().prepareGetMappings("logs-foobar").get();
        assertThat(getMappingsResponse.getMappings().size(), equalTo(2));
        assertThat(getMappingsResponse.getMappings().get(backingIndex1).get("_doc").getSourceAsMap(), equalTo(expectedMapping));
        assertThat(getMappingsResponse.getMappings().get(backingIndex2).get("_doc").getSourceAsMap(), equalTo(expectedMapping));

        expectedMapping = Map.of(
            "properties",
            Map.of("@timestamp", Map.of("type", "date"), "my_field", Map.of("type", "keyword")),
            "_data_stream_timestamp",
            Map.of("enabled", true)
        );
        client().admin()
            .indices()
            .preparePutMapping("logs-foobar")
            .setType("_doc")
            .setSource("{\"properties\":{\"my_field\":{\"type\":\"keyword\"}}}", XContentType.JSON)
            .get();
        // The mappings of all backing indices should be updated:
        getMappingsResponse = client().admin().indices().prepareGetMappings("logs-foobar").get();
        assertThat(getMappingsResponse.getMappings().size(), equalTo(2));
        assertThat(getMappingsResponse.getMappings().get(backingIndex1).get("_doc").getSourceAsMap(), equalTo(expectedMapping));
        assertThat(getMappingsResponse.getMappings().get(backingIndex2).get("_doc").getSourceAsMap(), equalTo(expectedMapping));
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
        assertThat(indexResponse.getIndex(), backingIndexEqualTo(dataStream, 1));

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
        assertThat(indexResponse.getIndex(), backingIndexEqualTo("logs-foobar", 1));

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
        assertThat(rolloverResponse.getNewIndex(), backingIndexEqualTo("metrics-foo", 2));

        // ingest some more data in the rolled data stream
        int numDocsRolledFoo = randomIntBetween(2, 16);
        indexDocs("metrics-foo", numDocsRolledFoo);

        SearchRequest searchRequest = new SearchRequest("*");
        SearchResponse searchResponse = client().search(searchRequest).actionGet();
        assertThat(searchResponse.getHits().getTotalHits().value, is((long) numDocsBar + numDocsFoo + numDocsRolledFoo));
    }

    public void testGetDataStream() throws Exception {
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, maximumNumberOfReplicas() + 2).build();
        putComposableIndexTemplate("template_for_foo", null, List.of("metrics-foo*"), settings, null);

        int numDocsFoo = randomIntBetween(2, 16);
        indexDocs("metrics-foo", numDocsFoo);

        GetDataStreamAction.Response response = client().execute(
            GetDataStreamAction.INSTANCE,
            new GetDataStreamAction.Request(new String[] { "metrics-foo" })
        ).actionGet();
        assertThat(response.getDataStreams().size(), is(1));
        DataStreamInfo metricsFooDataStream = response.getDataStreams().get(0);
        assertThat(metricsFooDataStream.getDataStream().getName(), is("metrics-foo"));
        assertThat(metricsFooDataStream.getDataStreamStatus(), is(ClusterHealthStatus.YELLOW));
        assertThat(metricsFooDataStream.getIndexTemplate(), is("template_for_foo"));
        assertThat(metricsFooDataStream.getIlmPolicy(), is(nullValue()));
    }

    private static void assertBackingIndex(String backingIndex, String timestampFieldPathInMapping, java.util.Map<?, ?> expectedMapping) {
        GetIndexResponse getIndexResponse = client().admin().indices().getIndex(new GetIndexRequest().indices(backingIndex)).actionGet();
        assertThat(getIndexResponse.getSettings().get(backingIndex), notNullValue());
        assertThat(getIndexResponse.getSettings().get(backingIndex).getAsBoolean("index.hidden", null), is(true));
        java.util.Map<?, ?> mappings = getIndexResponse.getMappings().get(backingIndex).get("_doc").getSourceAsMap();
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
                new ComposableIndexTemplate.DataStreamTemplate(),
                null
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

    public void testCreatingDataStreamAndFirstBackingIndexExistsFails() throws Exception {
        String dataStreamName = "logs-foobar";
        long now = System.currentTimeMillis();
        String backingIndex = DataStream.getDefaultBackingIndexName(dataStreamName, 1, now);

        createIndex(backingIndex);
        putComposableIndexTemplate("id", List.of("logs-*"));
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(dataStreamName, now);
        Exception e = expectThrows(
            ElasticsearchStatusException.class,
            () -> client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).actionGet()
        );
        assertThat(e.getMessage(), equalTo("data stream could not be created because backing index [" + backingIndex + "] already exists"));
    }

    public void testQueryDataStreamNameInIndexField() throws Exception {
        putComposableIndexTemplate("id1", List.of("metrics-*"));
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request("metrics-foo");
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();

        indexDocs("metrics-foo", 1);
        indexDocs("metrics-bar", 1);

        SearchRequest searchRequest = new SearchRequest("*");
        searchRequest.source().query(new TermQueryBuilder("_index", "metrics-foo"));
        SearchResponse searchResponse = client().search(searchRequest).actionGet();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        assertThat(searchResponse.getHits().getTotalHits().relation, equalTo(TotalHits.Relation.EQUAL_TO));
    }

    public void testDataStreamMetadata() throws Exception {
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build();
        putComposableIndexTemplate("id1", null, List.of("logs-*"), settings, Map.of("managed_by", "core-features"));
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request("logs-foobar");
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();

        GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(new String[] { "*" });
        GetDataStreamAction.Response getDataStreamResponse = client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest)
            .actionGet();
        getDataStreamResponse.getDataStreams().sort(Comparator.comparing(dataStreamInfo -> dataStreamInfo.getDataStream().getName()));
        assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(1));
        DataStreamInfo info = getDataStreamResponse.getDataStreams().get(0);
        assertThat(info.getIndexTemplate(), equalTo("id1"));
        assertThat(info.getDataStreamStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(info.getIlmPolicy(), nullValue());
        DataStream dataStream = info.getDataStream();
        assertThat(dataStream.getName(), equalTo("logs-foobar"));
        assertThat(dataStream.getTimeStampField().getName(), equalTo("@timestamp"));
        assertThat(dataStream.getIndices().size(), equalTo(1));
        assertThat(dataStream.getIndices().get(0).getName(), backingIndexEqualTo("logs-foobar", 1));
        assertThat(dataStream.getMetadata(), equalTo(Map.of("managed_by", "core-features")));
    }

    public void testClusterStateIncludeDataStream() throws Exception {
        putComposableIndexTemplate("id1", List.of("metrics-foo*"));
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request("metrics-foo");
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();

        // when querying a backing index then the data stream should be included as well.
        ClusterStateRequest request = new ClusterStateRequest().indices(".ds-metrics-foo-*000001");
        ClusterState state = client().admin().cluster().state(request).get().getState();
        assertThat(state.metadata().dataStreams().size(), equalTo(1));
        assertThat(state.metadata().dataStreams().get("metrics-foo").getName(), equalTo("metrics-foo"));
    }

    /**
     * Tests that multiple threads all racing to rollover based on a condition trigger one and only one rollover
     */
    public void testMultiThreadedRollover() throws Exception {
        final String dsName = "potato-biscuit";
        putComposableIndexTemplate("id1", List.of("potato-*"));

        ensureGreen();

        final int threadCount = randomIntBetween(5, 10);
        final CyclicBarrier barrier = new CyclicBarrier(threadCount + 1);
        final AtomicBoolean running = new AtomicBoolean(true);
        Set<Thread> threads = IntStream.range(0, threadCount).mapToObj(i -> new Thread(() -> {
            try {
                logger.info("--> [{}] waiting for all the other threads before starting", i);
                barrier.await();
                while (running.get()) {
                    RolloverResponse resp = client().admin().indices().prepareRolloverIndex(dsName).addMaxIndexDocsCondition(2).get();
                    if (resp.isRolledOver()) {
                        logger.info("--> thread [{}] successfully rolled over: {}", i, Strings.toString(resp));
                        assertThat(resp.getOldIndex(), backingIndexEqualTo("potato-biscuit", 1));
                        assertThat(resp.getNewIndex(), backingIndexEqualTo("potato-biscuit", 2));
                    }
                }
            } catch (Exception e) {
                logger.error(new ParameterizedMessage("thread [{}] encountered unexpected exception", i), e);
                fail("we should not encounter unexpected exceptions");
            }
        }, "rollover-thread-" + i)).collect(Collectors.toSet());

        threads.forEach(Thread::start);

        indexDocs(dsName, 1);

        // Okay, signal the floodgates to open
        barrier.await();

        indexDocs(dsName, 1);

        assertBusy(() -> {
            try {
                client().admin().indices().prepareGetIndex().addIndices(DataStream.getDefaultBackingIndexName("potato-biscuit", 2)).get();
            } catch (Exception e) {
                logger.info("--> expecting second index to be created but it has not yet been created");
                fail("expecting second index to exist");
            }
        });

        // Tell everyone to stop trying to roll over
        running.set(false);

        threads.forEach(thread -> {
            try {
                thread.join(1000);
            } catch (Exception e) {
                logger.warn("expected thread to be stopped, but got", e);
            }
        });

        // We should *NOT* have a third index, it should have rolled over *exactly* once
        expectThrows(
            Exception.class,
            () -> client().admin().indices().prepareGetIndex().addIndices(DataStream.getDefaultBackingIndexName("potato-biscuit", 3)).get()
        );
    }

    // Test that datastream's segments by default are sorted on @timestamp desc
    public void testSegmentsSortedOnTimestampDesc() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        putComposableIndexTemplate("template_for_foo", null, List.of("metrics-foo*"), settings, null);
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request("metrics-foo");
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();

        // We index data in the increasing order of @timestamp field
        int numDocs1 = randomIntBetween(2, 10);
        indexDocs("metrics-foo", numDocs1); // 1st segment
        int numDocs2 = randomIntBetween(2, 10);
        indexDocs("metrics-foo", numDocs2); // 2nd segment
        int numDocs3 = randomIntBetween(2, 10);
        indexDocs("metrics-foo", numDocs3); // 3rd segment
        int totalDocs = numDocs1 + numDocs2 + numDocs3;

        SearchSourceBuilder source = new SearchSourceBuilder();
        source.fetchField(new FieldAndFormat(DEFAULT_TIMESTAMP_FIELD, "epoch_millis"));
        source.size(totalDocs);
        SearchRequest searchRequest = new SearchRequest(new String[] { "metrics-foo" }, source);
        SearchResponse searchResponse = client().search(searchRequest).actionGet();
        assertEquals(totalDocs, searchResponse.getHits().getTotalHits().value);
        SearchHit[] hits = searchResponse.getHits().getHits();
        assertEquals(totalDocs, hits.length);

        // Test that when we read data, segments come in the reverse order with a segment with the latest date first
        long timestamp1 = Long.valueOf(hits[0].field(DEFAULT_TIMESTAMP_FIELD).getValue()); // 1st doc of 1st seg
        long timestamp2 = Long.valueOf(hits[0 + numDocs3].field(DEFAULT_TIMESTAMP_FIELD).getValue()); // 1st doc of the 2nd seg
        long timestamp3 = Long.valueOf(hits[0 + numDocs3 + numDocs2].field(DEFAULT_TIMESTAMP_FIELD).getValue());  // 1st doc of the 3rd seg
        assertTrue(timestamp1 > timestamp2);
        assertTrue(timestamp2 > timestamp3);
    }

    public void testCreateDataStreamWithSameNameAsIndexAlias() throws Exception {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("my-index").alias(new Alias("my-alias"));
        assertAcked(client().admin().indices().create(createIndexRequest).actionGet());

        // Important detail: create template with data stream template after the index has been created
        DataStreamIT.putComposableIndexTemplate("my-template", List.of("my-*"));

        CreateDataStreamAction.Request request = new CreateDataStreamAction.Request("my-alias");
        Exception e = expectThrows(
            IllegalStateException.class,
            () -> client().execute(CreateDataStreamAction.INSTANCE, request).actionGet()
        );
        assertThat(e.getMessage(), containsString("[my-alias (alias of ["));
        assertThat(e.getMessage(), containsString("]) conflicts with data stream"));
    }

    public void testCreateDataStreamWithSameNameAsIndex() throws Exception {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("my-index").alias(new Alias("my-alias"));
        assertAcked(client().admin().indices().create(createIndexRequest).actionGet());

        // Important detail: create template with data stream template after the index has been created
        DataStreamIT.putComposableIndexTemplate("my-template", List.of("my-*"));

        CreateDataStreamAction.Request request = new CreateDataStreamAction.Request("my-index");
        Exception e = expectThrows(
            IllegalStateException.class,
            () -> client().execute(CreateDataStreamAction.INSTANCE, request).actionGet()
        );
        assertThat(e.getMessage(), containsString("data stream [my-index] conflicts with index"));
    }

    public void testCreateDataStreamWithSameNameAsDataStreamAlias() throws Exception {
        {
            DataStreamIT.putComposableIndexTemplate("my-template", List.of("my-*"));
            CreateDataStreamAction.Request request = new CreateDataStreamAction.Request("my-ds");
            assertAcked(client().execute(CreateDataStreamAction.INSTANCE, request).actionGet());
            IndicesAliasesRequest aliasesAddRequest = new IndicesAliasesRequest();
            aliasesAddRequest.addAliasAction(new AliasActions(AliasActions.Type.ADD).index("my-ds").aliases("my-alias"));
            assertAcked(client().admin().indices().aliases(aliasesAddRequest).actionGet());

            CreateDataStreamAction.Request request2 = new CreateDataStreamAction.Request("my-alias");
            Exception e = expectThrows(
                IllegalStateException.class,
                () -> client().execute(CreateDataStreamAction.INSTANCE, request2).actionGet()
            );
            assertThat(e.getMessage(), containsString("data stream alias and data stream have the same name (my-alias)"));
        }
        {
            assertAcked(client().execute(DeleteDataStreamAction.INSTANCE, new DeleteDataStreamAction.Request("*")).actionGet());
            DataStreamIT.putComposableIndexTemplate(
                "my-template",
                null,
                List.of("my-*"),
                null,
                null,
                Map.of("my-alias", AliasMetadata.builder("my-alias").build())
            );
            CreateDataStreamAction.Request request = new CreateDataStreamAction.Request("my-ds");
            assertAcked(client().execute(CreateDataStreamAction.INSTANCE, request).actionGet());

            CreateDataStreamAction.Request request2 = new CreateDataStreamAction.Request("my-alias");
            Exception e = expectThrows(
                IllegalStateException.class,
                () -> client().execute(CreateDataStreamAction.INSTANCE, request2).actionGet()
            );
            assertThat(e.getMessage(), containsString("data stream alias and data stream have the same name (my-alias)"));
        }
    }

    public void testCreateDataStreamAliasWithSameNameAsIndexAlias() throws Exception {
        {
            DataStreamIT.putComposableIndexTemplate("my-template", List.of("logs-*"));
            CreateIndexRequest createIndexRequest = new CreateIndexRequest("es-logs").alias(new Alias("logs"));
            assertAcked(client().admin().indices().create(createIndexRequest).actionGet());

            CreateDataStreamAction.Request request = new CreateDataStreamAction.Request("logs-es");
            assertAcked(client().execute(CreateDataStreamAction.INSTANCE, request).actionGet());
            IndicesAliasesRequest aliasesAddRequest = new IndicesAliasesRequest();
            aliasesAddRequest.addAliasAction(new AliasActions(AliasActions.Type.ADD).index("logs-es").aliases("logs"));
            Exception e = expectThrows(
                IllegalStateException.class,
                () -> client().admin().indices().aliases(aliasesAddRequest).actionGet()
            );
            assertThat(e.getMessage(), containsString("data stream alias and indices alias have the same name (logs)"));
        }
        {
            assertAcked(client().execute(DeleteDataStreamAction.INSTANCE, new DeleteDataStreamAction.Request("*")).actionGet());
            DataStreamIT.putComposableIndexTemplate(
                "my-template",
                null,
                List.of("logs-*"),
                null,
                null,
                Map.of("logs", AliasMetadata.builder("logs").build())
            );

            CreateDataStreamAction.Request request = new CreateDataStreamAction.Request("logs-es");
            Exception e = expectThrows(
                IllegalStateException.class,
                () -> client().execute(CreateDataStreamAction.INSTANCE, request).actionGet()
            );
            assertThat(e.getMessage(), containsString("data stream alias and indices alias have the same name (logs)"));
        }
    }

    public void testCreateDataStreamAliasWithSameNameAsIndex() throws Exception {
        DataStreamIT.putComposableIndexTemplate("my-template", List.of("logs-*"));

        CreateIndexRequest createIndexRequest = new CreateIndexRequest("logs");
        assertAcked(client().admin().indices().create(createIndexRequest).actionGet());

        {
            CreateDataStreamAction.Request request = new CreateDataStreamAction.Request("logs-es");
            assertAcked(client().execute(CreateDataStreamAction.INSTANCE, request).actionGet());
            IndicesAliasesRequest aliasesAddRequest = new IndicesAliasesRequest();
            aliasesAddRequest.addAliasAction(new AliasActions(AliasActions.Type.ADD).index("logs-es").aliases("logs"));
            Exception e = expectThrows(
                InvalidAliasNameException.class,
                () -> client().admin().indices().aliases(aliasesAddRequest).actionGet()
            );
            assertThat(
                e.getMessage(),
                equalTo("Invalid alias name [logs]: an index or data stream exists with the same name as the alias")
            );
        }
        {
            assertAcked(client().execute(DeleteDataStreamAction.INSTANCE, new DeleteDataStreamAction.Request("*")).actionGet());
            Exception e = expectThrows(
                IllegalArgumentException.class,
                () -> DataStreamIT.putComposableIndexTemplate(
                    "my-template",
                    null,
                    List.of("logs-*"),
                    null,
                    null,
                    Map.of("logs", AliasMetadata.builder("logs").build())
                )
            );
            assertThat(
                e.getCause().getMessage(),
                equalTo("Invalid alias name [logs]: an index or data stream exists with the same name as the alias")
            );
        }
    }

    public void testCreateIndexWithSameNameAsDataStreamAlias() throws Exception {
        DataStreamIT.putComposableIndexTemplate("my-template", List.of("logs-*"));

        CreateDataStreamAction.Request request = new CreateDataStreamAction.Request("logs-es");
        assertAcked(client().execute(CreateDataStreamAction.INSTANCE, request).actionGet());
        IndicesAliasesRequest aliasesAddRequest = new IndicesAliasesRequest();
        aliasesAddRequest.addAliasAction(new AliasActions(AliasActions.Type.ADD).index("logs-es").aliases("logs"));
        assertAcked(client().admin().indices().aliases(aliasesAddRequest).actionGet());

        CreateIndexRequest createIndexRequest = new CreateIndexRequest("logs");
        Exception e = expectThrows(
            InvalidIndexNameException.class,
            () -> client().admin().indices().create(createIndexRequest).actionGet()
        );
        assertThat(e.getMessage(), equalTo("Invalid index name [logs], already exists as alias"));
    }

    public void testCreateIndexAliasWithSameNameAsDataStreamAlias() throws Exception {
        DataStreamIT.putComposableIndexTemplate("my-template", List.of("logs-*"));

        CreateDataStreamAction.Request request = new CreateDataStreamAction.Request("logs-es");
        assertAcked(client().execute(CreateDataStreamAction.INSTANCE, request).actionGet());
        IndicesAliasesRequest aliasesAddRequest = new IndicesAliasesRequest();
        aliasesAddRequest.addAliasAction(new AliasActions(AliasActions.Type.ADD).index("logs-es").aliases("logs"));
        assertAcked(client().admin().indices().aliases(aliasesAddRequest).actionGet());

        {
            CreateIndexRequest createIndexRequest = new CreateIndexRequest("my-index").alias(new Alias("logs"));
            Exception e = expectThrows(
                IllegalStateException.class,
                () -> client().admin().indices().create(createIndexRequest).actionGet()
            );
            assertThat(e.getMessage(), containsString("data stream alias and indices alias have the same name (logs)"));
        }
        {
            CreateIndexRequest createIndexRequest = new CreateIndexRequest("my-index");
            assertAcked(client().admin().indices().create(createIndexRequest).actionGet());
            IndicesAliasesRequest addAliasRequest = new IndicesAliasesRequest();
            addAliasRequest.addAliasAction(new AliasActions(AliasActions.Type.ADD).index("my-index").aliases("logs"));
            Exception e = expectThrows(IllegalStateException.class, () -> client().admin().indices().aliases(addAliasRequest).actionGet());
            assertThat(e.getMessage(), containsString("data stream alias and indices alias have the same name (logs)"));
        }
    }

    public void testRemoveGhostReference() throws Exception {
        String dataStreamName = "logs-es";
        DataStreamIT.putComposableIndexTemplate("my-template", List.of("logs-*"));
        CreateDataStreamAction.Request request = new CreateDataStreamAction.Request(dataStreamName);
        assertAcked(client().execute(CreateDataStreamAction.INSTANCE, request).actionGet());
        assertAcked(client().admin().indices().rolloverIndex(new RolloverRequest(dataStreamName, null)).actionGet());
        IndicesStatsResponse indicesStatsResponse = client().admin().indices().stats(new IndicesStatsRequest()).actionGet();
        assertThat(indicesStatsResponse.getIndices().size(), equalTo(2));
        ClusterState before = internalCluster().getCurrentMasterNodeInstance(ClusterService.class).state();
        assertThat(before.getMetadata().dataStreams().get(dataStreamName).getIndices(), hasSize(2));

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<DataStream> brokenDataStreamHolder = new AtomicReference<>();
        internalCluster().getCurrentMasterNodeInstance(ClusterService.class)
            .submitStateUpdateTask(getTestName(), new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    DataStream original = currentState.getMetadata().dataStreams().get(dataStreamName);
                    DataStream broken = new DataStream(
                        original.getName(),
                        new TimestampField("@timestamp"),
                        List.of(new Index(original.getIndices().get(0).getName(), "broken"), original.getIndices().get(1)),
                        original.getGeneration(),
                        original.getMetadata(),
                        original.isHidden(),
                        original.isReplicated(),
                        original.isSystem()
                    );
                    brokenDataStreamHolder.set(broken);
                    return ClusterState.builder(currentState)
                        .metadata(Metadata.builder(currentState.getMetadata()).put(broken).build())
                        .build();
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    latch.countDown();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.error("error while adding a broken data stream", e);
                    latch.countDown();
                }
            });
        latch.await();
        Index ghostReference = brokenDataStreamHolder.get().getIndices().get(0);

        // Many APIs fail with NPE, because of broken data stream:
        expectThrows(NullPointerException.class, () -> client().admin().indices().stats(new IndicesStatsRequest()).actionGet());
        expectThrows(NullPointerException.class, () -> client().search(new SearchRequest()).actionGet());

        assertAcked(
            client().execute(
                ModifyDataStreamsAction.INSTANCE,
                new ModifyDataStreamsAction.Request(List.of(DataStreamAction.removeBackingIndex(dataStreamName, ghostReference.getName())))
            ).actionGet()
        );
        ClusterState after = internalCluster().getCurrentMasterNodeInstance(ClusterService.class).state();
        assertThat(after.getMetadata().dataStreams().get(dataStreamName).getIndices(), hasSize(1));
        // Data stream resolves now to one backing index.
        // Note, that old backing index still exists and has been unhidden.
        // The modify data stream api only fixed the data stream by removing a broken reference to a backing index.
        indicesStatsResponse = client().admin().indices().stats(new IndicesStatsRequest()).actionGet();
        assertThat(indicesStatsResponse.getIndices().size(), equalTo(2));
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

    static void verifyDocs(String dataStream, long expectedNumHits, java.util.List<String> expectedIndices) {
        SearchRequest searchRequest = new SearchRequest(dataStream);
        searchRequest.source().size((int) expectedNumHits);
        SearchResponse searchResponse = client().search(searchRequest).actionGet();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(expectedNumHits));

        Arrays.stream(searchResponse.getHits().getHits()).forEach(hit -> { assertTrue(expectedIndices.contains(hit.getIndex())); });
    }

    static void verifyDocs(String dataStream, long expectedNumHits, long minGeneration, long maxGeneration) {
        java.util.List<String> expectedIndices = new ArrayList<>();
        for (long k = minGeneration; k <= maxGeneration; k++) {
            expectedIndices.add(DataStream.getDefaultBackingIndexName(dataStream, k));
        }
        verifyDocs(dataStream, expectedNumHits, expectedIndices);
    }

    public static void putComposableIndexTemplate(String id, java.util.List<String> patterns) throws IOException {
        putComposableIndexTemplate(id, null, patterns, null, null);
    }

    static void putComposableIndexTemplate(
        String id,
        @Nullable String mappings,
        java.util.List<String> patterns,
        @Nullable Settings settings,
        @Nullable java.util.Map<String, Object> metadata
    ) throws IOException {
        putComposableIndexTemplate(id, mappings, patterns, settings, metadata, null);
    }

    static void putComposableIndexTemplate(
        String id,
        @Nullable String mappings,
        java.util.List<String> patterns,
        @Nullable Settings settings,
        @Nullable java.util.Map<String, Object> metadata,
        @Nullable java.util.Map<String, AliasMetadata> aliases
    ) throws IOException {
        PutComposableIndexTemplateAction.Request request = new PutComposableIndexTemplateAction.Request(id);
        request.indexTemplate(
            new ComposableIndexTemplate(
                patterns,
                new Template(settings, mappings == null ? null : new CompressedXContent(mappings), aliases),
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
