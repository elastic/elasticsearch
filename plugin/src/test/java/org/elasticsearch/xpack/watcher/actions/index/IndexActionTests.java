/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.actions.index;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.security.InternalClient;
import org.elasticsearch.xpack.watcher.actions.Action;
import org.elasticsearch.xpack.watcher.actions.Action.Result.Status;
import org.elasticsearch.xpack.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.watcher.support.WatcherDateTimeUtils;
import org.elasticsearch.xpack.watcher.support.init.proxy.WatcherClientProxy;
import org.elasticsearch.xpack.watcher.support.xcontent.XContentSource;
import org.elasticsearch.xpack.watcher.test.WatcherTestUtils;
import org.elasticsearch.xpack.watcher.watch.Payload;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static java.util.Collections.unmodifiableSet;
import static org.elasticsearch.common.util.set.Sets.newHashSet;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.joda.time.DateTimeZone.UTC;

public class IndexActionTests extends ESIntegTestCase {

    public void testIndexActionExecuteSingleDoc() throws Exception {
        boolean customId = randomBoolean();
        boolean docIdAsParam = customId && randomBoolean();
        String docId = randomAlphaOfLength(5);
        String timestampField = randomFrom("@timestamp", null);
        boolean customTimestampField = timestampField != null;

        IndexAction action = new IndexAction("test-index", "test-type", docIdAsParam ? docId : null, timestampField, null, null);
        ExecutableIndexAction executable = new ExecutableIndexAction(action, logger, WatcherClientProxy.of(client()), null);
        DateTime executionTime = DateTime.now(UTC);
        Payload payload;

        if (customId && docIdAsParam == false) {
            // intentionally immutable because the other side needs to cut out _id
            payload = new Payload.Simple("_doc", MapBuilder.newMapBuilder().put("foo", "bar").put("_id", docId).immutableMap());
        } else {
            payload = randomBoolean() ? new Payload.Simple("foo", "bar") : new Payload.Simple("_doc", singletonMap("foo", "bar"));
        }

        WatchExecutionContext ctx = WatcherTestUtils.mockExecutionContext("_id", executionTime, payload);

        Action.Result result = executable.execute("_id", ctx, ctx.payload());

        assertThat(result.status(), equalTo(Status.SUCCESS));
        assertThat(result, instanceOf(IndexAction.Result.class));
        IndexAction.Result successResult = (IndexAction.Result) result;
        XContentSource response = successResult.response();
        assertThat(response.getValue("created"), equalTo((Object)Boolean.TRUE));
        assertThat(response.getValue("version"), equalTo((Object) 1));
        assertThat(response.getValue("type").toString(), equalTo("test-type"));
        assertThat(response.getValue("index").toString(), equalTo("test-index"));

        refresh(); //Manually refresh to make sure data is available

        SearchRequestBuilder searchRequestbuilder = client().prepareSearch("test-index")
                .setTypes("test-type")
                .setSource(searchSource().query(matchAllQuery()));

        if (customTimestampField) {
            searchRequestbuilder.addAggregation(terms("timestamps").field(timestampField));
        }

        SearchResponse searchResponse = searchRequestbuilder.get();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1L));
        SearchHit hit = searchResponse.getHits().getAt(0);

        if (customId) {
            assertThat(hit.getId(), is(docId));
        }

        if (customTimestampField) {
            assertThat(hit.getSourceAsMap().size(), is(2));
            assertThat(hit.getSourceAsMap(), hasEntry("foo", (Object) "bar"));
            assertThat(hit.getSourceAsMap(), hasEntry(timestampField, (Object) WatcherDateTimeUtils.formatDate(executionTime)));

            Terms terms = searchResponse.getAggregations().get("timestamps");
            assertThat(terms, notNullValue());
            assertThat(terms.getBuckets(), hasSize(1));
            assertThat(terms.getBuckets().get(0).getKeyAsNumber().longValue(), is(executionTime.getMillis()));
            assertThat(terms.getBuckets().get(0).getDocCount(), is(1L));
        } else {
            assertThat(hit.getSourceAsMap().size(), is(1));
            assertThat(hit.getSourceAsMap(), hasEntry("foo", (Object) "bar"));
        }
    }

    public void testIndexActionExecuteMultiDoc() throws Exception {
        String timestampField = randomFrom("@timestamp", null);
        boolean customTimestampField = "@timestamp".equals(timestampField);

        assertAcked(prepareCreate("test-index")
                .addMapping("test-type", "foo", "type=keyword"));

        List<Map> idList = Arrays.asList(
                MapBuilder.newMapBuilder().put("foo", "bar").put("_id", "0").immutableMap(),
                MapBuilder.newMapBuilder().put("foo", "bar1").put("_id", "1").map()
        );

        Object list = randomFrom(
                new Map[] { singletonMap("foo", "bar"), singletonMap("foo", "bar1") },
                Arrays.asList(singletonMap("foo", "bar"), singletonMap("foo", "bar1")),
                unmodifiableSet(newHashSet(singletonMap("foo", "bar"), singletonMap("foo", "bar1"))),
                idList
        );

        boolean customId = list == idList;

        IndexAction action = new IndexAction("test-index", "test-type", null, timestampField, null, null);
        ExecutableIndexAction executable = new ExecutableIndexAction(action, logger, WatcherClientProxy.of(client()), null);
        DateTime executionTime = DateTime.now(UTC);
        WatchExecutionContext ctx = WatcherTestUtils.mockExecutionContext("watch_id", executionTime, new Payload.Simple("_doc", list));

        Action.Result result = executable.execute("watch_id", ctx, ctx.payload());

        assertThat(result.status(), equalTo(Status.SUCCESS));
        assertThat(result, instanceOf(IndexAction.Result.class));
        IndexAction.Result successResult = (IndexAction.Result) result;
        XContentSource response = successResult.response();
        assertThat(successResult.toString(), response.getValue("0.created"), equalTo((Object)Boolean.TRUE));
        assertThat(successResult.toString(), response.getValue("0.version"), equalTo((Object) 1));
        assertThat(successResult.toString(), response.getValue("0.type").toString(), equalTo("test-type"));
        assertThat(successResult.toString(), response.getValue("0.index").toString(), equalTo("test-index"));
        assertThat(successResult.toString(), response.getValue("1.created"), equalTo((Object)Boolean.TRUE));
        assertThat(successResult.toString(), response.getValue("1.version"), equalTo((Object) 1));
        assertThat(successResult.toString(), response.getValue("1.type").toString(), equalTo("test-type"));
        assertThat(successResult.toString(), response.getValue("1.index").toString(), equalTo("test-index"));

        refresh(); //Manually refresh to make sure data is available

        SearchResponse searchResponse = client().prepareSearch("test-index")
                .setTypes("test-type")
                .setSource(searchSource().sort("foo", SortOrder.ASC)
                        .query(matchAllQuery()))
                .get();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2L));
        final int fields = customTimestampField ? 2 : 1;
        for (int i = 0; i < 2; ++i) {
            final SearchHit hit = searchResponse.getHits().getAt(i);
            final String value = "bar" + (i != 0 ? i : "");

            assertThat(hit.getSourceAsMap().size(), is(fields));

            if (customId) {
                assertThat(hit.getId(), is(Integer.toString(i)));
            }
            if (customTimestampField) {
                assertThat(hit.getSourceAsMap(), hasEntry(timestampField, (Object) WatcherDateTimeUtils.formatDate(executionTime)));
            }
            assertThat(hit.getSourceAsMap(), hasEntry("foo", (Object) value));
        }
    }

    public void testParser() throws Exception {
        String timestampField = randomBoolean() ? "@timestamp" : null;
        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        builder.field(IndexAction.Field.INDEX.getPreferredName(), "test-index");
        builder.field(IndexAction.Field.DOC_TYPE.getPreferredName(), "test-type");
        if (timestampField != null) {
            builder.field(IndexAction.Field.EXECUTION_TIME_FIELD.getPreferredName(), timestampField);
        }
        TimeValue writeTimeout = randomBoolean() ? TimeValue.timeValueSeconds(randomInt(10)) : null;
        if (writeTimeout != null) {
            builder.field(IndexAction.Field.TIMEOUT.getPreferredName(), writeTimeout.millis());
        }
        builder.endObject();
        Client client = client();
        InternalClient internalClient = new InternalClient(client.settings(), client.threadPool(), client);

        IndexActionFactory actionParser = new IndexActionFactory(Settings.EMPTY, internalClient);
        XContentParser parser = createParser(builder);
        parser.nextToken();

        ExecutableIndexAction executable = actionParser.parseExecutable(randomAlphaOfLength(5), randomAlphaOfLength(3), parser);

        assertThat(executable.action().docType, equalTo("test-type"));
        assertThat(executable.action().index, equalTo("test-index"));
        if (timestampField != null) {
            assertThat(executable.action().executionTimeField, equalTo(timestampField));
        }
        assertThat(executable.action().timeout, equalTo(writeTimeout));
    }

    public void testParserFailure() throws Exception {
        XContentBuilder builder = jsonBuilder();
        boolean useIndex = randomBoolean();
        boolean useType = randomBoolean();
        builder.startObject();
        {
            if (useIndex) {
                builder.field(IndexAction.Field.INDEX.getPreferredName(), "test-index");
            }
            if (useType) {
                builder.field(IndexAction.Field.DOC_TYPE.getPreferredName(), "test-type");
            }
        }
        builder.endObject();
        Client client = client();
        InternalClient internalClient = new InternalClient(client.settings(), client.threadPool(), client);

        IndexActionFactory actionParser = new IndexActionFactory(Settings.EMPTY, internalClient);
        XContentParser parser = createParser(builder);
        parser.nextToken();
        try {
            actionParser.parseExecutable(randomAlphaOfLength(4), randomAlphaOfLength(5), parser);
            if (!(useIndex && useType)) {
                fail();
            }
        } catch (ElasticsearchParseException iae) {
            assertThat(useIndex && useType, equalTo(false));
        }
    }

    // https://github.com/elastic/x-pack/issues/4416
    public void testIndexingWithWrongMappingReturnsFailureResult() throws Exception {
        // index a document to set the mapping of the foo field to a boolean
        client().prepareIndex("test-index", "test-type", "_id").setSource("foo", true)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        IndexAction action = new IndexAction("test-index", "test-type", null, "@timestamp", null, null);
        ExecutableIndexAction executable = new ExecutableIndexAction(action, logger, WatcherClientProxy.of(client()), null);

        List<Map<String, Object>> docs = new ArrayList<>();
        boolean addSuccessfulIndexedDoc = randomBoolean();
        if (addSuccessfulIndexedDoc) {
            docs.add(Collections.singletonMap("foo", randomBoolean()));
        }
        docs.add(Collections.singletonMap("foo", Collections.singletonMap("foo", "bar")));
        Payload payload = new Payload.Simple(Collections.singletonMap("_doc", docs));

        WatchExecutionContext ctx = WatcherTestUtils.mockExecutionContext("_id", DateTime.now(UTC), payload);

        Action.Result result = executable.execute("_id", ctx, payload);
        if (addSuccessfulIndexedDoc) {
            assertThat(result.status(), is(Status.PARTIAL_FAILURE));
        } else {
            assertThat(result.status(), is(Status.FAILURE));
        }
    }

    public void testUsingParameterIdWithBulkOrIdFieldThrowsIllegalState() {
        final IndexAction action = new IndexAction("test-index", "test-type", "123", null, null, null);
        final ExecutableIndexAction executable = new ExecutableIndexAction(action, logger, WatcherClientProxy.of(client()), null);
        final Map<String, Object> docWithId = MapBuilder.<String, Object>newMapBuilder().put("foo", "bar").put("_id", "0").immutableMap();
        final DateTime executionTime = DateTime.now(UTC);

        // using doc_id with bulk fails regardless of using ID
        expectThrows(IllegalStateException.class, () -> {
            final List<Map> idList = Arrays.asList(docWithId, MapBuilder.newMapBuilder().put("foo", "bar1").put("_id", "1").map());

            final Object list = randomFrom(
                    new Map[] { singletonMap("foo", "bar"), singletonMap("foo", "bar1") },
                    Arrays.asList(singletonMap("foo", "bar"), singletonMap("foo", "bar1")),
                    unmodifiableSet(newHashSet(singletonMap("foo", "bar"), singletonMap("foo", "bar1"))),
                    idList
            );

            final WatchExecutionContext ctx = WatcherTestUtils.mockExecutionContext("_id", executionTime, new Payload.Simple("_doc", list));

            executable.execute("_id", ctx, ctx.payload());
        });

        // using doc_id with _id
        expectThrows(IllegalStateException.class, () -> {
            final Payload payload = randomBoolean() ? new Payload.Simple("_doc", docWithId) : new Payload.Simple(docWithId);
            final WatchExecutionContext ctx = WatcherTestUtils.mockExecutionContext("_id", executionTime, payload);

            executable.execute("_id", ctx, ctx.payload());
        });
    }
}
