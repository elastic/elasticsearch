/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.index;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.watcher.actions.Action;
import org.elasticsearch.watcher.actions.Action.Result.Status;
import org.elasticsearch.watcher.execution.WatchExecutionContext;
import org.elasticsearch.watcher.support.DynamicIndexName;
import org.elasticsearch.watcher.support.WatcherDateTimeUtils;
import org.elasticsearch.watcher.support.init.proxy.ClientProxy;
import org.elasticsearch.watcher.support.xcontent.XContentSource;
import org.elasticsearch.watcher.test.WatcherTestUtils;
import org.elasticsearch.watcher.watch.Payload;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.junit.Test;

import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.hamcrest.Matchers.*;
import static org.joda.time.DateTimeZone.UTC;

/**
 */
public class IndexActionTests extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .build();
    }

    @Override
    protected Settings transportClientSettings() {
        return Settings.builder()
                .put(super.transportClientSettings())
                .build();
    }

    @Test
    public void testIndexActionExecute_SingleDoc() throws Exception {

        String timestampField = randomFrom(null, "_timestamp", "@timestamp");
        boolean customTimestampField = "@timestamp".equals(timestampField);

        if (timestampField == null || "_timestamp".equals(timestampField)) {
            assertThat(prepareCreate("test-index")
                    .addMapping("test-type", "{ \"test-type\" : { \"_timestamp\" : { \"enabled\" : \"true\" }}}")
                    .get().isAcknowledged(), is(true));
        }

        IndexAction action = new IndexAction("test-index", "test-type", timestampField, null, null);
        ExecutableIndexAction executable = new ExecutableIndexAction(action, logger, ClientProxy.of(client()), null, new DynamicIndexName.Parser());
        DateTime executionTime = DateTime.now(UTC);
        Payload payload = randomBoolean() ? new Payload.Simple("foo", "bar") : new Payload.Simple("_doc", ImmutableMap.of("foo", "bar"));
        WatchExecutionContext ctx = WatcherTestUtils.mockExecutionContext("_id", executionTime, payload);

        Action.Result result = executable.execute("_id", ctx, ctx.payload());

        assertThat(result.status(), equalTo(Status.SUCCESS));
        assertThat(result, instanceOf(IndexAction.Result.Success.class));
        IndexAction.Result.Success successResult = (IndexAction.Result.Success) result;
        XContentSource response = successResult.response();
        assertThat(response.getValue("created"), equalTo((Object)Boolean.TRUE));
        assertThat(response.getValue("version"), equalTo((Object) 1));
        assertThat(response.getValue("type").toString(), equalTo("test-type"));
        assertThat(response.getValue("index").toString(), equalTo("test-index"));

        refresh(); //Manually refresh to make sure data is available

        SearchResponse searchResponse = client().prepareSearch("test-index")
                .setTypes("test-type")
                .setSource(searchSource()
                        .query(matchAllQuery())
                        .aggregation(terms("timestamps").field(customTimestampField ? timestampField : "_timestamp"))
                        .buildAsBytes())
                .get();

        assertThat(searchResponse.getHits().totalHits(), equalTo(1L));
        SearchHit hit = searchResponse.getHits().getAt(0);

        if (customTimestampField) {
            assertThat(hit.getSource().size(), is(2));
            assertThat(hit.getSource(), hasEntry("foo", (Object) "bar"));
            assertThat(hit.getSource(), hasEntry(timestampField, (Object) WatcherDateTimeUtils.formatDate(executionTime)));
        } else {
            assertThat(hit.getSource().size(), is(1));
            assertThat(hit.getSource(), hasEntry("foo", (Object) "bar"));
        }
        Terms terms = searchResponse.getAggregations().get("timestamps");
        assertThat(terms, notNullValue());
        assertThat(terms.getBuckets(), hasSize(1));
        assertThat(terms.getBuckets().get(0).getKeyAsNumber().longValue(), is(executionTime.getMillis()));
        assertThat(terms.getBuckets().get(0).getDocCount(), is(1L));
    }

    @Test
    public void testIndexActionExecute_MultiDoc() throws Exception {

        String timestampField = randomFrom(null, "_timestamp", "@timestamp");
        boolean customTimestampField = "@timestamp".equals(timestampField);

        if (timestampField == null || "_timestamp".equals(timestampField)) {
            assertThat(prepareCreate("test-index")
                    .addMapping("test-type", "{ \"test-type\" : { \"_timestamp\" : { \"enabled\" : \"true\" }}}")
                    .get().isAcknowledged(), is(true));
        }

        Object list = randomFrom(
                new Map[] { ImmutableMap.of("foo", "bar"), ImmutableMap.of("foo", "bar1") },
                ImmutableList.of(ImmutableMap.of("foo", "bar"), ImmutableMap.of("foo", "bar1")),
                ImmutableSet.of(ImmutableMap.of("foo", "bar"), ImmutableMap.of("foo", "bar1"))
        );

        IndexAction action = new IndexAction("test-index", "test-type", timestampField, null, null);
        ExecutableIndexAction executable = new ExecutableIndexAction(action, logger, ClientProxy.of(client()), null, new DynamicIndexName.Parser());
        DateTime executionTime = DateTime.now(UTC);
        WatchExecutionContext ctx = WatcherTestUtils.mockExecutionContext("_id", executionTime, new Payload.Simple("_doc", list));

        Action.Result result = executable.execute("_id", ctx, ctx.payload());

        assertThat(result.status(), equalTo(Status.SUCCESS));
        assertThat(result, instanceOf(IndexAction.Result.Success.class));
        IndexAction.Result.Success successResult = (IndexAction.Result.Success) result;
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
                .addSort("foo", SortOrder.ASC)
                .setSource(searchSource()
                        .query(matchAllQuery())
                        .aggregation(terms("timestamps").field(customTimestampField ? timestampField : "_timestamp"))
                        .buildAsBytes())
                .get();

        assertThat(searchResponse.getHits().totalHits(), equalTo(2L));
        SearchHit hit = searchResponse.getHits().getAt(0);
        if (customTimestampField) {
            assertThat(hit.getSource().size(), is(2));
            assertThat(hit.getSource(), hasEntry("foo", (Object) "bar"));
            assertThat(hit.getSource(), hasEntry(timestampField, (Object) WatcherDateTimeUtils.formatDate(executionTime)));
        } else {
            assertThat(hit.getSource().size(), is(1));
            assertThat(hit.getSource(), hasEntry("foo", (Object) "bar"));
        }
        hit = searchResponse.getHits().getAt(1);
        if (customTimestampField) {
            assertThat(hit.getSource().size(), is(2));
            assertThat(hit.getSource(), hasEntry("foo", (Object) "bar1"));
            assertThat(hit.getSource(), hasEntry(timestampField, (Object) WatcherDateTimeUtils.formatDate(executionTime)));
        } else {
            assertThat(hit.getSource().size(), is(1));
            assertThat(hit.getSource(), hasEntry("foo", (Object) "bar1"));
        }
    }

    @Test
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
            builder.field(IndexAction.Field.TIMEOUT.getPreferredName(), writeTimeout);
        }
        builder.endObject();

        IndexActionFactory actionParser = new IndexActionFactory(Settings.EMPTY, ClientProxy.of(client()));
        XContentParser parser = JsonXContent.jsonXContent.createParser(builder.bytes());
        parser.nextToken();

        ExecutableIndexAction executable = actionParser.parseExecutable(randomAsciiOfLength(5), randomAsciiOfLength(3), parser);

        assertThat(executable.action().docType, equalTo("test-type"));
        assertThat(executable.action().index, equalTo("test-index"));
        if (timestampField != null) {
            assertThat(executable.action().executionTimeField, equalTo(timestampField));
        }
        assertThat(executable.action().timeout, equalTo(writeTimeout));
    }

    @Test
    public void testParser_DynamicIndex() throws Exception {

        DateTime now = DateTime.now(UTC);
        DateTimeZone timeZone = randomBoolean() ? DateTimeZone.forOffsetHours(-2) : null;
        if (timeZone != null) {
            now = now.withHourOfDay(0).withMinuteOfHour(0);
        }

        XContentBuilder builder = jsonBuilder();
        builder.startObject()
                .field(IndexAction.Field.INDEX.getPreferredName(), "<idx-{now/d}>")
                .field(IndexAction.Field.DOC_TYPE.getPreferredName(), "test-type");

        boolean timeZoneInWatch = randomBoolean();
        if (timeZone != null && timeZoneInWatch) {
            builder.field(IndexAction.Field.DYNAMIC_NAME_TIMEZONE.getPreferredName(), timeZone);
        }

        builder.endObject();

        Settings.Builder settings = Settings.builder();
        if (timeZone != null && !timeZoneInWatch) {
            settings.put("watcher.actions.index.dynamic_indices.time_zone", timeZone);
        }

        IndexActionFactory actionParser = new IndexActionFactory(settings.build(), ClientProxy.of(client()));
        XContentParser parser = JsonXContent.jsonXContent.createParser(builder.bytes());
        parser.nextToken();

        ExecutableIndexAction executable = actionParser.parseExecutable(randomAsciiOfLength(5), randomAsciiOfLength(3), parser);

        assertThat(executable, notNullValue());
        assertThat(executable.action().index, is("<idx-{now/d}>"));
        String indexName = executable.indexName().name(now);
        if (timeZone != null) {
            now = now.withZone(timeZone);
        }
        assertThat(indexName, is("idx-" + DateTimeFormat.forPattern("YYYY.MM.dd").print(now)));
    }

    @Test
    public void testParser_Failure() throws Exception {
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
        IndexActionFactory actionParser = new IndexActionFactory(Settings.EMPTY, ClientProxy.of(client()));
        XContentParser parser = JsonXContent.jsonXContent.createParser(builder.bytes());
        parser.nextToken();
        try {
            actionParser.parseExecutable(randomAsciiOfLength(4), randomAsciiOfLength(5), parser);
            if (!(useIndex && useType)) {
                fail();
            }
        } catch (ElasticsearchParseException iae) {
            assertThat(useIndex && useType, equalTo(false));
        }
    }
}
