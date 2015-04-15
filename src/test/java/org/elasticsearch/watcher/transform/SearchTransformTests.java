/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transform;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.watcher.execution.WatchExecutionContext;
import org.elasticsearch.watcher.support.init.proxy.ClientProxy;
import org.elasticsearch.watcher.test.AbstractWatcherIntegrationTests;
import org.elasticsearch.watcher.transform.search.ExecutableSearchTransform;
import org.elasticsearch.watcher.transform.search.SearchTransform;
import org.elasticsearch.watcher.transform.search.SearchTransformFactory;
import org.elasticsearch.watcher.trigger.schedule.ScheduleTriggerEvent;
import org.elasticsearch.watcher.watch.Payload;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.joda.time.DateTimeZone.UTC;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.FilterBuilders.*;
import static org.elasticsearch.index.query.QueryBuilders.filteredQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.watcher.support.WatcherDateUtils.parseDate;
import static org.elasticsearch.watcher.test.WatcherTestUtils.*;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class SearchTransformTests extends AbstractWatcherIntegrationTests {

    @Test
    public void testApply() throws Exception {

        index("idx", "type", "1");
        ensureGreen("idx");
        refresh();

        SearchRequest request = Requests.searchRequest("idx").source(jsonBuilder().startObject()
                .startObject("query")
                .startObject("match_all").endObject()
                .endObject()
                .endObject());
        ExecutableSearchTransform transform = new ExecutableSearchTransform(new SearchTransform(request), logger, scriptService(), ClientProxy.of(client()));

        WatchExecutionContext ctx = mockExecutionContext("_name", EMPTY_PAYLOAD);

        Transform.Result result = transform.execute(ctx, EMPTY_PAYLOAD);
        assertThat(result, notNullValue());
        assertThat(result.type(), is(SearchTransform.TYPE));

        SearchResponse response = client().search(request).get();
        Payload expectedPayload = new Payload.XContent(response);

        // we need to remove the "took" field from teh response as this is the only field
        // that most likely be different between the two... we don't really care about this
        // field, we just want to make sure that the important parts of the response are the same
        Map<String, Object> resultData = result.payload().data();
        resultData.remove("took");
        Map<String, Object> expectedData = expectedPayload.data();
        expectedData.remove("took");

        assertThat(resultData, equalTo(expectedData));
    }

    @Test
    public void testApply_MustacheTemplate() throws Exception {

        // The rational behind this test:
        //
        // - we index 4 documents each one associated with a unique value and each is associated with a day
        // - we build a search transform such that with a filter that
        //   - the date must be after [scheduled_time] variable
        //   - the date must be before [execution_time] variable
        //   - the value must match [payload.value] variable
        // - the variable are set as such:
        //   - scheduled_time = youngest document's date
        //   - fired_time = oldest document's date
        //   - payload.value = val_3
        // - when executed, the variables will be replaced with the scheduled_time, fired_time and the payload.value.
        // - we set all these variables accordingly (the search transform is responsible to populate them)
        // - when replaced correctly, the search should return document 3.
        //
        // we then do a search for document 3, and compare the response to the payload returned by the transform



        index("idx", "type", "1", doc("2015-01-01T00:00:00", "val_1"));
        index("idx", "type", "2", doc("2015-01-02T00:00:00", "val_2"));
        index("idx", "type", "3", doc("2015-01-03T00:00:00", "val_3"));
        index("idx", "type", "4", doc("2015-01-04T00:00:00", "val_4"));

        ensureGreen("idx");
        refresh();

        SearchRequest request = Requests.searchRequest("idx").source(searchSource().query(filteredQuery(matchAllQuery(), boolFilter()
                .must(rangeFilter("date").gt("{{ctx.trigger.scheduled_time}}"))
                .must(rangeFilter("date").lt("{{ctx.execution_time}}"))
                .must(termFilter("value", "{{ctx.payload.value}}")))));

        ExecutableSearchTransform transform = new ExecutableSearchTransform(new SearchTransform(request), logger, scriptService(), ClientProxy.of(client()));

        ScheduleTriggerEvent event = new ScheduleTriggerEvent("_name", parseDate("2015-01-04T00:00:00", UTC), parseDate("2015-01-01T00:00:00", UTC));
        WatchExecutionContext ctx = mockExecutionContext("_name", parseDate("2015-01-04T00:00:00", UTC), event, EMPTY_PAYLOAD);

        Payload payload = simplePayload("value", "val_3");

        Transform.Result result = transform.execute(ctx, payload);
        assertThat(result, notNullValue());
        assertThat(result.type(), is(SearchTransform.TYPE));

        SearchResponse response = client().prepareSearch("idx").setQuery(
                filteredQuery(matchAllQuery(), termFilter("value", "val_3")))
                .get();
        Payload expectedPayload = new Payload.XContent(response);

        // we need to remove the "took" field from teh response as this is the only field
        // that most likely be different between the two... we don't really care about this
        // field, we just want to make sure that the important parts of the response are the same
        Map<String, Object> resultData = result.payload().data();
        resultData.remove("took");
        Map<String, Object> expectedData = expectedPayload.data();
        expectedData.remove("took");

        assertThat(resultData, equalTo(expectedData));
    }

    @Test
    public void testParser() throws Exception {
        String[] indices = rarely() ? null : randomBoolean() ? new String[] { "idx" } : new String[] { "idx1", "idx2" };
        SearchType searchType = randomBoolean() ? null : randomFrom(SearchType.values());
        String templateName = randomBoolean() ? null : "template1";
        ScriptService.ScriptType templateType = templateName != null && randomBoolean() ? randomFrom(ScriptService.ScriptType.values()) : null;
        XContentBuilder builder = jsonBuilder().startObject();
        if (indices != null) {
            builder.array("indices", indices);
        }
        if (searchType != null) {
            builder.field("search_type", searchType.name());
        }
        if (templateName != null) {
            builder.startObject("template")
                    .field("name", templateName);
            if (templateType != null) {
                builder.field("type", templateType);
            }
            builder.endObject();
        }

        XContentBuilder sourceBuilder = jsonBuilder().startObject()
                    .startObject("query")
                        .startObject("match_all")
                        .endObject()
                    .endObject()
                .endObject();
        BytesReference source = sourceBuilder.bytes();

        builder.startObject("body")
                .startObject("query")
                .startObject("match_all")
                .endObject()
                .endObject()
                .endObject();

        builder.endObject();
        XContentParser parser = JsonXContent.jsonXContent.createParser(builder.bytes());
        parser.nextToken();
        ExecutableSearchTransform executable = new SearchTransformFactory(ImmutableSettings.EMPTY, scriptService(), ClientProxy.of(client())).parseExecutable("_id", parser);
        assertThat(executable, notNullValue());
        assertThat(executable.type(), is(SearchTransform.TYPE));
        assertThat(executable.transform().getRequest(), notNullValue());
        if (indices != null) {
            assertThat(executable.transform().getRequest().indices(), arrayContainingInAnyOrder(indices));
        }
        if (searchType != null) {
            assertThat(executable.transform().getRequest().searchType(), is(searchType));
        }
        if (templateName != null) {
            assertThat(executable.transform().getRequest().templateName(), equalTo(templateName));
        }
        if (templateType != null) {
            assertThat(executable.transform().getRequest().templateType(), equalTo(templateType));
        }
        assertThat(executable.transform().getRequest().source().toBytes(), equalTo(source.toBytes()));
    }

    private static Map<String, Object> doc(String date, String value) {
        Map<String, Object> doc = new HashMap<>();
        doc.put("date", parseDate(date, UTC));
        doc.put("value", value);
        return doc;
    }

}
