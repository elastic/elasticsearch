/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.transform;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.alerts.ExecutionContext;
import org.elasticsearch.alerts.Payload;
import org.elasticsearch.alerts.support.Variables;
import org.elasticsearch.alerts.support.init.proxy.ClientProxy;
import org.elasticsearch.alerts.test.AbstractAlertsSingleNodeTests;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.script.ScriptService;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.alerts.support.AlertsDateUtils.formatDate;
import static org.elasticsearch.alerts.support.AlertsDateUtils.parseDate;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.FilterBuilders.*;
import static org.elasticsearch.index.query.QueryBuilders.filteredQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 */
public class SearchTransformTests extends AbstractAlertsSingleNodeTests {

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
        SearchTransform transform = new SearchTransform(logger, scriptService(), ClientProxy.of(client()), request);

        ExecutionContext ctx = mock(ExecutionContext.class);
        DateTime now = new DateTime();
        when(ctx.scheduledTime()).thenReturn(now);
        when(ctx.fireTime()).thenReturn(now);

        Payload payload = new Payload.Simple(new HashMap<String, Object>());

        Transform.Result result = transform.apply(ctx, payload);
        assertThat(result, notNullValue());
        assertThat(result.type(), is(SearchTransform.TYPE));

        SearchResponse response = client().search(request).get();
        Payload expectedPayload = new Payload.ActionResponse(response);

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
        //   - the date must be before [fired_time] variable
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
                .must(rangeFilter("date").gt("{{" + Variables.SCHEDULED_FIRE_TIME + "}}"))
                .must(rangeFilter("date").lt("{{" + Variables.FIRE_TIME + "}}"))
                .must(termFilter("value", "{{" + Variables.PAYLOAD + ".value}}")))));

        SearchTransform transform = new SearchTransform(logger, scriptService(), ClientProxy.of(client()), request);

        ExecutionContext ctx = mock(ExecutionContext.class);
        when(ctx.scheduledTime()).thenReturn(parseDate("2015-01-01T00:00:00"));
        when(ctx.fireTime()).thenReturn(parseDate("2015-01-04T00:00:00"));

        Payload payload = new Payload.Simple(ImmutableMap.<String, Object>builder()
                .put("value", "val_3")
                .build());

        Transform.Result result = transform.apply(ctx, payload);
        assertThat(result, notNullValue());
        assertThat(result.type(), is(SearchTransform.TYPE));

        SearchResponse response = client().prepareSearch("idx").setQuery(
                filteredQuery(matchAllQuery(), termFilter("value", "val_3")))
                .get();
        Payload expectedPayload = new Payload.ActionResponse(response);

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
        BytesReference source = null;
        XContentBuilder builder = jsonBuilder().startObject();
        if (indices != null) {
            builder.array("indices", indices);
        }
        if (searchType != null) {
            builder.field("search_type", searchType.name());
        }
        if (templateName != null) {
            builder.field("template_name", templateName);
        }
        if (templateType != null) {
            builder.field("template_type", templateType);
        }

        XContentBuilder sourceBuilder = jsonBuilder().startObject()
                    .startObject("query")
                        .startObject("match_all")
                        .endObject()
                    .endObject()
                .endObject();
        source = sourceBuilder.bytes();

        builder.startObject("body")
                .startObject("query")
                .startObject("match_all")
                .endObject()
                .endObject()
                .endObject();

        builder.endObject();
        XContentParser parser = JsonXContent.jsonXContent.createParser(builder.bytes());
        parser.nextToken();
        SearchTransform transform = new SearchTransform.Parser(ImmutableSettings.EMPTY, scriptService(), ClientProxy.of(client())).parse(parser);
        assertThat(transform, notNullValue());
        assertThat(transform.type(), is(SearchTransform.TYPE));
        assertThat(transform.request, notNullValue());
        if (indices != null) {
            assertThat(transform.request.indices(), arrayContainingInAnyOrder(indices));
        }
        if (searchType != null) {
            assertThat(transform.request.searchType(), is(searchType));
        }
        if (templateName != null) {
            assertThat(transform.request.templateName(), equalTo(templateName));
        }
        if (templateType != null) {
            assertThat(transform.request.templateType(), equalTo(templateType));
        }
        assertThat(transform.request.source().toBytes(), equalTo(source.toBytes()));
    }

    @Test
    public void testFlatten() throws Exception {
        DateTime now = new DateTime();
        Map<String, Object> map = ImmutableMap.<String, Object>builder()
                .put("a", ImmutableMap.builder().put("a1", new int[] { 0, 1, 2 }).build())
                .put("b", new String[] { "b0", "b1", "b2" })
                .put("c", ImmutableList.of( TimeValue.timeValueSeconds(0), TimeValue.timeValueSeconds(1)))
                .put("d", now)
                .build();

        Map<String, String> result = SearchTransform.flatten(map);
        assertThat(result.size(), is(9));
        assertThat(result, hasEntry("a.a1.0", "0"));
        assertThat(result, hasEntry("a.a1.1", "1"));
        assertThat(result, hasEntry("a.a1.2", "2"));
        assertThat(result, hasEntry("b.0", "b0"));
        assertThat(result, hasEntry("b.1", "b1"));
        assertThat(result, hasEntry("b.2", "b2"));
        assertThat(result, hasEntry("c.0", "0"));
        assertThat(result, hasEntry("c.1", "1000"));
        assertThat(result, hasEntry("d", formatDate(now)));
    }

    private static Map<String, Object> doc(String date, String value) {
        Map<String, Object> doc = new HashMap<>();
        doc.put("date", parseDate(date));
        doc.put("value", value);
        return doc;
    }

}
