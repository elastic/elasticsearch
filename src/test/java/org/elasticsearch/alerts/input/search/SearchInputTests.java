/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.input.search;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.alerts.ExecutionContext;
import org.elasticsearch.alerts.input.Input;
import org.elasticsearch.alerts.input.InputException;
import org.elasticsearch.alerts.support.AlertUtils;
import org.elasticsearch.alerts.support.Variables;
import org.elasticsearch.alerts.support.init.proxy.ClientProxy;
import org.elasticsearch.alerts.support.init.proxy.ScriptServiceProxy;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.joda.time.DateTimeZone;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.FilterBuilders.rangeFilter;
import static org.elasticsearch.index.query.QueryBuilders.filteredQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.hamcrest.Matchers.equalTo;


/**
 */
public class SearchInputTests extends ElasticsearchIntegrationTest {

    @Test
    public void testExecute() throws Exception {

        SearchSourceBuilder searchSourceBuilder = searchSource().query(
                filteredQuery(matchQuery("event_type", "a"), rangeFilter("_timestamp").from("{{" + Variables.SCHEDULED_FIRE_TIME + "}}||-30s").to("{{" + Variables.SCHEDULED_FIRE_TIME + "}}"))
        );
        SearchRequest request = client()
                .prepareSearch()
                .setSearchType(SearchInput.DEFAULT_SEARCH_TYPE)
                .request()
                .source(searchSourceBuilder);

        SearchInput searchInput = new SearchInput(logger,
                ScriptServiceProxy.of(internalCluster().getInstance(ScriptService.class)),
                ClientProxy.of(client()), request);
        ExecutionContext ctx = new ExecutionContext("test-alert", null,
                new DateTime(0, DateTimeZone.UTC), new DateTime(0, DateTimeZone.UTC));
        SearchInput.Result result = searchInput.execute(ctx);

        assertThat((Integer) XContentMapValues.extractValue("hits.total", result.payload().data()), equalTo(0));
        assertNotNull(result.request());
        assertEquals(result.request().searchType(),request.searchType());
        assertArrayEquals(result.request().indices(), request.indices());
        assertEquals(result.request().indicesOptions(), request.indicesOptions());
    }

    @Test
    public void testParser_Valid() throws Exception {
        SearchRequest request = client().prepareSearch()
                .setSearchType(SearchInput.DEFAULT_SEARCH_TYPE)
                .request()
                .source(searchSource()
                        .query(filteredQuery(matchQuery("event_type", "a"), rangeFilter("_timestamp").from("{{" + Variables.SCHEDULED_FIRE_TIME + "}}||-30s").to("{{" + Variables.SCHEDULED_FIRE_TIME + "}}"))));

        XContentBuilder builder = AlertUtils.writeSearchRequest(request, jsonBuilder(), ToXContent.EMPTY_PARAMS);
        XContentParser parser = JsonXContent.jsonXContent.createParser(builder.bytes());
        parser.nextToken();

        Input.Parser searchInputParser = new SearchInput.Parser(ImmutableSettings.EMPTY,
                ScriptServiceProxy.of(internalCluster().getInstance(ScriptService.class)),
                ClientProxy.of(client()));

        Input searchInput = searchInputParser.parse(parser);
        assertEquals(SearchInput.TYPE, searchInput.type());
    }

    @Test(expected = InputException.class)
    public void testParser_Invalid() throws Exception {
        Input.Parser searchInputParser = new SearchInput.Parser(ImmutableSettings.settingsBuilder().build(),
                ScriptServiceProxy.of(internalCluster().getInstance(ScriptService.class)),
                ClientProxy.of(client()));

        Map<String, Object> data = new HashMap<>();
        data.put("foo", "bar");
        data.put("baz", new ArrayList<String>() );

        XContentBuilder jsonBuilder = jsonBuilder();
        jsonBuilder.startObject();
        jsonBuilder.field(Input.Result.PAYLOAD_FIELD.getPreferredName(), data);
        jsonBuilder.endObject();

        searchInputParser.parseResult(XContentFactory.xContent(jsonBuilder.bytes()).createParser(jsonBuilder.bytes()));
        fail("result parsing should fail if payload is provided but request is missing");
    }

    @Test
    public void testResultParser() throws Exception {
        Map<String, Object> data = new HashMap<>();
        data.put("foo", "bar");
        data.put("baz", new ArrayList<String>() );

        SearchSourceBuilder searchSourceBuilder = searchSource().query(
                filteredQuery(matchQuery("event_type", "a"), rangeFilter("_timestamp").from("{{" + Variables.SCHEDULED_FIRE_TIME + "}}||-30s").to("{{" + Variables.SCHEDULED_FIRE_TIME + "}}"))
        );
        SearchRequest request = client()
                .prepareSearch()
                .setSearchType(SearchInput.DEFAULT_SEARCH_TYPE)
                .request()
                .source(searchSourceBuilder);

        XContentBuilder jsonBuilder = jsonBuilder();
        jsonBuilder.startObject();
        jsonBuilder.field(Input.Result.PAYLOAD_FIELD.getPreferredName(), data);
        jsonBuilder.field(SearchInput.Parser.REQUEST_FIELD.getPreferredName());
        AlertUtils.writeSearchRequest(request, jsonBuilder, ToXContent.EMPTY_PARAMS);
        jsonBuilder.endObject();

        Input.Parser searchInputParser = new SearchInput.Parser(ImmutableSettings.settingsBuilder().build(),
                ScriptServiceProxy.of(internalCluster().getInstance(ScriptService.class)),
                ClientProxy.of(client()));

        Input.Result result = searchInputParser.parseResult(XContentFactory.xContent(jsonBuilder.bytes()).createParser(jsonBuilder.bytes()));

        assertEquals(SearchInput.TYPE, result.type());
        assertEquals(result.payload().data().get("foo"), "bar");
        List baz = (List)result.payload().data().get("baz");
        assertTrue(baz.isEmpty());

        assertTrue(result instanceof SearchInput.Result);
        SearchInput.Result searchInputResult = (SearchInput.Result) result;
        assertNotNull(searchInputResult.request());
    }
}
