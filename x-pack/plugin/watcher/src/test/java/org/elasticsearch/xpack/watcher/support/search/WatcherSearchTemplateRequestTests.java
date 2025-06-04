/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.support.search;

import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class WatcherSearchTemplateRequestTests extends ESTestCase {

    public void testFromXContentWithTemplateDefaultLang() throws IOException {
        String source = """
            {"template":{"id":"default-script", "params":{"foo":"bar"}}}""";
        assertTemplate(source, "default-script", null, singletonMap("foo", "bar"));
    }

    public void testFromXContentWithTemplateCustomLang() throws IOException {
        String source = """
            {"template":{"source":"custom-script", "lang":"painful","params":{"bar":"baz"}}}""";
        assertTemplate(source, "custom-script", "painful", singletonMap("bar", "baz"));
    }

    public void testFromXContentWithEmptyTypes() throws IOException {
        String source = """
                {
                    "search_type" : "query_then_fetch",
                    "indices" : [ ".ml-anomalies-*" ],
                    "types" : [ ],
                    "body" : {
                        "query" : {
                            "bool" : {
                                "filter" : [ { "term" : { "job_id" : "my-job" } }, { "range" : { "timestamp" : { "gte" : "now-30m" } } } ]
                            }
                        }
                    }
                }
            """;
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, source)) {
            parser.nextToken();
            ElasticsearchParseException e = expectThrows(
                ElasticsearchParseException.class,
                () -> WatcherSearchTemplateRequest.fromXContent(parser, randomFrom(SearchType.values()))
            );
            assertThat(e.getMessage(), is("could not read search request. unexpected array field [types]"));
        }
    }

    public void testFromXContentWithNonEmptyTypes() throws IOException {
        String source = """
                {
                    "search_type" : "query_then_fetch",
                    "indices" : [ "my-index" ],
                    "types" : [ "my-type" ],
                    "body" : {
                        "query" : { "match_all" : {} }
                    }
                }
            """;
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, source)) {
            parser.nextToken();
            ElasticsearchParseException e = expectThrows(
                ElasticsearchParseException.class,
                () -> WatcherSearchTemplateRequest.fromXContent(parser, randomFrom(SearchType.values()))
            );
            assertThat(e.getMessage(), is("could not read search request. unexpected array field [types]"));
        }
    }

    public void testDefaultHitCountsDefaults() throws IOException {
        assertHitCount("{}", true);
    }

    public void testDefaultHitCountsConfigured() throws IOException {
        boolean hitCountsAsInt = randomBoolean();
        String source = "{ \"rest_total_hits_as_int\" : " + hitCountsAsInt + " }";
        assertHitCount(source, hitCountsAsInt);
    }

    private void assertHitCount(String source, boolean expectedHitCountAsInt) throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, source)) {
            parser.nextToken();
            WatcherSearchTemplateRequest request = WatcherSearchTemplateRequest.fromXContent(parser, SearchType.QUERY_THEN_FETCH);
            assertThat(request.isRestTotalHitsAsint(), is(expectedHitCountAsInt));
        }
    }

    private void assertTemplate(String source, String expectedScript, String expectedLang, Map<String, Object> expectedParams)
        throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, source)) {
            parser.nextToken();
            WatcherSearchTemplateRequest result = WatcherSearchTemplateRequest.fromXContent(parser, randomFrom(SearchType.values()));
            assertNotNull(result.getTemplate());
            assertThat(result.getTemplate().getIdOrCode(), equalTo(expectedScript));
            assertThat(result.getTemplate().getLang(), equalTo(expectedLang));
            assertThat(result.getTemplate().getParams(), equalTo(expectedParams));
        }
    }

    protected List<String> filteredWarnings() {
        return List.of(WatcherSearchTemplateRequest.TYPES_DEPRECATION_MESSAGE);
    }
}
