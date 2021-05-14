/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.action;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.hamcrest.Matchers.containsString;

public class EqlRequestParserTests extends ESTestCase {

    private static final NamedXContentRegistry REGISTRY =
        new NamedXContentRegistry(new SearchModule(Settings.EMPTY, List.of()).getNamedXContents());
    public void testUnknownFieldParsingErrors() throws IOException {
        assertParsingErrorMessage("{\"key\" : \"value\"}", "unknown field [key]", EqlSearchRequest::fromXContent);
    }

    public void testSearchRequestParser() throws IOException {
        assertParsingErrorMessage("{\"filter\" : 123}", "filter doesn't support values of type: VALUE_NUMBER",
            EqlSearchRequest::fromXContent);
        assertParsingErrorMessage("{\"timestamp_field\" : 123}", "timestamp_field doesn't support values of type: VALUE_NUMBER",
            EqlSearchRequest::fromXContent);
        assertParsingErrorMessage("{\"event_category_field\" : 123}", "event_category_field doesn't support values of type: VALUE_NUMBER",
            EqlSearchRequest::fromXContent);
        assertParsingErrorMessage("{\"size\" : \"foo\"}", "failed to parse field [size]", EqlSearchRequest::fromXContent);
        assertParsingErrorMessage("{\"query\" : 123}", "query doesn't support values of type: VALUE_NUMBER",
            EqlSearchRequest::fromXContent);
        assertParsingErrorMessage("{\"query\" : \"whatever\", \"size\":\"abc\"}", "failed to parse field [size]",
            EqlSearchRequest::fromXContent);

        EqlSearchRequest request = generateRequest("endgame-*", "{\"filter\" : {\"match\" : {\"foo\":\"bar\"}}, "
            + "\"timestamp_field\" : \"tsf\", "
            + "\"event_category_field\" : \"etf\","
            + "\"size\" : \"101\","
            + "\"query\" : \"file where user != 'SYSTEM' by file_path\"}"
            , EqlSearchRequest::fromXContent);
        assertArrayEquals(new String[]{"endgame-*"}, request.indices());
        assertNotNull(request.query());
        assertTrue(request.filter() instanceof MatchQueryBuilder);
        MatchQueryBuilder filter = (MatchQueryBuilder)request.filter();
        assertEquals("foo", filter.fieldName());
        assertEquals("bar", filter.value());
        assertEquals("tsf", request.timestampField());
        assertEquals("etf", request.eventCategoryField());
        assertEquals(101, request.size());
        assertEquals(1000, request.fetchSize());
        assertEquals("file where user != 'SYSTEM' by file_path", request.query());
    }

    private EqlSearchRequest generateRequest(String index, String json, Function<XContentParser, EqlSearchRequest> fromXContent)
            throws IOException {
        XContentParser parser = parser(json);
        return fromXContent.apply(parser).indices(index);
    }

    private void assertParsingErrorMessage(String json, String errorMessage, Consumer<XContentParser> consumer) throws IOException {
        XContentParser parser = parser(json);
        Exception e = expectThrows(IllegalArgumentException.class, () -> consumer.accept(parser));
        assertThat(e.getMessage(), containsString(errorMessage));
    }

    private XContentParser parser(String content) throws IOException {
        XContentType xContentType = XContentType.JSON;

        return xContentType.xContent().createParser(REGISTRY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, content);
    }
}
