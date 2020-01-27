/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
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

    private static NamedXContentRegistry registry =
        new NamedXContentRegistry(new SearchModule(Settings.EMPTY, List.of()).getNamedXContents());
    public void testUnknownFieldParsingErrors() throws IOException {
        assertParsingErrorMessage("{\"key\" : \"value\"}", "unknown field [key]", EqlSearchRequest::fromXContent);
    }

    public void testSearchRequestParser() throws IOException {
        assertParsingErrorMessage("{\"query\" : 123}", "query doesn't support values of type: VALUE_NUMBER",
            EqlSearchRequest::fromXContent);
        assertParsingErrorMessage("{\"timestamp_field\" : 123}", "timestamp_field doesn't support values of type: VALUE_NUMBER",
            EqlSearchRequest::fromXContent);
        assertParsingErrorMessage("{\"event_type_field\" : 123}", "event_type_field doesn't support values of type: VALUE_NUMBER",
            EqlSearchRequest::fromXContent);
        assertParsingErrorMessage("{\"implicit_join_key_field\" : 123}",
            "implicit_join_key_field doesn't support values of type: VALUE_NUMBER",
            EqlSearchRequest::fromXContent);
        assertParsingErrorMessage("{\"search_after\" : 123}", "search_after doesn't support values of type: VALUE_NUMBER",
            EqlSearchRequest::fromXContent);
        assertParsingErrorMessage("{\"size\" : \"foo\"}", "failed to parse field [size]", EqlSearchRequest::fromXContent);
        assertParsingErrorMessage("{\"rule\" : 123}", "rule doesn't support values of type: VALUE_NUMBER",
            EqlSearchRequest::fromXContent);

        assertParsingErrorMessage("{\"rule\" : \"whatever\", \"size\":\"abc\"}", "failed to parse field [size]",
            EqlSearchRequest::fromXContent);

        EqlSearchRequest request = generateRequest("endgame-*", "{\"query\" : {\"match\" : {\"foo\":\"bar\"}}, "
            + "\"timestamp_field\" : \"tsf\", "
            + "\"event_type_field\" : \"etf\","
            + "\"implicit_join_key_field\" : \"imjf\","
            + "\"search_after\" : [ 12345678, \"device-20184\", \"/user/local/foo.exe\", \"2019-11-26T00:45:43.542\" ],"
            + "\"size\" : \"101\","
            + "\"rule\" : \"file where user != 'SYSTEM' by file_path\""
            + "}", EqlSearchRequest::fromXContent);
        assertArrayEquals(new String[]{"endgame-*"}, request.indices());
        assertNotNull(request.query());
        assertTrue(request.query() instanceof MatchQueryBuilder);
        MatchQueryBuilder query = (MatchQueryBuilder)request.query();
        assertEquals("foo", query.fieldName());
        assertEquals("bar", query.value());
        assertEquals("tsf", request.timestampField());
        assertEquals("etf", request.eventTypeField());
        assertEquals("imjf", request.implicitJoinKeyField());
        assertArrayEquals(new Object[]{12345678, "device-20184", "/user/local/foo.exe", "2019-11-26T00:45:43.542"}, request.searchAfter());
        assertEquals(101, request.fetchSize());
        assertEquals("file where user != 'SYSTEM' by file_path", request.rule());
    }

    private EqlSearchRequest generateRequest(String index, String json, Function<XContentParser, EqlSearchRequest> fromXContent)
            throws IOException {
        XContentParser parser = parser(json);
        return fromXContent.apply(parser).indices(new String[]{index});
    }

    private void assertParsingErrorMessage(String json, String errorMessage, Consumer<XContentParser> consumer) throws IOException {
        XContentParser parser = parser(json);
        Exception e = expectThrows(IllegalArgumentException.class, () -> consumer.accept(parser));
        assertThat(e.getMessage(), containsString(errorMessage));
    }

    private XContentParser parser(String content) throws IOException {
        XContentType xContentType = XContentType.JSON;

        return xContentType.xContent().createParser(registry, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, content);
    }
}
