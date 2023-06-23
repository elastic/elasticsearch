/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import static org.hamcrest.Matchers.containsString;

public class EsqlQueryRequestTests extends ESTestCase {

    public void testParseFields() throws IOException {
        String query = randomAlphaOfLengthBetween(1, 100);
        boolean columnar = randomBoolean();
        ZoneId zoneId = randomZone();
        QueryBuilder filter = randomQueryBuilder();
        List<Object> params = randomList(5, () -> randomBoolean() ? randomInt(100) : randomAlphaOfLength(10));
        StringBuilder paramsString = new StringBuilder();
        paramsString.append("[");
        boolean first = true;
        for (Object param : params) {
            if (first == false) {
                paramsString.append(", ");
            }
            first = false;
            if (param instanceof String) {
                paramsString.append("\"");
                paramsString.append(param);
                paramsString.append("\"");
            } else {
                paramsString.append(param);
            }
        }
        paramsString.append("]");
        String json = String.format(Locale.ROOT, """
            {
                "query": "%s",
                "columnar": %s,
                "time_zone": "%s",
                "filter": %s,
                "params": %s
            }""", query, columnar, zoneId, filter, paramsString);

        EsqlQueryRequest request = parseEsqlQueryRequest(json);

        assertEquals(query, request.query());
        assertEquals(columnar, request.columnar());
        assertEquals(zoneId, request.zoneId());
        assertEquals(filter, request.filter());

        assertEquals(params.size(), request.params().size());
        for (int i = 0; i < params.size(); i++) {
            assertEquals(params.get(i), request.params().get(i).value);
        }
    }

    public void testRejectUnknownFields() {
        assertParserErrorMessage("""
            {
                "query": "foo",
                "time_z0ne": "Z"
            }""", "unknown field [time_z0ne] did you mean [time_zone]?");

        assertParserErrorMessage("""
            {
                "query": "foo",
                "asdf": "Z"
            }""", "unknown field [asdf]");
    }

    public void testMissingQueryIsNotValidation() throws IOException {
        EsqlQueryRequest request = parseEsqlQueryRequest("""
            {
                "time_zone": "Z"
            }""");
        assertNotNull(request.validate());
        assertThat(request.validate().getMessage(), containsString("[query] is required"));

    }

    private static void assertParserErrorMessage(String json, String message) {
        Exception e = expectThrows(IllegalArgumentException.class, () -> parseEsqlQueryRequest(json));
        assertThat(e.getMessage(), containsString(message));
    }

    private static EsqlQueryRequest parseEsqlQueryRequest(String json) throws IOException {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        XContentParserConfiguration config = XContentParserConfiguration.EMPTY.withRegistry(
            new NamedXContentRegistry(searchModule.getNamedXContents())
        );
        try (XContentParser parser = XContentType.JSON.xContent().createParser(config, json)) {
            return EsqlQueryRequest.fromXContent(parser);
        }
    }

    private static QueryBuilder randomQueryBuilder() {
        return randomFrom(
            new TermQueryBuilder(randomAlphaOfLength(5), randomAlphaOfLengthBetween(1, 10)),
            new RangeQueryBuilder(randomAlphaOfLength(5)).gt(randomIntBetween(0, 1000))
        );
    }
}
