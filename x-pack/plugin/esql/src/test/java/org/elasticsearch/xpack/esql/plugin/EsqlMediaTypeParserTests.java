/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.MediaType;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;

import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.xpack.esql.formatter.TextFormat.CSV;
import static org.elasticsearch.xpack.esql.formatter.TextFormat.PLAIN_TEXT;
import static org.elasticsearch.xpack.esql.formatter.TextFormat.TSV;
import static org.elasticsearch.xpack.esql.plugin.EsqlMediaTypeParser.getResponseMediaType;
import static org.hamcrest.CoreMatchers.is;

public class EsqlMediaTypeParserTests extends ESTestCase {

    public void testPlainTextDetection() {
        MediaType text = getResponseMediaType(reqWithAccept("text/plain"), createTestInstance(false));
        assertThat(text, is(PLAIN_TEXT));
    }

    public void testCsvDetection() {
        MediaType text = getResponseMediaType(reqWithAccept("text/csv"), createTestInstance(false));
        assertThat(text, is(CSV));

        text = getResponseMediaType(reqWithAccept("text/csv; delimiter=x"), createTestInstance(false));
        assertThat(text, is(CSV));
    }

    public void testTsvDetection() {
        MediaType text = getResponseMediaType(reqWithAccept("text/tab-separated-values"), createTestInstance(false));
        assertThat(text, is(TSV));
    }

    public void testMediaTypeDetectionWithParameters() {
        assertThat(getResponseMediaType(reqWithAccept("text/plain; charset=utf-8"), createTestInstance(false)), is(PLAIN_TEXT));
        assertThat(getResponseMediaType(reqWithAccept("text/plain; header=present"), createTestInstance(false)), is(PLAIN_TEXT));
        assertThat(
            getResponseMediaType(reqWithAccept("text/plain; charset=utf-8; header=present"), createTestInstance(false)),
            is(PLAIN_TEXT)
        );

        assertThat(getResponseMediaType(reqWithAccept("text/csv; charset=utf-8"), createTestInstance(false)), is(CSV));
        assertThat(getResponseMediaType(reqWithAccept("text/csv; header=present"), createTestInstance(false)), is(CSV));
        assertThat(getResponseMediaType(reqWithAccept("text/csv; charset=utf-8; header=present"), createTestInstance(false)), is(CSV));

        assertThat(getResponseMediaType(reqWithAccept("text/tab-separated-values; charset=utf-8"), createTestInstance(false)), is(TSV));
        assertThat(getResponseMediaType(reqWithAccept("text/tab-separated-values; header=present"), createTestInstance(false)), is(TSV));
        assertThat(
            getResponseMediaType(reqWithAccept("text/tab-separated-values; charset=utf-8; header=present"), createTestInstance(false)),
            is(TSV)
        );
    }

    public void testInvalidFormat() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> getResponseMediaType(reqWithAccept("text/garbage"), createTestInstance(false))
        );
        assertEquals(e.getMessage(), "Invalid request content type: Accept=[text/garbage], Content-Type=[application/json], format=[null]");
    }

    public void testColumnarWithAcceptText() {
        var accept = randomFrom("text/plain", "text/csv", "text/tab-separated-values");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> getResponseMediaType(reqWithAccept(accept), createTestInstance(true))
        );
        assertEquals(e.getMessage(), "Invalid use of [columnar] argument: cannot be used in combination with [txt, csv, tsv] formats");
    }

    public void testColumnarWithParamText() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> getResponseMediaType(reqWithParams(Map.of("format", randomFrom("txt", "csv", "tsv"))), createTestInstance(true))
        );
        assertEquals(e.getMessage(), "Invalid use of [columnar] argument: cannot be used in combination with [txt, csv, tsv] formats");
    }

    public void testNoFormat() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> getResponseMediaType(new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).build(), createTestInstance(false))
        );
        assertEquals(e.getMessage(), "Invalid request content type: Accept=[null], Content-Type=[null], format=[null]");
    }

    private static RestRequest reqWithAccept(String acceptHeader) {
        return new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withHeaders(
            Map.of("Content-Type", Collections.singletonList("application/json"), "Accept", Collections.singletonList(acceptHeader))
        ).build();
    }

    private static RestRequest reqWithParams(Map<String, String> params) {
        return new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withHeaders(
            Map.of("Content-Type", Collections.singletonList("application/json"))
        ).withParams(params).build();
    }

    protected EsqlQueryRequest createTestInstance(boolean columnar) {
        var request = new EsqlQueryRequest();
        request.columnar(columnar);
        return request;
    }
}
