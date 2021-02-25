/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.MediaType;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xpack.sql.action.SqlQueryRequest;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.proto.RequestInfo;

import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.xpack.sql.plugin.TextFormat.CSV;
import static org.elasticsearch.xpack.sql.plugin.TextFormat.PLAIN_TEXT;
import static org.elasticsearch.xpack.sql.plugin.TextFormat.TSV;
import static org.elasticsearch.xpack.sql.proto.RequestInfo.CLIENT_IDS;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;

public class SqlMediaTypeParserTests extends ESTestCase {
    SqlMediaTypeParser parser = new SqlMediaTypeParser();

    public void testPlainTextDetection() {
        MediaType text = parser.getResponseMediaType(reqWithAccept("text/plain"), createTestInstance(false, Mode.PLAIN, false));
        assertThat(text, is(PLAIN_TEXT));
    }

    public void testCsvDetection() {
        MediaType text = parser.getResponseMediaType(reqWithAccept("text/csv"), createTestInstance(false, Mode.PLAIN, false));
        assertThat(text, is(CSV));

        text = parser.getResponseMediaType(reqWithAccept("text/csv; delimiter=x"), createTestInstance(false, Mode.PLAIN, false));
        assertThat(text, is(CSV));
    }

    public void testTsvDetection() {
        MediaType text = parser.getResponseMediaType(reqWithAccept("text/tab-separated-values"),
            createTestInstance(false, Mode.PLAIN, false));
        assertThat(text, is(TSV));
    }

    public void testMediaTypeDetectionWithParameters() {
        assertThat(parser.getResponseMediaType(reqWithAccept("text/plain; charset=utf-8"),
            createTestInstance(false, Mode.PLAIN, false)), is(PLAIN_TEXT));
        assertThat(parser.getResponseMediaType(reqWithAccept("text/plain; header=present"),
            createTestInstance(false, Mode.PLAIN, false)), is(PLAIN_TEXT));
        assertThat(parser.getResponseMediaType(reqWithAccept("text/plain; charset=utf-8; header=present"),
            createTestInstance(false, Mode.PLAIN, false)), is(PLAIN_TEXT));

        assertThat(parser.getResponseMediaType(reqWithAccept("text/csv; charset=utf-8"),
            createTestInstance(false, Mode.PLAIN, false)), is(CSV));
        assertThat(parser.getResponseMediaType(reqWithAccept("text/csv; header=present"),
            createTestInstance(false, Mode.PLAIN, false)), is(CSV));
        assertThat(parser.getResponseMediaType(reqWithAccept("text/csv; charset=utf-8; header=present"),
            createTestInstance(false, Mode.PLAIN, false)), is(CSV));

        assertThat(parser.getResponseMediaType(reqWithAccept("text/tab-separated-values; charset=utf-8"),
            createTestInstance(false, Mode.PLAIN, false)), is(TSV));
        assertThat(parser.getResponseMediaType(reqWithAccept("text/tab-separated-values; header=present"),
            createTestInstance(false, Mode.PLAIN, false)), is(TSV));
        assertThat(parser.getResponseMediaType(reqWithAccept("text/tab-separated-values; charset=utf-8; header=present"),
            createTestInstance(false, Mode.PLAIN, false)), is(TSV));
    }

    public void testInvalidFormat() {
        MediaType mediaType = parser.getResponseMediaType(reqWithAccept("text/garbage"), createTestInstance(false, Mode.PLAIN, false));
        assertThat(mediaType, is(nullValue()));
    }

    private static RestRequest reqWithAccept(String acceptHeader) {

        return new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY)
            .withHeaders(Map.of("Content-Type", Collections.singletonList("application/json"),
                "Accept", Collections.singletonList(acceptHeader)))
            .build();
    }

    protected SqlQueryRequest createTestInstance(boolean binaryCommunication, Mode mode, boolean columnar) {
        return new SqlQueryRequest(randomAlphaOfLength(10), Collections.emptyList(), null,
            randomZone(), between(1, Integer.MAX_VALUE), TimeValue.parseTimeValue(randomTimeValue(), null, "test"),
            TimeValue.parseTimeValue(randomTimeValue(), null, "test"), columnar, randomAlphaOfLength(10),
            new RequestInfo(mode, randomFrom(randomFrom(CLIENT_IDS), randomAlphaOfLengthBetween(10, 20))),
            randomBoolean(), randomBoolean()).binaryCommunication(binaryCommunication);
    }
}
