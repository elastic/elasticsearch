/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.sql.action.SqlQueryRequest;
import org.elasticsearch.xpack.sql.plugin.SqlMediaTypeParser.SqlMediaType;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.proto.RequestInfo;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.elasticsearch.xpack.sql.plugin.SqlMediaTypeParser.getResponseMediaType;
import static org.elasticsearch.xpack.sql.plugin.TextFormat.CSV;
import static org.elasticsearch.xpack.sql.plugin.TextFormat.PLAIN_TEXT;
import static org.elasticsearch.xpack.sql.plugin.TextFormat.TSV;
import static org.elasticsearch.xpack.sql.proto.RequestInfo.CLIENT_IDS;
import static org.hamcrest.CoreMatchers.is;

public class SqlMediaTypeParserTests extends ESTestCase {

    public void testPlainTextDetection() {
        SqlMediaType text = getResponseMediaType(reqWithAccept("text/plain"), createTestInstance());
        assertThat(text.textFormat(), is(PLAIN_TEXT));
    }

    public void testCsvDetection() {
        SqlMediaType text = getResponseMediaType(reqWithAccept("text/csv"), createTestInstance());
        assertThat(text.textFormat(), is(CSV));

        text = getResponseMediaType(reqWithAccept("text/csv; delimiter=x"), createTestInstance());
        assertThat(text.textFormat(), is(CSV));
    }

    public void testTsvDetection() {
        SqlMediaType text = getResponseMediaType(reqWithAccept("text/tab-separated-values"), createTestInstance());
        assertThat(text.textFormat(), is(TSV));
    }

    public void testMediaTypeDetectionWithParameters() {
        assertThat(getResponseMediaType(reqWithAccept("text/plain; charset=utf-8"), createTestInstance()).textFormat(), is(PLAIN_TEXT));
        assertThat(getResponseMediaType(reqWithAccept("text/plain; header=present"), createTestInstance()).textFormat(), is(PLAIN_TEXT));
        assertThat(
            getResponseMediaType(reqWithAccept("text/plain; charset=utf-8; header=present"), createTestInstance()).textFormat(),
            is(PLAIN_TEXT)
        );

        assertThat(getResponseMediaType(reqWithAccept("text/csv; charset=utf-8"), createTestInstance()).textFormat(), is(CSV));
        assertThat(getResponseMediaType(reqWithAccept("text/csv; header=present"), createTestInstance()).textFormat(), is(CSV));
        assertThat(
            getResponseMediaType(reqWithAccept("text/csv; charset=utf-8; header=present"), createTestInstance()).textFormat(),
            is(CSV)
        );

        assertThat(
            getResponseMediaType(reqWithAccept("text/tab-separated-values; charset=utf-8"), createTestInstance()).textFormat(),
            is(TSV)
        );
        assertThat(
            getResponseMediaType(reqWithAccept("text/tab-separated-values; header=present"), createTestInstance()).textFormat(),
            is(TSV)
        );
        assertThat(
            getResponseMediaType(reqWithAccept("text/tab-separated-values; charset=utf-8; header=present"), createTestInstance())
                .textFormat(),
            is(TSV)
        );
    }

    public void testInvalidFormat() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> getResponseMediaType(reqWithAccept("text/garbage"), createTestInstance())
        );
        assertEquals(e.getMessage(), "invalid format [text/garbage]");
    }

    public void testNoFormat() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> getResponseMediaType(new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).build(), createTestInstance())
        );
        assertEquals(e.getMessage(), "Invalid request content type: Accept=[null], Content-Type=[null], format=[null]");
    }

    private static RestRequest reqWithAccept(String acceptHeader) {

        return new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withHeaders(new HashMap<String, List<String>>() {
            {
                put("Content-Type", Collections.singletonList("application/json"));
                put("Accept", Collections.singletonList(acceptHeader));
            }
        }).build();
    }

    protected SqlQueryRequest createTestInstance() {
        return new SqlQueryRequest(
            randomAlphaOfLength(10),
            Collections.emptyList(),
            null,
            null,
            randomZone(),
            randomAlphaOfLength(9),
            between(1, Integer.MAX_VALUE),
            TimeValue.parseTimeValue(randomTimeValue(), null, "test"),
            TimeValue.parseTimeValue(randomTimeValue(), null, "test"),
            false,
            randomAlphaOfLength(10),
            new RequestInfo(Mode.PLAIN, randomFrom(randomFrom(CLIENT_IDS), randomAlphaOfLengthBetween(10, 20))),
            randomBoolean(),
            randomBoolean(),
            TimeValue.parseTimeValue(randomTimeValue(), null, "test"),
            randomBoolean(),
            TimeValue.parseTimeValue(randomTimeValue(), null, "test")
        ).binaryCommunication(false);
    }
}
