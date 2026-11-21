/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.plugin.TransportReadStreamPagesAction.Request;
import org.elasticsearch.xpack.esql.plugin.TransportReadStreamPagesAction.Response;
import org.elasticsearch.xpack.esql.plugin.TransportReadStreamPagesAction.StoredPage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.plugin.TransportReadStreamPagesAction.MAX_PAGES_PER_READ;
import static org.elasticsearch.xpack.esql.plugin.TransportReadStreamPagesAction.MAX_WAIT_TIMEOUT;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class ReadStreamPagesRequestTests extends ESTestCase {

    public void testValid() {
        assertThat(new Request("r", 1, MAX_PAGES_PER_READ, 0, MAX_WAIT_TIMEOUT).validate(), nullValue());
        assertThat(Request.fromRow("r", 0, 1, Long.MAX_VALUE, TimeValue.ZERO).validate(), nullValue());
    }

    public void testInvalid() {
        assertInvalid(new Request("r", -1, 1, 0, TimeValue.ZERO), "[fromSeq] must be at least 1");
        // seq 0 is the metadata document, not a page
        assertInvalid(new Request("r", 0, 1, 0, TimeValue.ZERO), "[fromSeq] must be at least 1");
        assertInvalid(new Request("r", 1, 0, 0, TimeValue.ZERO), "[maxPages]");
        assertInvalid(new Request("r", 1, MAX_PAGES_PER_READ + 1, 0, TimeValue.ZERO), "[maxPages]");
        assertInvalid(new Request("r", 1, 1, -1, TimeValue.ZERO), "[maxBytes]");
        assertInvalid(new Request("r", 1, 1, 0, TimeValue.timeValueMillis(MAX_WAIT_TIMEOUT.millis() + 1)), "[waitTimeout]");
    }

    public void testRequestRoundTrip() throws IOException {
        Request request = randomBoolean()
            ? new Request(
                randomAlphaOfLength(8),
                randomIntBetween(1, Integer.MAX_VALUE),
                randomIntBetween(1, MAX_PAGES_PER_READ),
                randomNonNegativeLong(),
                randomTimeValue()
            )
            : Request.fromRow(randomAlphaOfLength(8), randomNonNegativeLong(), randomIntBetween(1, MAX_PAGES_PER_READ), 0, TimeValue.ZERO);
        Request copy = copyWriteable(request, new NamedWriteableRegistry(List.of()), Request::new);
        assertThat(copy.streamId(), equalTo(request.streamId()));
        assertThat(copy.fromSeq(), equalTo(request.fromSeq()));
        assertThat(copy.fromRow(), equalTo(request.fromRow()));
        assertThat(copy.maxPages(), equalTo(request.maxPages()));
        assertThat(copy.maxBytes(), equalTo(request.maxBytes()));
        assertThat(copy.waitTimeout(), equalTo(request.waitTimeout()));
    }

    /**
     * A response read from another node holds its pages as they were written, charged to the breaker of the reading node
     * until it is released.
     */
    public void testResponseRoundTripChargesBreaker() throws IOException {
        List<StoredPage> pages = new ArrayList<>();
        long bytes = 0;
        long firstRow = 0;
        int count = between(0, 5);
        for (int seq = 1; seq <= count; seq++) {
            int rowCount = between(1, 100);
            byte[] content = randomByteArrayOfLength(between(0, 1024));
            pages.add(new StoredPage(seq, firstRow, rowCount, TransportVersionUtils.randomVersion(), new BytesArray(content)));
            firstRow += rowCount;
            bytes += content.length;
        }
        boolean ended = randomBoolean();
        Response response = new Response(pages, ended, () -> {});
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(1));
        Response copy;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            response.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                copy = new Response(in, breaker);
            }
        } finally {
            response.decRef();
        }
        assertThat(copy.resultEnded(), equalTo(ended));
        assertThat(copy.pages(), equalTo(pages));
        assertThat(breaker.getUsed(), equalTo(bytes));
        copy.decRef();
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /**
     * Pages that trip the breaker of the reading node fail the read there, holding nothing, instead of being read again from
     * another copy.
     */
    public void testResponseThatTripsBreakerFailsRead() throws IOException {
        byte[] content = randomByteArrayOfLength(between(2, 1024));
        StoredPage page = new StoredPage(1, 0, between(1, 100), TransportVersionUtils.randomVersion(), new BytesArray(content));
        Response response = new Response(List.of(page), randomBoolean(), () -> {});
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofBytes(content.length - 1));
        Response copy;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            response.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                copy = new Response(in, breaker);
            }
        } finally {
            response.decRef();
        }
        assertThat(copy.pages(), empty());
        assertThat(breaker.getUsed(), equalTo(0L));
        PlainActionFuture<Response> future = new PlainActionFuture<>();
        TransportReadStreamPagesAction.failOnTrip(future).onResponse(copy);
        expectThrows(CircuitBreakingException.class, future::actionGet);
        copy.decRef();
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    private static void assertInvalid(Request request, String message) {
        assertThat(request.validate().getMessage(), containsString(message));
    }
}
