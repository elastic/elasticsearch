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
import java.util.List;

import static org.elasticsearch.xpack.esql.plugin.TransportReadStreamPagesAction.MAX_WAIT_TIMEOUT;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class ReadStreamPagesRequestTests extends ESTestCase {

    public void testValid() {
        assertThat(new Request("r", 0, MAX_WAIT_TIMEOUT).validate(), nullValue());
        assertThat(Request.fromRow("r", 0, TimeValue.ZERO).validate(), nullValue());
    }

    public void testInvalid() {
        // seq -1 is the metadata document, not a page
        assertInvalid(new Request("r", -1, TimeValue.ZERO), "[fromSeq] or [fromRow] must be at least 0");
        assertInvalid(Request.fromRow("r", -5, TimeValue.ZERO), "got [-1] and [-5]");
        assertInvalid(new Request("r", randomIntBetween(Integer.MIN_VALUE, -2), TimeValue.ZERO), "[fromSeq]");
        assertInvalid(new Request("r", 1, TimeValue.timeValueMillis(MAX_WAIT_TIMEOUT.millis() + 1)), "[waitTimeout]");
    }

    public void testRequestRoundTrip() throws IOException {
        Request request = randomBoolean()
            ? new Request(randomAlphaOfLength(8), randomIntBetween(0, Integer.MAX_VALUE), randomTimeValue())
            : Request.fromRow(randomAlphaOfLength(8), randomNonNegativeLong(), TimeValue.ZERO);
        Request copy = copyWriteable(request, new NamedWriteableRegistry(List.of()), Request::new);
        assertThat(copy.streamId(), equalTo(request.streamId()));
        assertThat(copy.fromSeq(), equalTo(request.fromSeq()));
        assertThat(copy.fromRow(), equalTo(request.fromRow()));
        assertThat(copy.waitTimeout(), equalTo(request.waitTimeout()));
    }

    /**
     * A response read from another node holds its page as it was written, charged to the breaker of the reading node until it
     * is released.
     */
    public void testResponseRoundTripChargesBreaker() throws IOException {
        StoredPage page = randomBoolean() ? null : randomPage(between(0, 1024));
        long bytes = page == null ? 0 : page.bytes().length();
        boolean ended = randomBoolean();
        Response response = new Response(page, ended, () -> {});
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
        assertThat(copy.page(), equalTo(page));
        assertThat(breaker.getUsed(), equalTo(bytes));
        copy.decRef();
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /**
     * A page that trips the breaker of the reading node fails the read there, holding nothing, instead of being read again from
     * another copy.
     */
    public void testResponseThatTripsBreakerFailsRead() throws IOException {
        StoredPage page = randomPage(between(1, 1024));
        boolean ended = randomBoolean();
        Response response = new Response(page, ended, () -> {});
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofBytes(between(0, page.bytes().length() - 1)));
        Response copy;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            response.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                copy = new Response(in, breaker);
            }
        } finally {
            response.decRef();
        }
        assertThat(copy.page(), nullValue());
        assertThat(copy.resultEnded(), equalTo(ended));
        assertThat(breaker.getUsed(), equalTo(0L));
        PlainActionFuture<Response> future = new PlainActionFuture<>();
        TransportReadStreamPagesAction.failOnTrip(future).onResponse(copy);
        expectThrows(CircuitBreakingException.class, future::actionGet);
        copy.decRef();
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    private static StoredPage randomPage(int bytes) {
        return new StoredPage(
            between(0, 100),
            randomNonNegativeLong(),
            between(1, 100),
            TransportVersionUtils.randomVersion(),
            new BytesArray(randomByteArrayOfLength(bytes))
        );
    }

    private static void assertInvalid(Request request, String message) {
        assertThat(request.validate().getMessage(), containsString(message));
    }
}
