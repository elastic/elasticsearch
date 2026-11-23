/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.streaming;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.xpack.esql.streaming.GetStreamResultRequest.MAX_WAIT_TIMEOUT;
import static org.elasticsearch.xpack.esql.streaming.GetStreamResultRequest.NO_LIMIT;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Tests the parameters of {@link GetStreamResultRequest}: what is accepted, what is rejected, and the values derived from them.
 * The request runs on the node that receives it and is never sent to another node, so there is no wire serialization test.
 */
public class GetStreamResultRequestTests extends ESTestCase {

    private static GetStreamResultRequest request(long maxRows, long maxBytes) {
        return new GetStreamResultRequest("id", 0, maxRows, maxBytes, null);
    }

    // ---- construction ----

    public void testDefaults() {
        GetStreamResultRequest request = request(NO_LIMIT, NO_LIMIT);
        assertThat(request.offset(), equalTo(0L));
        assertThat(request.hasRowLimit(), equalTo(false));
        assertThat(request.hasByteLimit(), equalTo(false));
        assertThat(request.statusOnly(), equalTo(false));
        // an unset wait_timeout means no wait, as an unset wait_for_completion_timeout does in the async API
        assertThat(request.waitTimeout().millis(), lessThanOrEqualTo(0L));
    }

    public void testValidValues() {
        long offset = randomLongBetween(0, 1_000_000);
        long maxRows = randomLongBetween(1, 1_000_000);
        long maxBytes = randomLongBetween(1, 1_000_000);
        TimeValue wait = TimeValue.timeValueMillis(randomLongBetween(1, 10_000));
        GetStreamResultRequest request = new GetStreamResultRequest("id", offset, maxRows, maxBytes, wait);
        assertThat(request.streamId(), equalTo("id"));
        assertThat(request.offset(), equalTo(offset));
        assertThat(request.maxRows(), equalTo(maxRows));
        assertThat(request.maxBytes(), equalTo(maxBytes));
        assertThat(request.hasRowLimit(), equalTo(true));
        assertThat(request.hasByteLimit(), equalTo(true));
        assertThat(request.waitTimeout(), equalTo(wait));
        assertThat(request.statusOnly(), equalTo(false));
    }

    // ---- rejected values ----

    public void testNegativeOffsetIsRejected() {
        expectThrows(
            IllegalArgumentException.class,
            containsString("[offset]"),
            () -> new GetStreamResultRequest("id", -1, NO_LIMIT, NO_LIMIT, null)
        );
    }

    /** {@link GetStreamResultRequest#NO_LIMIT} (-1) is the only negative limit allowed, as the marker for "not provided". */
    public void testNegativeLimitsOtherThanNoLimitAreRejected() {
        long negative = randomLongBetween(Long.MIN_VALUE, -2);
        expectThrows(IllegalArgumentException.class, containsString("[max_rows]"), () -> request(negative, NO_LIMIT));
        expectThrows(IllegalArgumentException.class, containsString("[max_bytes]"), () -> request(NO_LIMIT, negative));
    }

    public void testWaitTimeoutUpperLimit() {
        GetStreamResultRequest atLimit = new GetStreamResultRequest("id", 0, 1, 1, MAX_WAIT_TIMEOUT);
        assertThat(atLimit.waitTimeout(), equalTo(MAX_WAIT_TIMEOUT));

        TimeValue tooLong = TimeValue.timeValueMillis(MAX_WAIT_TIMEOUT.millis() + 1);
        expectThrows(
            IllegalArgumentException.class,
            containsString("[wait_timeout] must be less than or equal to [5m]"),
            () -> new GetStreamResultRequest("id", 0, 1, 1, tooLong)
        );
    }

    // ---- derived values ----

    /** A zero row limit or a zero byte limit, alone or together, asks for the status only. */
    public void testStatusOnly() {
        assertThat(request(0, NO_LIMIT).statusOnly(), equalTo(true));
        assertThat(request(NO_LIMIT, 0).statusOnly(), equalTo(true));
        assertThat(request(0, 0).statusOnly(), equalTo(true));
        assertThat(request(1, 1).statusOnly(), equalTo(false));
        assertThat(request(NO_LIMIT, NO_LIMIT).statusOnly(), equalTo(false));
    }

    // ---- validate() and serialization ----

    public void testValidateRequiresId() {
        assertThat(new GetStreamResultRequest("id", 0, 1, 1, null).validate(), nullValue());
        for (String missing : new String[] { null, "" }) {
            ActionRequestValidationException e = new GetStreamResultRequest(missing, 0, 1, 1, null).validate();
            assertThat(e, notNullValue());
            assertThat(e.getMessage(), containsString("id is missing"));
        }
    }

    /** {@code TransportAction.localOnly()} is an assertion, as for the other local-only requests. */
    public void testWriteToIsNotSupported() {
        expectThrows(AssertionError.class, () -> new GetStreamResultRequest("id", 0, 1, 1, null).writeTo(null));
    }
}
