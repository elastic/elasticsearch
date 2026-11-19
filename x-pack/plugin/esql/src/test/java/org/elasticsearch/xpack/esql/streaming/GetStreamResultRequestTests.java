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

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * The request is local to the node that receives it and is never serialized, so there is no wire test. This covers its parameters.
 */
public class GetStreamResultRequestTests extends ESTestCase {

    public void testDefaults() {
        GetStreamResultRequest request = new GetStreamResultRequest(
            "id",
            0,
            GetStreamResultRequest.NO_LIMIT,
            GetStreamResultRequest.NO_LIMIT,
            null
        );
        assertThat(request.offset(), equalTo(0L));
        assertThat(request.hasRowLimit(), equalTo(false));
        assertThat(request.hasByteLimit(), equalTo(false));
        assertThat(request.statusOnly(), equalTo(false));
        // unset means no wait, like an unset wait_for_completion_timeout in the async API
        assertThat(request.waitTimeout().millis() <= 0, equalTo(true));
    }

    public void testValidValues() {
        long offset = randomLongBetween(0, 1_000_000);
        long maxRows = randomLongBetween(1, 1_000_000);
        long maxBytes = randomLongBetween(1, 1_000_000);
        TimeValue wait = TimeValue.timeValueMillis(randomLongBetween(1, 10_000));
        GetStreamResultRequest request = new GetStreamResultRequest("id", offset, maxRows, maxBytes, wait);
        assertThat(request.id(), equalTo("id"));
        assertThat(request.offset(), equalTo(offset));
        assertThat(request.maxRows(), equalTo(maxRows));
        assertThat(request.maxBytes(), equalTo(maxBytes));
        assertThat(request.hasRowLimit(), equalTo(true));
        assertThat(request.hasByteLimit(), equalTo(true));
        assertThat(request.waitTimeout(), equalTo(wait));
        assertThat(request.statusOnly(), equalTo(false));
    }

    public void testInvalidValues() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new GetStreamResultRequest("id", -1, GetStreamResultRequest.NO_LIMIT, GetStreamResultRequest.NO_LIMIT, null)
        );
        assertThat(e.getMessage(), containsString("[offset]"));
        e = expectThrows(
            IllegalArgumentException.class,
            () -> new GetStreamResultRequest("id", 0, -2, GetStreamResultRequest.NO_LIMIT, null)
        );
        assertThat(e.getMessage(), containsString("[max_rows]"));
        e = expectThrows(
            IllegalArgumentException.class,
            () -> new GetStreamResultRequest("id", 0, GetStreamResultRequest.NO_LIMIT, -2, null)
        );
        assertThat(e.getMessage(), containsString("[max_bytes]"));
    }

    public void testStatusOnly() {
        assertThat(new GetStreamResultRequest("id", 0, 0, GetStreamResultRequest.NO_LIMIT, null).statusOnly(), equalTo(true));
        assertThat(new GetStreamResultRequest("id", 0, GetStreamResultRequest.NO_LIMIT, 0, null).statusOnly(), equalTo(true));
        assertThat(new GetStreamResultRequest("id", 0, 0, 0, null).statusOnly(), equalTo(true));
        assertThat(new GetStreamResultRequest("id", 0, 1, 1, null).statusOnly(), equalTo(false));
    }

    public void testValidateRequiresId() {
        assertThat(new GetStreamResultRequest("id", 0, 1, 1, null).validate(), nullValue());
        ActionRequestValidationException e = new GetStreamResultRequest(null, 0, 1, 1, null).validate();
        assertThat(e, notNullValue());
        assertThat(e.getMessage(), containsString("id is missing"));
    }

    public void testIsNotSerializable() {
        // TransportAction.localOnly() is an assertion, the same as for the other local-only requests
        expectThrows(AssertionError.class, () -> new GetStreamResultRequest("id", 0, 1, 1, null).writeTo(null));
    }
}
