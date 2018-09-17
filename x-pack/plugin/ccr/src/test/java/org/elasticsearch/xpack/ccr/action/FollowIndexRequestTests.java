/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;
import org.elasticsearch.xpack.core.ccr.action.FollowIndexAction;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class FollowIndexRequestTests extends AbstractStreamableXContentTestCase<FollowIndexAction.Request> {

    @Override
    protected FollowIndexAction.Request createBlankInstance() {
        return new FollowIndexAction.Request();
    }

    @Override
    protected FollowIndexAction.Request createTestInstance() {
        return createTestRequest();
    }

    @Override
    protected FollowIndexAction.Request doParseInstance(XContentParser parser) throws IOException {
        return FollowIndexAction.Request.fromXContent(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    static FollowIndexAction.Request createTestRequest() {
        return new FollowIndexAction.Request(randomAlphaOfLength(4), randomAlphaOfLength(4), randomIntBetween(1, Integer.MAX_VALUE),
            randomIntBetween(1, Integer.MAX_VALUE), randomNonNegativeLong(), randomIntBetween(1, Integer.MAX_VALUE),
            randomIntBetween(1, Integer.MAX_VALUE), TimeValue.timeValueMillis(500), TimeValue.timeValueMillis(500));
    }

    public void testValidate() {
        FollowIndexAction.Request request = new FollowIndexAction.Request("index1", "index2", null, null, null, null,
            null, TimeValue.ZERO, null);
        ActionRequestValidationException validationException = request.validate();
        assertThat(validationException, notNullValue());
        assertThat(validationException.getMessage(), containsString("[max_retry_delay] must be positive but was [0ms]"));

        request = new FollowIndexAction.Request("index1", "index2", null, null, null, null, null, TimeValue.timeValueMinutes(10), null);
        validationException = request.validate();
        assertThat(validationException, notNullValue());
        assertThat(validationException.getMessage(), containsString("[max_retry_delay] must be less than [5m] but was [10m]"));

        request = new FollowIndexAction.Request("index1", "index2", null, null, null, null, null, TimeValue.timeValueMinutes(1), null);
        validationException = request.validate();
        assertThat(validationException, nullValue());
    }
}
