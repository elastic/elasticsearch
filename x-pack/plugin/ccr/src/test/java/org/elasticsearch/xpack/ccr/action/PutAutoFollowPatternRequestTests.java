/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;
import org.elasticsearch.xpack.core.ccr.action.PutAutoFollowPatternAction;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class PutAutoFollowPatternRequestTests extends AbstractStreamableXContentTestCase<PutAutoFollowPatternAction.Request> {

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected PutAutoFollowPatternAction.Request doParseInstance(XContentParser parser) throws IOException {
        return PutAutoFollowPatternAction.Request.fromXContent(parser, null);
    }

    @Override
    protected PutAutoFollowPatternAction.Request createBlankInstance() {
        return new PutAutoFollowPatternAction.Request();
    }

    @Override
    protected PutAutoFollowPatternAction.Request createTestInstance() {
        PutAutoFollowPatternAction.Request request = new PutAutoFollowPatternAction.Request();
        request.setLeaderClusterAlias(randomAlphaOfLength(4));
        request.setLeaderIndexPatterns(Arrays.asList(generateRandomStringArray(4, 4, false)));
        if (randomBoolean()) {
            request.setFollowIndexNamePattern(randomAlphaOfLength(4));
        }
        if (randomBoolean()) {
            request.setPollTimeout(TimeValue.timeValueMillis(500));
        }
        if (randomBoolean()) {
            request.setMaxRetryDelay(TimeValue.timeValueMillis(500));
        }
        if (randomBoolean()) {
            request.setMaxBatchOperationCount(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            request.setMaxConcurrentReadBatches(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            request.setMaxConcurrentWriteBatches(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            request.setMaxBatchSize(new ByteSizeValue(randomNonNegativeLong(), ByteSizeUnit.BYTES));
        }
        if (randomBoolean()) {
            request.setMaxWriteBufferSize(randomIntBetween(0, Integer.MAX_VALUE));
        }
        return request;
    }

    public void testValidate() {
        PutAutoFollowPatternAction.Request request = new PutAutoFollowPatternAction.Request();
        ActionRequestValidationException validationException = request.validate();
        assertThat(validationException, notNullValue());
        assertThat(validationException.getMessage(), containsString("[leader_cluster_alias] is missing"));

        request.setLeaderClusterAlias("_alias");
        validationException = request.validate();
        assertThat(validationException, notNullValue());
        assertThat(validationException.getMessage(), containsString("[leader_index_patterns] is missing"));

        request.setLeaderIndexPatterns(Collections.emptyList());
        validationException = request.validate();
        assertThat(validationException, notNullValue());
        assertThat(validationException.getMessage(), containsString("[leader_index_patterns] is missing"));

        request.setLeaderIndexPatterns(Collections.singletonList("logs-*"));
        validationException = request.validate();
        assertThat(validationException, nullValue());

        request.setMaxRetryDelay(TimeValue.ZERO);
        validationException = request.validate();
        assertThat(validationException, notNullValue());
        assertThat(validationException.getMessage(), containsString("[max_retry_delay] must be positive but was [0ms]"));

        request.setMaxRetryDelay(TimeValue.timeValueMinutes(10));
        validationException = request.validate();
        assertThat(validationException, notNullValue());
        assertThat(validationException.getMessage(), containsString("[max_retry_delay] must be less than [5m] but was [10m]"));

        request.setMaxRetryDelay(TimeValue.timeValueMinutes(1));
        validationException = request.validate();
        assertThat(validationException, nullValue());
    }
}
