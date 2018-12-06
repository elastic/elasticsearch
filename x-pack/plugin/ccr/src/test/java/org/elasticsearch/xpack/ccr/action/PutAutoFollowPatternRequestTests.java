/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.ccr.action.PutAutoFollowPatternAction;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class PutAutoFollowPatternRequestTests extends AbstractSerializingTestCase<PutAutoFollowPatternAction.Request> {

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected PutAutoFollowPatternAction.Request doParseInstance(XContentParser parser) throws IOException {
        return PutAutoFollowPatternAction.Request.fromXContent(parser, null);
    }

    @Override
    protected Writeable.Reader<PutAutoFollowPatternAction.Request> instanceReader() {
        return PutAutoFollowPatternAction.Request::new;
    }

    @Override
    protected PutAutoFollowPatternAction.Request createTestInstance() {
        PutAutoFollowPatternAction.Request request = new PutAutoFollowPatternAction.Request();
        request.setName(randomAlphaOfLength(4));
        request.setRemoteCluster(randomAlphaOfLength(4));
        request.setLeaderIndexPatterns(Arrays.asList(generateRandomStringArray(4, 4, false)));
        if (randomBoolean()) {
            request.setFollowIndexNamePattern(randomAlphaOfLength(4));
        }
        if (randomBoolean()) {
            request.setReadPollTimeout(TimeValue.timeValueMillis(500));
        }
        if (randomBoolean()) {
            request.setMaxRetryDelay(TimeValue.timeValueMillis(500));
        }
        if (randomBoolean()) {
            request.setMaxWriteRequestOperationCount(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            request.setMaxWriteBufferSize(new ByteSizeValue(randomNonNegativeLong()));
        }
        if (randomBoolean()) {
            request.setMaxWriteRequestSize(new ByteSizeValue(randomNonNegativeLong()));
        }
        if (randomBoolean()) {
            request.setMaxReadRequestOperationCount(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            request.setMaxConcurrentReadBatches(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            request.setMaxConcurrentWriteBatches(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            request.setMaxReadRequestSize(new ByteSizeValue(randomNonNegativeLong(), ByteSizeUnit.BYTES));
        }
        if (randomBoolean()) {
            request.setMaxWriteBufferCount(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            request.setMaxWriteBufferSize(new ByteSizeValue(randomNonNegativeLong()));
        }
        return request;
    }

    public void testValidate() {
        PutAutoFollowPatternAction.Request request = new PutAutoFollowPatternAction.Request();
        ActionRequestValidationException validationException = request.validate();
        assertThat(validationException, notNullValue());
        assertThat(validationException.getMessage(), containsString("[name] is missing"));

        request.setName("name");
        validationException = request.validate();
        assertThat(validationException, notNullValue());
        assertThat(validationException.getMessage(), containsString("[remote_cluster] is missing"));

        request.setRemoteCluster("_alias");
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

    public void testValidateName() {
        PutAutoFollowPatternAction.Request request = new PutAutoFollowPatternAction.Request();
        request.setRemoteCluster("_alias");
        request.setLeaderIndexPatterns(Collections.singletonList("logs-*"));

        request.setName("name");
        ActionRequestValidationException validationException = request.validate();
        assertThat(validationException, nullValue());
    }

    public void testValidateNameComma() {
        PutAutoFollowPatternAction.Request request = new PutAutoFollowPatternAction.Request();
        request.setRemoteCluster("_alias");
        request.setLeaderIndexPatterns(Collections.singletonList("logs-*"));

        request.setName("name1,name2");
        ActionRequestValidationException validationException = request.validate();
        assertThat(validationException, notNullValue());
        assertThat(validationException.getMessage(), containsString("name must not contain a ','"));
    }

    public void testValidateNameLeadingUnderscore() {
        PutAutoFollowPatternAction.Request request = new PutAutoFollowPatternAction.Request();
        request.setRemoteCluster("_alias");
        request.setLeaderIndexPatterns(Collections.singletonList("logs-*"));

        request.setName("_name");
        ActionRequestValidationException validationException = request.validate();
        assertThat(validationException, notNullValue());
        assertThat(validationException.getMessage(), containsString("name must not start with '_'"));
    }

    public void testValidateNameUnderscores() {
        PutAutoFollowPatternAction.Request request = new PutAutoFollowPatternAction.Request();
        request.setRemoteCluster("_alias");
        request.setLeaderIndexPatterns(Collections.singletonList("logs-*"));

        request.setName("n_a_m_e_");
        ActionRequestValidationException validationException = request.validate();
        assertThat(validationException, nullValue());
    }

    public void testValidateNameTooLong() {
        PutAutoFollowPatternAction.Request request = new PutAutoFollowPatternAction.Request();
        request.setRemoteCluster("_alias");
        request.setLeaderIndexPatterns(Collections.singletonList("logs-*"));

        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < 256; i++) {
            stringBuilder.append('x');
        }
        request.setName(stringBuilder.toString());
        ActionRequestValidationException validationException = request.validate();
        assertThat(validationException, notNullValue());
        assertThat(validationException.getMessage(), containsString("name is too long (256 > 255)"));

        request.setName("name");
        validationException = request.validate();
        assertThat(validationException, nullValue());
    }
}
