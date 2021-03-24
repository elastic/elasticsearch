/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ml.action.PreviewDatafeedAction.Request;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfigTests;
import org.elasticsearch.xpack.core.ml.job.config.JobTests;

import java.util.Collections;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class PreviewDatafeedActionRequestTests extends AbstractWireSerializingTestCase<Request> {

    @Override
    protected Request createTestInstance() {
        boolean includeDatafeed = randomBoolean();
        String jobId = randomAlphaOfLength(10);
        return randomBoolean() ?
            new Request(randomAlphaOfLength(10)) :
            new Request(
                includeDatafeed ? null : randomAlphaOfLength(10),
                includeDatafeed ? DatafeedConfigTests.createRandomizedDatafeedConfig(jobId) : null,
                randomBoolean() ? JobTests.buildJobBuilder(jobId) : null
            );
    }

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    public void testCtor() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> new Request((String) null));
        assertThat(ex.getMessage(), equalTo("[datafeed_id] must not be null."));
    }

    public void testValidation() {
        String jobId = randomAlphaOfLength(10);
        Request request = new Request(
            randomAlphaOfLength(10),
            DatafeedConfigTests.createRandomizedDatafeedConfig(jobId),
            randomBoolean() ? JobTests.buildJobBuilder(jobId) : null
        );

        ActionRequestValidationException validationException = request.validate();
        assertThat(validationException, is(not(nullValue())));
        assertThat(validationException.getMessage(), containsString("must provide either [datafeed_id] or [datafeed_config] but not both"));

        request = new Request(
            null,
            null,
            randomBoolean() ? JobTests.buildJobBuilder(jobId) : null
        );
        validationException = request.validate();
        assertThat(validationException, is(not(nullValue())));
        assertThat(validationException.getMessage(), containsString("must provide [datafeed_id] or [datafeed_config]"));
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedWriteableRegistry(searchModule.getNamedWriteables());
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }
}
