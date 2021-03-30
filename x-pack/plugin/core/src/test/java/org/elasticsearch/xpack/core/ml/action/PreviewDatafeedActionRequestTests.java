/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ml.action.PreviewDatafeedAction.Request;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfigTests;
import org.elasticsearch.xpack.core.ml.job.config.JobTests;

import java.util.Collections;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class PreviewDatafeedActionRequestTests extends AbstractWireSerializingTestCase<Request> {

    @Override
    protected Request createTestInstance() {
        String jobId = randomAlphaOfLength(10);
        return randomBoolean() ?
            new Request(randomAlphaOfLength(10)) :
            new Request(
                DatafeedConfigTests.createRandomizedDatafeedConfig(jobId),
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
        Request.Builder requestBuilder = new Request.Builder()
            .setDatafeedId(randomAlphaOfLength(10))
            .setDatafeedBuilder(new DatafeedConfig.Builder(DatafeedConfigTests.createRandomizedDatafeedConfig(jobId)))
            .setJobBuilder(randomBoolean() ? JobTests.buildJobBuilder(jobId) : null);
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, requestBuilder::build);
        assertThat(ex.getMessage(),
            containsString("[datafeed_id] cannot be supplied when either [job_config] or [datafeed_config] is present"));

        requestBuilder.setJobBuilder(null)
            .setDatafeedId(null)
            .setDatafeedBuilder(new DatafeedConfig.Builder());

        ex = expectThrows(IllegalArgumentException.class, requestBuilder::build);
        assertThat(ex.getMessage(),
            containsString("[datafeed_config.job_id] must be set or a [job_config] must be provided"));
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
