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
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ml.action.PreviewDatafeedAction.Request;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfigBuilderTests;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfigTests;
import org.elasticsearch.xpack.core.ml.job.config.JobTests;

import java.util.Collections;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class PreviewDatafeedActionRequestTests extends AbstractWireSerializingTestCase<Request> {

    @Override
    protected Request createTestInstance() {
        String jobId = randomAlphaOfLength(10);
        long start = randomLongBetween(0, Long.MAX_VALUE / 4);
        return switch (randomInt(2)) {
            case 0 -> new Request(
                randomAlphaOfLength(10),
                randomBoolean() ? null : start,
                randomBoolean() ? null : randomLongBetween(start + 1, Long.MAX_VALUE)
            );
            case 1 -> new Request(
                DatafeedConfigTests.createRandomizedDatafeedConfig(jobId),
                randomBoolean() ? JobTests.buildJobBuilder(jobId) : null,
                randomBoolean() ? null : start,
                randomBoolean() ? null : randomLongBetween(start + 1, Long.MAX_VALUE)
            );
            case 2 -> new Request.Builder().setJobBuilder(
                JobTests.buildJobBuilder(jobId)
                    .setDatafeed(DatafeedConfigBuilderTests.createRandomizedDatafeedConfigBuilder(null, null, 3600000))
            ).build();
            default -> throw new IllegalArgumentException("Unexpected test state");
        };
    }

    @Override
    protected Request mutateInstance(Request instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    public void testCtor() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> new Request(null, randomLong(), null));
        assertThat(ex.getMessage(), equalTo("[datafeed_id] must not be null."));
    }

    public void testValidation() {
        String jobId = randomAlphaOfLength(10);
        Request.Builder requestBuilder = new Request.Builder().setDatafeedId(randomAlphaOfLength(10))
            .setDatafeedBuilder(new DatafeedConfig.Builder(DatafeedConfigTests.createRandomizedDatafeedConfig(jobId)))
            .setJobBuilder(randomBoolean() ? JobTests.buildJobBuilder(jobId) : null);
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, requestBuilder::build);
        assertThat(
            ex.getMessage(),
            containsString("[datafeed_id] cannot be supplied when either [job_config] or [datafeed_config] is present")
        );

        requestBuilder.setJobBuilder(null).setDatafeedId(null).setDatafeedBuilder(new DatafeedConfig.Builder());

        ex = expectThrows(IllegalArgumentException.class, requestBuilder::build);
        assertThat(ex.getMessage(), containsString("[datafeed_config.job_id] must be set or a [job_config] must be provided"));

        requestBuilder.setJobBuilder(
            JobTests.buildJobBuilder(jobId)
                .setDatafeed(new DatafeedConfig.Builder(DatafeedConfigTests.createRandomizedDatafeedConfig(jobId)))
        ).setDatafeedId(null).setDatafeedBuilder(new DatafeedConfig.Builder());
        ex = expectThrows(IllegalArgumentException.class, requestBuilder::build);
        assertThat(ex.getMessage(), containsString("[datafeed_config] must not be present when a [job_config.datafeed_config] is present"));

        requestBuilder.setJobBuilder(JobTests.buildJobBuilder(jobId)).setDatafeedId(null).setDatafeedBuilder(null);
        ex = expectThrows(IllegalArgumentException.class, requestBuilder::build);
        assertThat(ex.getMessage(), containsString("[datafeed_config] must be present when a [job_config.datafeed_config] is not present"));
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
