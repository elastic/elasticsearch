/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.rollup.job;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.rollup.job.RollupJobConfig;
import org.elasticsearch.xpack.core.rollup.ConfigTestHelpers;

import static org.hamcrest.Matchers.equalTo;


public class RollupJobConfigTests extends AbstractSerializingTestCase<RollupJobConfig> {

    @Override
    protected RollupJobConfig createTestInstance() {
        return ConfigTestHelpers.getRollupJob(randomAlphaOfLengthBetween(1,10)).build();
    }

    @Override
    protected Writeable.Reader<RollupJobConfig> instanceReader() {
        return RollupJobConfig::new;
    }

    @Override
    protected RollupJobConfig doParseInstance(XContentParser parser) {
        return RollupJobConfig.PARSER.apply(parser, null).build();
    }

    public void testEmptyIndexPattern() {
        RollupJobConfig.Builder builder = ConfigTestHelpers.getRollupJob(randomAlphaOfLengthBetween(1, 10));
        builder.setIndexPattern(null);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, builder::build);
        assertThat(e.getMessage(), equalTo("An index pattern is mandatory."));

        builder = ConfigTestHelpers.getRollupJob(randomAlphaOfLengthBetween(1, 10));
        builder.setIndexPattern("");
        e = expectThrows(IllegalArgumentException.class, builder::build);
        assertThat(e.getMessage(), equalTo("An index pattern is mandatory."));
    }

    public void testEmptyCron() {
        RollupJobConfig.Builder builder = ConfigTestHelpers.getRollupJob(randomAlphaOfLengthBetween(1, 10));
        builder.setCron(null);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, builder::build);
        assertThat(e.getMessage(), equalTo("A cron schedule is mandatory."));

        builder = ConfigTestHelpers.getRollupJob(randomAlphaOfLengthBetween(1, 10));
        builder.setCron("");
        e = expectThrows(IllegalArgumentException.class, builder::build);
        assertThat(e.getMessage(), equalTo("A cron schedule is mandatory."));
    }

    public void testEmptyID() {
        RollupJobConfig.Builder builder = ConfigTestHelpers.getRollupJob(randomAlphaOfLengthBetween(1, 10));
        builder.setId(null);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, builder::build);
        assertThat(e.getMessage(), equalTo("An ID is mandatory."));

        builder = ConfigTestHelpers.getRollupJob(randomAlphaOfLengthBetween(1, 10));
        builder.setId("");
        e = expectThrows(IllegalArgumentException.class, builder::build);
        assertThat(e.getMessage(), equalTo("An ID is mandatory."));
    }
}
