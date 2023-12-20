/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.rollup.job;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.junit.Before;

import java.io.IOException;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.core.rollup.ConfigTestHelpers.randomRollupJobConfig;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;

public class RollupJobConfigTests extends AbstractXContentSerializingTestCase<RollupJobConfig> {

    private String jobId;

    @Before
    public void setUpOptionalId() {
        jobId = randomAlphaOfLengthBetween(1, 10);
    }

    @Override
    protected RollupJobConfig createTestInstance() {
        return randomRollupJobConfig(random(), jobId);
    }

    @Override
    protected RollupJobConfig mutateInstance(RollupJobConfig instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<RollupJobConfig> instanceReader() {
        return RollupJobConfig::new;
    }

    @Override
    protected RollupJobConfig doParseInstance(final XContentParser parser) throws IOException {
        if (randomBoolean()) {
            return RollupJobConfig.fromXContent(parser, jobId);
        } else {
            return RollupJobConfig.fromXContent(parser, null);
        }
    }

    public void testEmptyIndexPattern() {
        final RollupJobConfig sample = randomRollupJobConfig(random());

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new RollupJobConfig(
                sample.getId(),
                null,
                sample.getRollupIndex(),
                sample.getCron(),
                sample.getPageSize(),
                sample.getGroupConfig(),
                sample.getMetricsConfig(),
                sample.getTimeout()
            )
        );
        assertThat(e.getMessage(), equalTo("Index pattern must be a non-null, non-empty string"));

        e = expectThrows(
            IllegalArgumentException.class,
            () -> new RollupJobConfig(
                sample.getId(),
                "",
                sample.getRollupIndex(),
                sample.getCron(),
                sample.getPageSize(),
                sample.getGroupConfig(),
                sample.getMetricsConfig(),
                sample.getTimeout()
            )
        );
        assertThat(e.getMessage(), equalTo("Index pattern must be a non-null, non-empty string"));
    }

    public void testEmptyCron() {
        final RollupJobConfig sample = randomRollupJobConfig(random());

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new RollupJobConfig(
                sample.getId(),
                sample.getIndexPattern(),
                sample.getRollupIndex(),
                null,
                sample.getPageSize(),
                sample.getGroupConfig(),
                sample.getMetricsConfig(),
                sample.getTimeout()
            )
        );
        assertThat(e.getMessage(), equalTo("Cron schedule must be a non-null, non-empty string"));

        e = expectThrows(
            IllegalArgumentException.class,
            () -> new RollupJobConfig(
                sample.getId(),
                sample.getIndexPattern(),
                sample.getRollupIndex(),
                "",
                sample.getPageSize(),
                sample.getGroupConfig(),
                sample.getMetricsConfig(),
                sample.getTimeout()
            )
        );
        assertThat(e.getMessage(), equalTo("Cron schedule must be a non-null, non-empty string"));
    }

    public void testEmptyID() {
        final RollupJobConfig sample = randomRollupJobConfig(random());

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new RollupJobConfig(
                null,
                sample.getIndexPattern(),
                sample.getRollupIndex(),
                sample.getCron(),
                sample.getPageSize(),
                sample.getGroupConfig(),
                sample.getMetricsConfig(),
                sample.getTimeout()
            )
        );
        assertThat(e.getMessage(), equalTo("Id must be a non-null, non-empty string"));

        e = expectThrows(
            IllegalArgumentException.class,
            () -> new RollupJobConfig(
                "",
                sample.getIndexPattern(),
                sample.getRollupIndex(),
                sample.getCron(),
                sample.getPageSize(),
                sample.getGroupConfig(),
                sample.getMetricsConfig(),
                sample.getTimeout()
            )
        );
        assertThat(e.getMessage(), equalTo("Id must be a non-null, non-empty string"));
    }

    public void testMatchAllIndexPattern() {
        final RollupJobConfig sample = randomRollupJobConfig(random());

        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> new RollupJobConfig(
                sample.getId(),
                "*",
                sample.getRollupIndex(),
                sample.getCron(),
                sample.getPageSize(),
                sample.getGroupConfig(),
                sample.getMetricsConfig(),
                sample.getTimeout()
            )
        );
        assertThat(e.getMessage(), equalTo("Index pattern must not match all indices (as it would match it's own rollup index"));

        e = expectThrows(
            IllegalArgumentException.class,
            () -> new RollupJobConfig(
                sample.getId(),
                "test,*",
                sample.getRollupIndex(),
                sample.getCron(),
                sample.getPageSize(),
                sample.getGroupConfig(),
                sample.getMetricsConfig(),
                sample.getTimeout()
            )
        );
        assertThat(e.getMessage(), equalTo("Index pattern must not match all indices (as it would match it's own rollup index"));
    }

    public void testMatchOwnRollupPatternPrefix() {
        final RollupJobConfig sample = randomRollupJobConfig(random());

        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> new RollupJobConfig(
                sample.getId(),
                "foo-*",
                "foo-rollup",
                sample.getCron(),
                sample.getPageSize(),
                sample.getGroupConfig(),
                sample.getMetricsConfig(),
                sample.getTimeout()
            )
        );
        assertThat(e.getMessage(), equalTo("Index pattern would match rollup index name which is not allowed"));

        e = expectThrows(
            IllegalArgumentException.class,
            () -> new RollupJobConfig(
                sample.getId(),
                "test,foo-*",
                "foo-rollup",
                sample.getCron(),
                sample.getPageSize(),
                sample.getGroupConfig(),
                sample.getMetricsConfig(),
                sample.getTimeout()
            )
        );
        assertThat(e.getMessage(), equalTo("Index pattern would match rollup index name which is not allowed"));
    }

    public void testMatchOwnRollupPatternSuffix() {
        final RollupJobConfig sample = randomRollupJobConfig(random());

        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> new RollupJobConfig(
                sample.getId(),
                "*-rollup",
                "foo-rollup",
                sample.getCron(),
                sample.getPageSize(),
                sample.getGroupConfig(),
                sample.getMetricsConfig(),
                sample.getTimeout()
            )
        );
        assertThat(e.getMessage(), equalTo("Index pattern would match rollup index name which is not allowed"));

        e = expectThrows(
            IllegalArgumentException.class,
            () -> new RollupJobConfig(
                sample.getId(),
                "test,*-rollup",
                "foo-rollup",
                sample.getCron(),
                sample.getPageSize(),
                sample.getGroupConfig(),
                sample.getMetricsConfig(),
                sample.getTimeout()
            )
        );
        assertThat(e.getMessage(), equalTo("Index pattern would match rollup index name which is not allowed"));
    }

    public void testIndexPatternIdenticalToRollup() {
        final RollupJobConfig sample = randomRollupJobConfig(random());

        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> new RollupJobConfig(
                sample.getId(),
                "foo",
                "foo",
                sample.getCron(),
                sample.getPageSize(),
                sample.getGroupConfig(),
                sample.getMetricsConfig(),
                sample.getTimeout()
            )
        );
        assertThat(e.getMessage(), equalTo("Rollup index may not be the same as the index pattern"));

        e = expectThrows(
            IllegalArgumentException.class,
            () -> new RollupJobConfig(
                sample.getId(),
                "test,foo",
                "foo",
                sample.getCron(),
                sample.getPageSize(),
                sample.getGroupConfig(),
                sample.getMetricsConfig(),
                sample.getTimeout()
            )
        );
        assertThat(e.getMessage(), equalTo("Rollup index may not be the same as the index pattern"));
    }

    public void testEmptyRollupIndex() {
        final RollupJobConfig sample = randomRollupJobConfig(random());
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> new RollupJobConfig(
                sample.getId(),
                sample.getIndexPattern(),
                "",
                sample.getCron(),
                sample.getPageSize(),
                sample.getGroupConfig(),
                sample.getMetricsConfig(),
                sample.getTimeout()
            )
        );
        assertThat(e.getMessage(), equalTo("Rollup index must be a non-null, non-empty string"));

        e = expectThrows(
            IllegalArgumentException.class,
            () -> new RollupJobConfig(
                sample.getId(),
                sample.getIndexPattern(),
                null,
                sample.getCron(),
                sample.getPageSize(),
                sample.getGroupConfig(),
                sample.getMetricsConfig(),
                sample.getTimeout()
            )
        );
        assertThat(e.getMessage(), equalTo("Rollup index must be a non-null, non-empty string"));
    }

    public void testBadSize() {
        final RollupJobConfig sample = randomRollupJobConfig(random());

        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> new RollupJobConfig(
                sample.getId(),
                sample.getIndexPattern(),
                sample.getRollupIndex(),
                sample.getCron(),
                -1,
                sample.getGroupConfig(),
                sample.getMetricsConfig(),
                sample.getTimeout()
            )
        );
        assertThat(e.getMessage(), equalTo("Page size is mandatory and  must be a positive long"));

        e = expectThrows(
            IllegalArgumentException.class,
            () -> new RollupJobConfig(
                sample.getId(),
                sample.getIndexPattern(),
                sample.getRollupIndex(),
                sample.getCron(),
                0,
                sample.getGroupConfig(),
                sample.getMetricsConfig(),
                sample.getTimeout()
            )
        );
        assertThat(e.getMessage(), equalTo("Page size is mandatory and  must be a positive long"));
    }

    public void testEmptyGroupAndMetrics() {
        final RollupJobConfig sample = randomRollupJobConfig(random());

        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> new RollupJobConfig(
                sample.getId(),
                sample.getIndexPattern(),
                sample.getRollupIndex(),
                sample.getCron(),
                sample.getPageSize(),
                null,
                null,
                sample.getTimeout()
            )
        );
        assertThat(e.getMessage(), equalTo("At least one grouping or metric must be configured"));

        e = expectThrows(
            IllegalArgumentException.class,
            () -> new RollupJobConfig(
                sample.getId(),
                sample.getIndexPattern(),
                sample.getRollupIndex(),
                sample.getCron(),
                sample.getPageSize(),
                null,
                emptyList(),
                sample.getTimeout()
            )
        );
        assertThat(e.getMessage(), equalTo("At least one grouping or metric must be configured"));
    }

    public void testIndices() {
        final RollupJobConfig sample = randomRollupJobConfig(random());

        RollupJobConfig config = new RollupJobConfig(
            sample.getId(),
            "foo",
            sample.getRollupIndex(),
            sample.getCron(),
            sample.getPageSize(),
            sample.getGroupConfig(),
            sample.getMetricsConfig(),
            sample.getTimeout()
        );
        assertThat(config.indices(), arrayContaining("foo"));

        config = new RollupJobConfig(
            sample.getId(),
            "foo,bar",
            sample.getRollupIndex(),
            sample.getCron(),
            sample.getPageSize(),
            sample.getGroupConfig(),
            sample.getMetricsConfig(),
            sample.getTimeout()
        );
        assertThat(config.indices(), arrayContaining("foo", "bar"));
    }
}
