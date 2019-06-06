/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.rollup.job.config;

import org.elasticsearch.client.ValidationException;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class RollupJobConfigTests extends AbstractXContentTestCase<RollupJobConfig> {

    private String id;

    @Before
    public void setUpOptionalId() {
        id = randomAlphaOfLengthBetween(1, 10);
    }

    @Override
    protected RollupJobConfig createTestInstance() {
        return randomRollupJobConfig(id);
    }

    @Override
    protected RollupJobConfig doParseInstance(final XContentParser parser) throws IOException {
        return RollupJobConfig.fromXContent(parser, randomBoolean() ? id : null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    public void testValidateNullId() {
        final RollupJobConfig sample = randomRollupJobConfig(id);

        final RollupJobConfig config = new RollupJobConfig(null, sample.getIndexPattern(), sample.getRollupIndex(), sample.getCron(),
            sample.getPageSize(), sample.getGroupConfig(), sample.getMetricsConfig(), sample.getTimeout());

        Optional<ValidationException> validation = config.validate();
        assertThat(validation, notNullValue());
        assertThat(validation.isPresent(), is(true));
        ValidationException validationException = validation.get();
        assertThat(validationException.validationErrors().size(), is(1));
        assertThat(validationException.validationErrors(), contains("Id must be a non-null, non-empty string"));
    }

    public void testValidateEmptyId() {
        final RollupJobConfig sample = randomRollupJobConfig(id);

        final RollupJobConfig config = new RollupJobConfig("", sample.getIndexPattern(), sample.getRollupIndex(), sample.getCron(),
            sample.getPageSize(), sample.getGroupConfig(), sample.getMetricsConfig(), sample.getTimeout());

        Optional<ValidationException> validation = config.validate();
        assertThat(validation, notNullValue());
        assertThat(validation.isPresent(), is(true));
        ValidationException validationException = validation.get();
        assertThat(validationException.validationErrors().size(), is(1));
        assertThat(validationException.validationErrors(), contains("Id must be a non-null, non-empty string"));
    }

    public void testValidateNullIndexPattern() {
        final RollupJobConfig sample = randomRollupJobConfig(id);

        final RollupJobConfig config = new RollupJobConfig(sample.getId(), null, sample.getRollupIndex(), sample.getCron(),
            sample.getPageSize(), sample.getGroupConfig(), sample.getMetricsConfig(), sample.getTimeout());

        Optional<ValidationException> validation = config.validate();
        assertThat(validation, notNullValue());
        assertThat(validation.isPresent(), is(true));
        ValidationException validationException = validation.get();
        assertThat(validationException.validationErrors().size(), is(1));
        assertThat(validationException.validationErrors(), contains("Index pattern must be a non-null, non-empty string"));
    }

    public void testValidateEmptyIndexPattern() {
        final RollupJobConfig sample = randomRollupJobConfig(id);

        final RollupJobConfig config = new RollupJobConfig(sample.getId(), "", sample.getRollupIndex(), sample.getCron(),
            sample.getPageSize(), sample.getGroupConfig(), sample.getMetricsConfig(), sample.getTimeout());

        Optional<ValidationException> validation = config.validate();
        assertThat(validation, notNullValue());
        assertThat(validation.isPresent(), is(true));
        ValidationException validationException = validation.get();
        assertThat(validationException.validationErrors().size(), is(1));
        assertThat(validationException.validationErrors(), contains("Index pattern must be a non-null, non-empty string"));
    }

    public void testValidateMatchAllIndexPattern() {
        final RollupJobConfig sample = randomRollupJobConfig(id);

        final RollupJobConfig config = new RollupJobConfig(sample.getId(), "*", sample.getRollupIndex(), sample.getCron(),
            sample.getPageSize(), sample.getGroupConfig(), sample.getMetricsConfig(), sample.getTimeout());

        Optional<ValidationException> validation = config.validate();
        assertThat(validation, notNullValue());
        assertThat(validation.isPresent(), is(true));
        ValidationException validationException = validation.get();
        assertThat(validationException.validationErrors().size(), is(1));
        assertThat(validationException.validationErrors(),
            contains("Index pattern must not match all indices (as it would match it's own rollup index"));
    }

    public void testValidateIndexPatternMatchesRollupIndex() {
        final RollupJobConfig sample = randomRollupJobConfig(id);

        final RollupJobConfig config = new RollupJobConfig(sample.getId(), "rollup*", "rollup", sample.getCron(),
            sample.getPageSize(), sample.getGroupConfig(), sample.getMetricsConfig(), sample.getTimeout());

        Optional<ValidationException> validation = config.validate();
        assertThat(validation, notNullValue());
        assertThat(validation.isPresent(), is(true));
        ValidationException validationException = validation.get();
        assertThat(validationException.validationErrors().size(), is(1));
        assertThat(validationException.validationErrors(), contains("Index pattern would match rollup index name which is not allowed"));
    }

    public void testValidateSameIndexAndRollupPatterns() {
        final RollupJobConfig sample = randomRollupJobConfig(id);

        final RollupJobConfig config = new RollupJobConfig(sample.getId(), "test", "test", sample.getCron(),
            sample.getPageSize(), sample.getGroupConfig(), sample.getMetricsConfig(), sample.getTimeout());

        Optional<ValidationException> validation = config.validate();
        assertThat(validation, notNullValue());
        assertThat(validation.isPresent(), is(true));
        ValidationException validationException = validation.get();
        assertThat(validationException.validationErrors().size(), is(1));
        assertThat(validationException.validationErrors(), contains("Rollup index may not be the same as the index pattern"));
    }

    public void testValidateNullRollupPattern() {
        final RollupJobConfig sample = randomRollupJobConfig(id);

        final RollupJobConfig config = new RollupJobConfig(sample.getId(), sample.getIndexPattern(), null, sample.getCron(),
            sample.getPageSize(), sample.getGroupConfig(), sample.getMetricsConfig(), sample.getTimeout());

        Optional<ValidationException> validation = config.validate();
        assertThat(validation, notNullValue());
        assertThat(validation.isPresent(), is(true));
        ValidationException validationException = validation.get();
        assertThat(validationException.validationErrors().size(), is(1));
        assertThat(validationException.validationErrors(), contains("Rollup index must be a non-null, non-empty string"));
    }

    public void testValidateEmptyRollupPattern() {
        final RollupJobConfig sample = randomRollupJobConfig(id);

        final RollupJobConfig config = new RollupJobConfig(sample.getId(), sample.getIndexPattern(), "", sample.getCron(),
            sample.getPageSize(), sample.getGroupConfig(), sample.getMetricsConfig(), sample.getTimeout());

        Optional<ValidationException> validation = config.validate();
        assertThat(validation, notNullValue());
        assertThat(validation.isPresent(), is(true));
        ValidationException validationException = validation.get();
        assertThat(validationException.validationErrors().size(), is(1));
        assertThat(validationException.validationErrors(), contains("Rollup index must be a non-null, non-empty string"));
    }

    public void testValidateNullCron() {
        final RollupJobConfig sample = randomRollupJobConfig(id);

        final RollupJobConfig config = new RollupJobConfig(sample.getId(), sample.getIndexPattern(), sample.getRollupIndex(), null,
            sample.getPageSize(), sample.getGroupConfig(), sample.getMetricsConfig(), sample.getTimeout());

        Optional<ValidationException> validation = config.validate();
        assertThat(validation, notNullValue());
        assertThat(validation.isPresent(), is(true));
        ValidationException validationException = validation.get();
        assertThat(validationException.validationErrors().size(), is(1));
        assertThat(validationException.validationErrors(), contains("Cron schedule must be a non-null, non-empty string"));
    }

    public void testValidateEmptyCron() {
        final RollupJobConfig sample = randomRollupJobConfig(id);

        final RollupJobConfig config = new RollupJobConfig(sample.getId(), sample.getIndexPattern(), sample.getRollupIndex(), "",
            sample.getPageSize(), sample.getGroupConfig(), sample.getMetricsConfig(), sample.getTimeout());

        Optional<ValidationException> validation = config.validate();
        assertThat(validation, notNullValue());
        assertThat(validation.isPresent(), is(true));
        ValidationException validationException = validation.get();
        assertThat(validationException.validationErrors().size(), is(1));
        assertThat(validationException.validationErrors(), contains("Cron schedule must be a non-null, non-empty string"));
    }

    public void testValidatePageSize() {
        final RollupJobConfig sample = randomRollupJobConfig(id);

        final RollupJobConfig config = new RollupJobConfig(sample.getId(), sample.getIndexPattern(), sample.getRollupIndex(),
            sample.getCron(), 0, sample.getGroupConfig(), sample.getMetricsConfig(), sample.getTimeout());

        Optional<ValidationException> validation = config.validate();
        assertThat(validation, notNullValue());
        assertThat(validation.isPresent(), is(true));
        ValidationException validationException = validation.get();
        assertThat(validationException.validationErrors().size(), is(1));
        assertThat(validationException.validationErrors(), contains("Page size is mandatory and  must be a positive long"));
    }

    public void testValidateGroupOrMetrics() {
        final RollupJobConfig sample = randomRollupJobConfig(id);

        final RollupJobConfig config = new RollupJobConfig(sample.getId(), sample.getIndexPattern(), sample.getRollupIndex(),
            sample.getCron(), sample.getPageSize(), null, null, sample.getTimeout());

        Optional<ValidationException> validation = config.validate();
        assertThat(validation, notNullValue());
        assertThat(validation.isPresent(), is(true));
        ValidationException validationException = validation.get();
        assertThat(validationException.validationErrors().size(), is(1));
        assertThat(validationException.validationErrors(), contains("At least one grouping or metric must be configured"));
    }

    public void testValidateGroupConfigWithErrors() {
        final GroupConfig groupConfig = new GroupConfig(null);

        final RollupJobConfig sample = randomRollupJobConfig(id);
        final RollupJobConfig config = new RollupJobConfig(sample.getId(), sample.getIndexPattern(), sample.getRollupIndex(),
            sample.getCron(), sample.getPageSize(), groupConfig, sample.getMetricsConfig(), sample.getTimeout());

        Optional<ValidationException> validation = config.validate();
        assertThat(validation, notNullValue());
        assertThat(validation.isPresent(), is(true));
        ValidationException validationException = validation.get();
        assertThat(validationException.validationErrors().size(), is(1));
        assertThat(validationException.validationErrors(), contains("Date histogram must not be null"));
    }

    public void testValidateListOfMetricsWithErrors() {
        final List<MetricConfig> metricsConfigs = singletonList(new MetricConfig(null, null));

        final RollupJobConfig sample = randomRollupJobConfig(id);
        final RollupJobConfig config = new RollupJobConfig(sample.getId(), sample.getIndexPattern(), sample.getRollupIndex(),
            sample.getCron(), sample.getPageSize(), sample.getGroupConfig(), metricsConfigs, sample.getTimeout());

        Optional<ValidationException> validation = config.validate();
        assertThat(validation, notNullValue());
        assertThat(validation.isPresent(), is(true));
        ValidationException validationException = validation.get();
        assertThat(validationException.validationErrors().size(), is(2));
        assertThat(validationException.validationErrors(),
            containsInAnyOrder("Field name is required", "Metrics must be a non-null, non-empty array of strings"));
    }

    public static RollupJobConfig randomRollupJobConfig(final String id) {
        final String indexPattern = randomAlphaOfLengthBetween(5, 20);
        final String rollupIndex = "rollup_" + indexPattern;
        final String cron = randomCron();
        final int pageSize = randomIntBetween(1, 100);
        final TimeValue timeout = randomBoolean() ? null :
            new TimeValue(randomIntBetween(0, 60), randomFrom(Arrays.asList(TimeUnit.MILLISECONDS, TimeUnit.SECONDS, TimeUnit.MINUTES)));
        final GroupConfig groups = GroupConfigTests.randomGroupConfig();

        final List<MetricConfig> metrics = new ArrayList<>();
        if (randomBoolean()) {
            final int numMetrics = randomIntBetween(1, 10);
            for (int i = 0; i < numMetrics; i++) {
                metrics.add(MetricConfigTests.randomMetricConfig());
            }
        }
        return new RollupJobConfig(id, indexPattern, rollupIndex, cron, pageSize, groups, unmodifiableList(metrics), timeout);
    }

    private static String randomCron() {
        return (ESTestCase.randomBoolean() ? "*" : String.valueOf(ESTestCase.randomIntBetween(0, 59)))         + //second
            " " + (ESTestCase.randomBoolean() ? "*" : String.valueOf(ESTestCase.randomIntBetween(0, 59)))      + //minute
            " " + (ESTestCase.randomBoolean() ? "*" : String.valueOf(ESTestCase.randomIntBetween(0, 23)))      + //hour
            " " + (ESTestCase.randomBoolean() ? "*" : String.valueOf(ESTestCase.randomIntBetween(1, 31)))      + //day of month
            " " + (ESTestCase.randomBoolean() ? "*" : String.valueOf(ESTestCase.randomIntBetween(1, 12)))      + //month
            " ?"                                                                                               + //day of week
            " " + (ESTestCase.randomBoolean() ? "*" : String.valueOf(ESTestCase.randomIntBetween(1970, 2199)));  //year
    }
}
