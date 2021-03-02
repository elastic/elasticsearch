/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.rollup.job.config;

import org.elasticsearch.client.ValidationException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class MetricConfigTests extends AbstractXContentTestCase<MetricConfig> {

    @Override
    protected MetricConfig createTestInstance() {
        return randomMetricConfig();
    }

    @Override
    protected MetricConfig doParseInstance(final XContentParser parser) throws IOException {
        return MetricConfig.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    public void testValidateNullField() {
        final MetricConfig config = new MetricConfig(null, randomMetricConfig().getMetrics());

        Optional<ValidationException> validation = config.validate();
        assertThat(validation, notNullValue());
        assertThat(validation.isPresent(), is(true));
        ValidationException validationException = validation.get();
        assertThat(validationException.validationErrors().size(), is(1));
        assertThat(validationException.validationErrors(), contains(is("Field name is required")));
    }

    public void testValidateEmptyField() {
        final MetricConfig config = new MetricConfig("", randomMetricConfig().getMetrics());

        Optional<ValidationException> validation = config.validate();
        assertThat(validation, notNullValue());
        assertThat(validation.isPresent(), is(true));
        ValidationException validationException = validation.get();
        assertThat(validationException.validationErrors().size(), is(1));
        assertThat(validationException.validationErrors(), contains(is("Field name is required")));
    }

    public void testValidateNullListOfMetrics() {
        final MetricConfig config = new MetricConfig("field", null);

        Optional<ValidationException> validation = config.validate();
        assertThat(validation, notNullValue());
        assertThat(validation.isPresent(), is(true));
        ValidationException validationException = validation.get();
        assertThat(validationException.validationErrors().size(), is(1));
        assertThat(validationException.validationErrors(), contains(is("Metrics must be a non-null, non-empty array of strings")));
    }

    public void testValidateEmptyListOfMetrics() {
        final MetricConfig config = new MetricConfig("field", Collections.emptyList());

        Optional<ValidationException> validation = config.validate();
        assertThat(validation, notNullValue());
        assertThat(validation.isPresent(), is(true));
        ValidationException validationException = validation.get();
        assertThat(validationException.validationErrors().size(), is(1));
        assertThat(validationException.validationErrors(), contains(is("Metrics must be a non-null, non-empty array of strings")));
    }

    public void testValidate() {
        final MetricConfig config = randomMetricConfig();

        Optional<ValidationException> validation = config.validate();
        assertThat(validation, notNullValue());
        assertThat(validation.isPresent(), is(false));
    }

    static MetricConfig randomMetricConfig() {
        final List<String> metrics = new ArrayList<>();
        if (randomBoolean()) {
            metrics.add("min");
        }
        if (randomBoolean()) {
            metrics.add("max");
        }
        if (randomBoolean()) {
            metrics.add("sum");
        }
        if (randomBoolean()) {
            metrics.add("avg");
        }
        if (randomBoolean()) {
            metrics.add("value_count");
        }
        if (metrics.size() == 0) {
            metrics.add("min");
        }
        // large name so we don't accidentally collide
        return new MetricConfig(randomAlphaOfLengthBetween(15, 25), Collections.unmodifiableList(metrics));
    }
}
