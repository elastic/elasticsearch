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
