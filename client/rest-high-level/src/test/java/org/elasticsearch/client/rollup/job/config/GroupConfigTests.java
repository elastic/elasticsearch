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
import java.util.Optional;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class GroupConfigTests extends AbstractXContentTestCase<GroupConfig> {

    @Override
    protected GroupConfig createTestInstance() {
        return randomGroupConfig();
    }

    @Override
    protected GroupConfig doParseInstance(final XContentParser parser) throws IOException {
        return GroupConfig.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    public void testValidateNullDateHistogramGroupConfig() {
        final GroupConfig config = new GroupConfig(null);

        Optional<ValidationException> validation = config.validate();
        assertThat(validation, notNullValue());
        assertThat(validation.isPresent(), is(true));
        ValidationException validationException = validation.get();
        assertThat(validationException.validationErrors().size(), is(1));
        assertThat(validationException.validationErrors(), contains(is("Date histogram must not be null")));
    }

    public void testValidateDateHistogramGroupConfigWithErrors() {
        final DateHistogramGroupConfig dateHistogramGroupConfig = new DateHistogramGroupConfig(null, null, null, null);

        final GroupConfig config = new GroupConfig(dateHistogramGroupConfig);

        Optional<ValidationException> validation = config.validate();
        assertThat(validation, notNullValue());
        assertThat(validation.isPresent(), is(true));
        ValidationException validationException = validation.get();
        assertThat(validationException.validationErrors().size(), is(2));
        assertThat(validationException.validationErrors(),
            containsInAnyOrder("Field name is required", "Interval is required"));
    }

    public void testValidateHistogramGroupConfigWithErrors() {
        final HistogramGroupConfig histogramGroupConfig = new HistogramGroupConfig(0L);

        final GroupConfig config = new GroupConfig(randomGroupConfig().getDateHistogram(), histogramGroupConfig, null);

        Optional<ValidationException> validation = config.validate();
        assertThat(validation, notNullValue());
        assertThat(validation.isPresent(), is(true));
        ValidationException validationException = validation.get();
        assertThat(validationException.validationErrors().size(), is(2));
        assertThat(validationException.validationErrors(),
            containsInAnyOrder("Fields must have at least one value", "Interval must be a positive long"));
    }

    public void testValidateTermsGroupConfigWithErrors() {
        final TermsGroupConfig termsGroupConfig = new TermsGroupConfig();

        final GroupConfig config = new GroupConfig(randomGroupConfig().getDateHistogram(), null, termsGroupConfig);

        Optional<ValidationException> validation = config.validate();
        assertThat(validation, notNullValue());
        assertThat(validation.isPresent(), is(true));
        ValidationException validationException = validation.get();
        assertThat(validationException.validationErrors().size(), is(1));
        assertThat(validationException.validationErrors(), contains("Fields must have at least one value"));
    }

    public void testValidate() {
        final GroupConfig config = randomGroupConfig();

        Optional<ValidationException> validation = config.validate();
        assertThat(validation, notNullValue());
        assertThat(validation.isPresent(), is(false));
    }

    static GroupConfig randomGroupConfig() {
        DateHistogramGroupConfig dateHistogram = DateHistogramGroupConfigTests.randomDateHistogramGroupConfig();
        HistogramGroupConfig histogram = randomBoolean() ? HistogramGroupConfigTests.randomHistogramGroupConfig() : null;
        TermsGroupConfig terms = randomBoolean()  ? TermsGroupConfigTests.randomTermsGroupConfig() : null;
        return new GroupConfig(dateHistogram, histogram, terms);
    }
}
