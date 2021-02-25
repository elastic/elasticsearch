/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.rollup.job.config;

import org.elasticsearch.client.ValidationException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.Optional;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class TermsGroupConfigTests extends AbstractXContentTestCase<TermsGroupConfig> {

    @Override
    protected TermsGroupConfig createTestInstance() {
        return randomTermsGroupConfig();
    }

    @Override
    protected TermsGroupConfig doParseInstance(final XContentParser parser) throws IOException {
        return TermsGroupConfig.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    public void testValidateNullFields() {
        final TermsGroupConfig config = new TermsGroupConfig();

        Optional<ValidationException> validation = config.validate();
        assertThat(validation, notNullValue());
        assertThat(validation.isPresent(), is(true));
        ValidationException validationException = validation.get();
        assertThat(validationException.validationErrors().size(), is(1));
        assertThat(validationException.validationErrors(), contains(is("Fields must have at least one value")));
    }

    public void testValidatEmptyFields() {
        final TermsGroupConfig config = new TermsGroupConfig(Strings.EMPTY_ARRAY);

        Optional<ValidationException> validation = config.validate();
        assertThat(validation, notNullValue());
        assertThat(validation.isPresent(), is(true));
        ValidationException validationException = validation.get();
        assertThat(validationException.validationErrors().size(), is(1));
        assertThat(validationException.validationErrors(), contains(is("Fields must have at least one value")));
    }

    public void testValidate() {
        final TermsGroupConfig config = randomTermsGroupConfig();

        Optional<ValidationException> validation = config.validate();
        assertThat(validation, notNullValue());
        assertThat(validation.isPresent(), is(false));
    }

    static TermsGroupConfig randomTermsGroupConfig() {
        final String[] fields = new String[randomIntBetween(1, 10)];
        for (int i = 0; i < fields.length; i++) {
            fields[i] = randomAlphaOfLength(randomIntBetween(3, 10));
        }
        return new TermsGroupConfig(fields);
    }
}
