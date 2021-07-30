/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.test.rest.yaml.section;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.xcontent.XContentLocation;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.equalToIgnoringCase;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

/**
 * Represents an is_true assert section:
 *
 *   - is_true:  get.fields.bar
 *
 */
public class IsTrueAssertion extends Assertion {
    public static IsTrueAssertion parse(XContentParser parser) throws IOException {
        return new IsTrueAssertion(parser.getTokenLocation(), ParserUtils.parseField(parser));
    }

    private static final Logger logger = LogManager.getLogger(IsTrueAssertion.class);

    public IsTrueAssertion(XContentLocation location, String field) {
        super(location, field, true);
    }

    @Override
    protected void doAssert(Object actualValue, Object expectedValue) {
        logger.trace("assert that [{}] has a true value (field [{}])", actualValue, getField());
        String errorMessage = errorMessage();
        assertThat(errorMessage, actualValue, notNullValue());
        String actualString = actualValue.toString();
        assertThat(errorMessage, actualString, not(equalTo("")));
        assertThat(errorMessage, actualString, not(equalToIgnoringCase(Boolean.FALSE.toString())));
        assertThat(errorMessage, actualString, not(equalTo("0")));
    }

    private String errorMessage() {
        return "field [" + getField() + "] doesn't have a true value";
    }
}
