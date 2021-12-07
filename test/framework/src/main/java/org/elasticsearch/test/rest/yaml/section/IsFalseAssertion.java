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
import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.equalToIgnoringCase;
import static org.junit.Assert.assertThat;

/**
 * Represents an is_false assert section:
 *
 *   - is_false:  get.fields.bar
 *
 */
public class IsFalseAssertion extends Assertion {
    public static IsFalseAssertion parse(XContentParser parser) throws IOException {
        return new IsFalseAssertion(parser.getTokenLocation(), ParserUtils.parseField(parser));
    }

    private static final Logger logger = LogManager.getLogger(IsFalseAssertion.class);

    public IsFalseAssertion(XContentLocation location, String field) {
        super(location, field, false);
    }

    @Override
    protected void doAssert(Object actualValue, Object expectedValue) {
        logger.trace("assert that [{}] doesn't have a true value (field: [{}])", actualValue, getField());

        if (actualValue == null) {
            return;
        }

        String actualString = actualValue.toString();
        assertThat(errorMessage(), actualString, anyOf(equalTo(""), equalToIgnoringCase(Boolean.FALSE.toString()), equalTo("0")));
    }

    private String errorMessage() {
        return "field [" + getField() + "] has a true value but it shouldn't";
    }
}
