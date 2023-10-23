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

import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

/**
 * Represents an exists assert section:
 * <p>
 * - not_exists:  get.fields.bar
 */
public class NotExistsAssertion extends Assertion {

    private static final Logger logger = LogManager.getLogger(NotExistsAssertion.class);

    public static NotExistsAssertion parse(XContentParser parser) throws IOException {
        return new NotExistsAssertion(parser.getTokenLocation(), ParserUtils.parseField(parser));
    }

    public NotExistsAssertion(XContentLocation location, String field) {
        super(location, field, false /* not used */);
    }

    @Override
    protected void doAssert(Object actualValue, Object expectedValue) {
        logger.trace("assert that field [{}] does not exists with any value", getField());
        String errorMessage = errorMessage();
        assertThat(errorMessage, actualValue, nullValue());
    }

    private String errorMessage() {
        return "field [" + getField() + "] exists, but should not.";
    }
}
