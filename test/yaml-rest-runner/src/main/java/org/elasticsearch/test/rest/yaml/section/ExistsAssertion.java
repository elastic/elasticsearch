/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.rest.yaml.section;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

/**
 * Represents an exists assert section:
 *
 *   - exists:  get.fields.bar
 *
 */
public class ExistsAssertion extends Assertion {

    private static final Logger logger = LogManager.getLogger(ExistsAssertion.class);

    public static ExistsAssertion parse(XContentParser parser) throws IOException {
        return new ExistsAssertion(parser.getTokenLocation(), ParserUtils.parseField(parser));
    }

    public ExistsAssertion(XContentLocation location, String field) {
        super(location, field, true);
    }

    @Override
    protected void doAssert(Object actualValue, Object expectedValue) {
        logger.trace("assert that field [{}] exists with any value", getField());
        String errorMessage = errorMessage();
        assertThat(errorMessage, actualValue, notNullValue());
    }

    private String errorMessage() {
        return "field [" + getField() + "] does not exist";
    }
}
