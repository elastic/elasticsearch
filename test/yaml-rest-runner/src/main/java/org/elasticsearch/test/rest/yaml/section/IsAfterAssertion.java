/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.rest.yaml.section;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeParseException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Represents an is after assert section:
 *
 *    - is_after: { result.some_instant: "2023-05-25T12:30:00.000Z" }
 *
 */
public class IsAfterAssertion extends Assertion {

    public static IsAfterAssertion parse(XContentParser parser) throws IOException {
        XContentLocation location = parser.getTokenLocation();
        Tuple<String, Object> stringObjectTuple = ParserUtils.parseTuple(parser);
        return new IsAfterAssertion(location, stringObjectTuple.v1(), stringObjectTuple.v2());
    }

    private static final Logger logger = LogManager.getLogger(IsAfterAssertion.class);

    public IsAfterAssertion(XContentLocation location, String field, Object expectedValue) {
        super(location, field, expectedValue);
    }

    @Override
    protected void doAssert(Object actualValue, Object expectedValue) {
        assertNotNull("field [" + getField() + "] is null", actualValue);
        assertNotNull("value to test against cannot be null", expectedValue);

        Instant fieldInstant = parseToInstant(
            actualValue.toString(),
            "field [" + getField() + "] cannot be parsed to " + Instant.class.getSimpleName() + ", got [" + actualValue + "]"
        );
        Instant valueInstant = parseToInstant(
            expectedValue.toString(),
            "value to test against [" + expectedValue + "] cannot be parsed to " + Instant.class.getSimpleName()
        );

        logger.trace("assert that [{}] is after [{}] (field [{}])", fieldInstant, valueInstant);
        assertTrue("field [" + getField() + "] should be after [" + actualValue + "], but was not", fieldInstant.isAfter(valueInstant));
    }

    private Instant parseToInstant(String string, String onErrorMessage) {
        try {
            return Instant.parse(string);
        } catch (DateTimeParseException e) {
            throw new AssertionError(onErrorMessage, e);
        }
    }
}
