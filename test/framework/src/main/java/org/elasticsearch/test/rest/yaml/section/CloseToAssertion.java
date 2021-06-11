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
import org.elasticsearch.core.Tuple;
import org.elasticsearch.common.xcontent.XContentLocation;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.closeTo;
import static org.junit.Assert.assertThat;

/**
 * Represents a close_to assert section:
 *
 *   - close_to:   { get.fields._routing: { value: 5.1, error: 0.00001 } }
 *
 */
public class CloseToAssertion extends Assertion {
    public static CloseToAssertion parse(XContentParser parser) throws IOException {
        XContentLocation location = parser.getTokenLocation();
        Tuple<String,Object> fieldValueTuple = ParserUtils.parseTuple(parser);
        if (fieldValueTuple.v2() instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) fieldValueTuple.v2();
            if (map.size() != 2) {
                throw new IllegalArgumentException("expected a map with value and error but got a map with " + map.size() + " fields");
            }
            Object valObj = map.get("value");
            if (valObj instanceof Number == false) {
                throw new IllegalArgumentException("value is missing or not a number");
            }
            Object errObj = map.get("error");
            if (errObj instanceof Number == false) {
                throw new IllegalArgumentException("error is missing or not a number");
            }
            return new CloseToAssertion(location, fieldValueTuple.v1(), ((Number)valObj).doubleValue(), ((Number)errObj).doubleValue());
        } else {
            throw new IllegalArgumentException("expected a map with value and error but got " +
                fieldValueTuple.v2().getClass().getSimpleName());
        }

    }

    private static final Logger logger = LogManager.getLogger(CloseToAssertion.class);

    private final double error;

    public CloseToAssertion(XContentLocation location, String field, Double expectedValue, Double error) {
        super(location, field, expectedValue);
        this.error = error;
    }

    public final double getError() {
        return error;
    }

    @Override
    protected void doAssert(Object actualValue, Object expectedValue) {
        logger.trace("assert that [{}] is close to [{}] with error [{}] (field [{}])", actualValue, expectedValue, error, getField());
        if (actualValue instanceof Number) {
            assertThat(((Number) actualValue).doubleValue(), closeTo((Double) expectedValue, error));
        } else {
            throw new AssertionError("excpected a value close to " + expectedValue + " but got " + actualValue + ", which is not a number");
        }
    }
}
