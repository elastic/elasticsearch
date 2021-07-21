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

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;

/**
 * Represents a lt assert section:
 *
 *  - lt:    { fields._ttl: 20000}
 *
 */
public class LessThanAssertion extends Assertion {
    public static LessThanAssertion parse(XContentParser parser) throws IOException {
        XContentLocation location = parser.getTokenLocation();
        Tuple<String,Object> stringObjectTuple = ParserUtils.parseTuple(parser);
        if (false == stringObjectTuple.v2() instanceof Comparable) {
            throw new IllegalArgumentException("lt section can only be used with objects that support natural ordering, found "
                    + stringObjectTuple.v2().getClass().getSimpleName());
        }
        return new LessThanAssertion(location, stringObjectTuple.v1(), stringObjectTuple.v2());
    }

    private static final Logger logger = LogManager.getLogger(LessThanAssertion.class);

    public LessThanAssertion(XContentLocation location, String field, Object expectedValue) {
        super(location, field, expectedValue);
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    protected void doAssert(Object actualValue, Object expectedValue) {
        logger.trace("assert that [{}] is less than [{}] (field: [{}])", actualValue, expectedValue, getField());
        assertThat("value of [" + getField() + "] is not comparable (got [" + safeClass(actualValue) + "])",
                actualValue, instanceOf(Comparable.class));
        assertThat("expected value of [" + getField() + "] is not comparable (got [" + expectedValue.getClass() + "])",
                expectedValue, instanceOf(Comparable.class));
        if (actualValue instanceof Long && expectedValue instanceof Integer) {
            expectedValue = (long) (int) expectedValue;
        }
        try {
            assertThat(errorMessage(), (Comparable) actualValue, lessThan((Comparable) expectedValue));
        } catch (ClassCastException e) {
            throw new AssertionError("cast error while checking (" + errorMessage() + "): " + e, e);
        }
    }

    private String errorMessage() {
        return "field [" + getField() + "] is not less than [" + getExpectedValue() + "]";
    }
}
