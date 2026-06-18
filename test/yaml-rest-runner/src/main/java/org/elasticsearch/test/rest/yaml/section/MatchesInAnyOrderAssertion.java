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
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.fail;

/**
 * Represents a matches_in_any_order assert section:
 *
 * <pre>
 *   - matches_in_any_order: { tags: ["b", "a"] }
 * </pre>
 */
public class MatchesInAnyOrderAssertion extends Assertion {

    public static final String NAME = "matches_in_any_order";

    public static MatchesInAnyOrderAssertion parse(XContentParser parser) throws IOException {
        XContentLocation location = parser.getTokenLocation();
        Tuple<String, Object> stringObjectTuple = ParserUtils.parseTuple(parser);
        return new MatchesInAnyOrderAssertion(location, stringObjectTuple.v1(), stringObjectTuple.v2());
    }

    private static final Logger logger = LogManager.getLogger(MatchesInAnyOrderAssertion.class);

    public MatchesInAnyOrderAssertion(XContentLocation location, String field, Object expectedValue) {
        super(location, field, expectedValue);
    }

    @Override
    protected void doAssert(Object actualValue, Object expectedValue) {
        if (actualValue instanceof List == false && expectedValue instanceof List == false) {
            // allow comparing non-list values using standard match semantics
            MatchAssertion.assertMatches(getField(), actualValue, expectedValue);
            return;
        }
        if (actualValue instanceof List == false) {
            fail("field [" + getField() + "] expected to be a list but was [" + actualValue + "] of type [" + safeClass(actualValue) + "]");
        }
        if (expectedValue instanceof List == false) {
            fail(
                "field ["
                    + getField()
                    + "] expected value must be a list but was ["
                    + expectedValue
                    + "] of type ["
                    + safeClass(expectedValue)
                    + "]"
            );
        }
        List<Object> actualList = new ArrayList<>((List<?>) actualValue);
        List<?> expectedList = (List<?>) expectedValue;
        logger.trace("assert that [{}] matches in any order [{}] (field [{}])", actualList, expectedList, getField());
        for (Object expectedElement : expectedList) {
            boolean matched = false;
            for (Iterator<Object> iterator = actualList.iterator(); iterator.hasNext();) {
                Object actualElement = iterator.next();
                if (valuesMatch(actualElement, expectedElement)) {
                    iterator.remove();
                    matched = true;
                    break;
                }
            }
            if (matched == false) {
                fail(
                    "field ["
                        + getField()
                        + "] expected to contain the same values in any order but no value matched ["
                        + expectedElement
                        + "] in ["
                        + actualValue
                        + "]"
                );
            }
        }
        if (actualList.isEmpty() == false) {
            fail(
                "field ["
                    + getField()
                    + "] expected to contain the same values in any order but contained unexpected values "
                    + actualList
                    + " in "
                    + actualValue
            );
        }
    }

    private boolean valuesMatch(Object actualValue, Object expectedValue) {
        try {
            MatchAssertion.assertMatches(getField(), actualValue, expectedValue);
            return true;
        } catch (AssertionError e) {
            return false;
        }
    }
}
