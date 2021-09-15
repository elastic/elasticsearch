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
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static io.github.nik9000.mapmatcher.ListMatcher.matchesList;
import static io.github.nik9000.mapmatcher.MapMatcher.assertMap;
import static io.github.nik9000.mapmatcher.MapMatcher.matchesMap;
import static org.elasticsearch.test.hamcrest.RegexMatcher.matches;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

/**
 * Represents a match assert section:
 *
 *   - match:   { get.fields._routing: "5" }
 *
 */
public class MatchAssertion extends Assertion {
    public static MatchAssertion parse(XContentParser parser) throws IOException {
        XContentLocation location = parser.getTokenLocation();
        Tuple<String,Object> stringObjectTuple = ParserUtils.parseTuple(parser);
        return new MatchAssertion(location, stringObjectTuple.v1(), stringObjectTuple.v2());
    }

    private static final Logger logger = LogManager.getLogger(MatchAssertion.class);

    public MatchAssertion(XContentLocation location, String field, Object expectedValue) {
        super(location, field, expectedValue);
    }

    @Override
    protected void doAssert(Object actualValue, Object expectedValue) {
        //if the value is wrapped into / it is a regexp (e.g. /s+d+/)
        if (expectedValue instanceof String) {
            String expValue = ((String) expectedValue).trim();
            if (expValue.length() > 2 && expValue.startsWith("/") && expValue.endsWith("/")) {
                assertThat("field [" + getField() + "] was expected to be of type String but is an instanceof [" +
                        safeClass(actualValue) + "]", actualValue, instanceOf(String.class));
                String stringValue = (String) actualValue;
                String regex = expValue.substring(1, expValue.length() - 1);
                logger.trace("assert that [{}] matches [{}]", stringValue, regex);
                assertThat("field [" + getField() + "] was expected to match the provided regex but didn't",
                        stringValue, matches(regex, Pattern.COMMENTS));
                return;
            }
        }

        logger.trace("assert that [{}] matches [{}] (field [{}])", actualValue, expectedValue, getField());
        if (expectedValue == null) {
            assertNull("field [" + getField() + "] should be null but was [" + actualValue + "]", actualValue);
            return;
        }
        assertNotNull("field [" + getField() + "] is null", actualValue);

        if (actualValue.getClass().equals(safeClass(expectedValue)) == false) {
            if (actualValue instanceof Number && expectedValue instanceof Number) {
                //Double 1.0 is equal to Integer 1
                assertThat("field [" + getField() + "] doesn't match the expected value",
                        ((Number) actualValue).doubleValue(), equalTo(((Number) expectedValue).doubleValue()));
                return;
            }
        }

        if (expectedValue instanceof Map) {
            assertThat(actualValue, instanceOf(Map.class));
            assertMap((Map<?, ?>) actualValue, matchesMap((Map<?, ?>) expectedValue));
        } else if (expectedValue instanceof List) {
            assertThat(actualValue, instanceOf(List.class));
            assertMap((List<?>) actualValue, matchesList((List<?>) expectedValue));
        }
        assertThat(actualValue, equalTo(expectedValue));
    }
}
