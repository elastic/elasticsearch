/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.test.rest.yaml.section;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentLocation;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.NotEqualMessageBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.elasticsearch.test.hamcrest.RegexMatcher.matches;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertNotNull;
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

    private static final Logger logger = Loggers.getLogger(MatchAssertion.class);

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

        assertNotNull("field [" + getField() + "] is null", actualValue);
        logger.trace("assert that [{}] matches [{}] (field [{}])", actualValue, expectedValue, getField());
        if (actualValue.getClass().equals(safeClass(expectedValue)) == false) {
            if (actualValue instanceof Number && expectedValue instanceof Number) {
                //Double 1.0 is equal to Integer 1
                assertThat("field [" + getField() + "] doesn't match the expected value",
                        ((Number) actualValue).doubleValue(), equalTo(((Number) expectedValue).doubleValue()));
                return;
            }
        }

        if (checkEquals(actualValue, expectedValue) == false) {
            NotEqualMessageBuilder message = new NotEqualMessageBuilder();
            message.compare(getField(), actualValue, expectedValue);
            throw new AssertionError(getField() + " didn't match expected value:\n" + message);
        }
    }

    /**
     * Like {@link Map#equals(Object)} except it treats {@link Float}s as {@link Double}s.
     */
    private boolean checkEquals(Object actualValue, Object expectedValue) {
        if (expectedValue instanceof Map) {
            if (false == actualValue instanceof Map) {
                return false;
            }
            Map<?, ?> actualMap = (Map<?, ?>) actualValue;
            Map<?, ?> expectedMap = (Map<?, ?>) expectedValue;
            if (actualMap.size() != expectedMap.size()) {
                return false;
            }
            for (Map.Entry<?, ?> e : expectedMap.entrySet()) {
                Object a = actualMap.get(e.getKey());
                if (a == null || false == checkEquals(a, e.getValue())) {
                    return false;
                }
            }
            return true;
        }
        if (expectedValue instanceof List) {
            if (false == actualValue instanceof List) {
                return false;
            }
            List<?> actualList = (List<?>) actualValue;
            List<?> expectedList = (List<?>) expectedValue;
            if (actualList.size() != expectedList.size()) {
                return false;
            }
            for (int i = 0; i < expectedList.size(); i++) {
                if (false == checkEquals(actualList.get(i), expectedList.get(i))) {
                    return false;
                }
            }
            return true;
        }
        if (expectedValue instanceof Double) {
            Double expectedDouble = (Double) expectedValue;
            if (actualValue instanceof Float) {
                /*
                 * We only ever parse doubles in expected values but
                 * some xcontent types return floats in some fields.
                 * So we compare with the precision we have. The truth
                 * is that those types actually have higher fidelity.
                 * The xcontent types that return doubles have rounded
                 * real floats to doubles as part of the serialization
                 * process.
                 */
                float actualFloat = (Float) actualValue;
                return actualFloat == expectedDouble.floatValue();
            }
            if (actualValue instanceof Double) {
                double actualDouble = (Double) actualValue;
                return actualDouble == expectedDouble;
            }
            return false;
        }
        return expectedValue.equals(actualValue);
    }
}
