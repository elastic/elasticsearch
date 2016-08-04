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

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentLocation;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
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

    private static final ESLogger logger = Loggers.getLogger(MatchAssertion.class);

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

        if (expectedValue.equals(actualValue) == false) {
            FailureMessage message = new FailureMessage(getField());
            message.compare(getField(), actualValue, expectedValue);
            throw new AssertionError(message.message);
        }
    }

    private static class FailureMessage {
        private final StringBuilder message;
        private int indent = 0;

        private FailureMessage(String field) {
            this.message = new StringBuilder(field + " didn't match the expected value:\n");
        }

        private void compareMaps(Map<String, Object> actual, Map<String, Object> expected) {
            actual = new TreeMap<>(actual);
            expected = new TreeMap<>(expected);
            for (Map.Entry<String, Object> expectedEntry : expected.entrySet()) {
                compare(expectedEntry.getKey(), actual.remove(expectedEntry.getKey()), expectedEntry.getValue());
            }
            for (Map.Entry<String, Object> unmatchedEntry : actual.entrySet()) {
                field(unmatchedEntry.getKey(), "unexpected but found [" + unmatchedEntry.getValue() + "]");
            }
        }

        private void compareLists(List<Object> actual, List<Object> expected) {
            int i = 0;
            while (i < actual.size() && i < expected.size()) {
                compare(Integer.toString(i), actual.get(i), expected.get(i));
                i++;
            }
            if (actual.size() == expected.size()) {
                return;
            }
            indent();
            if (actual.size() < expected.size()) {
                message.append("expected [").append(expected.size() - i).append("] more entries\n");
                return;
            }
            message.append("received [").append(actual.size() - i).append("] more entries than expected\n");
        }

        private void compare(String field, @Nullable Object actual, Object expected) {
            if (expected instanceof Map) {
                if (actual == null) {
                    field(field, "expected map but not found");
                    return;
                }
                if (false == actual instanceof Map) {
                    field(field, "expected map but found [" + actual + "]");
                    return;
                }
                @SuppressWarnings("unchecked")
                Map<String, Object> expectedMap = (Map<String, Object>) expected;
                @SuppressWarnings("unchecked")
                Map<String, Object> actualMap = (Map<String, Object>) actual;
                if (expectedMap.isEmpty() && actualMap.isEmpty()) {
                    field(field, "same [empty map]");
                    return;
                }
                field(field, null);
                indent += 1;
                compareMaps(actualMap, expectedMap);
                indent -= 1;
                return;
            }
            if (expected instanceof List) {
                if (actual == null) {
                    field(field, "expected list but not found");
                    return;
                }
                if (false == actual instanceof List) {
                    field(field, "expected list but found [" + actual + "]");
                    return;
                }
                @SuppressWarnings("unchecked")
                List<Object> expectedList = (List<Object>) expected;
                @SuppressWarnings("unchecked")
                List<Object> actualList = (List<Object>) actual;
                if (expectedList.isEmpty() && actualList.isEmpty()) {
                    field(field, "same [empty list]");
                    return;
                }
                field(field, null);
                indent += 1;
                compareLists(actualList, expectedList);
                indent -= 1;
                return;
            }
            if (actual == null) {
                field(field, "expected [" + expected + "] but not found");
                return;
            }
            if (Objects.equals(expected, actual)) {
                field(field, "same [" + expected + "]");
                return;
            }
            field(field, "expected [" + expected + "] but was [" + actual + "]");
        }

        private void indent() {
            for (int i = 0; i < indent; i++) {
                message.append("  ");
            }
        }

        private void field(Object name, String info) {
            indent();
            message.append(String.format(Locale.ROOT, "%30s: ", name));
            if (info != null) {
                message.append(info);
            }
            message.append('\n');
        }
    }
}
