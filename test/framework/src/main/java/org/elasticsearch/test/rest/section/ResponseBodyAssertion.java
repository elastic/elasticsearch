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
package org.elasticsearch.test.rest.section;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.rest.parser.RestTestFragmentParser;
import org.elasticsearch.test.rest.parser.RestTestParseException;
import org.elasticsearch.test.rest.parser.RestTestSuiteParseContext;

/**
 * Checks that the response body matches some text. 
 */
public class ResponseBodyAssertion extends Assertion {
    public static final RestTestFragmentParser<ResponseBodyAssertion> PARSER = new RestTestFragmentParser<ResponseBodyAssertion>() {
        @Override
        public ResponseBodyAssertion parse(RestTestSuiteParseContext parseContext) throws IOException, RestTestParseException {
            try (XContentParser parser = JsonXContent.jsonXContent.createParser(parseContext.parseField())) {
                return new ResponseBodyAssertion("$body", parser.map());
            }
        }
    };

    private ResponseBodyAssertion(String field, Map<String, Object> expectedValue) {
        super(field, expectedValue);
    }

    @Override
    protected void doAssert(Object actualValue, Object expectedValue) {
        if (false == expectedValue.equals(actualValue)) {
            @SuppressWarnings("unchecked")
            Map<String, Object> actual = (Map<String, Object>) actualValue;
            @SuppressWarnings("unchecked")
            Map<String, Object> expected = (Map<String, Object>) expectedValue;
            FailureMessage message = new FailureMessage();
            message.compareMaps(actual, expected);
            throw new AssertionError(message.message);
        }
    }

    private class FailureMessage {
        private final StringBuilder message = new StringBuilder("body didn't match the expected value:\n");
        private int indent = 0;

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
                compare(i, actual.get(i), expected.get(i));
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

        private void compare(Object field, @Nullable Object actual, Object expected) {
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
