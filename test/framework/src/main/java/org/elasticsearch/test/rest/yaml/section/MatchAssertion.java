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
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.test.hamcrest.RegexMatcher.matches;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

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

        // add support for matching objects ({a:b}) against list of objects ([ {a:b, c:d} ])
        if(expectedValue instanceof Map && actualValue instanceof List) {
            Map<String, Object> expectedMap = (Map<String, Object>) expectedValue;
            List<Object> actualList = (List<Object>) actualValue;
            assertTrue(
                getField() + " was expected to be a list with Map but it's " + actualValue,
                actualList.stream()
                    .filter((each) -> each instanceof Map)
                    .findAny()
                    .isPresent()
            );

            List<Map<String, Object>> actualValues = actualList.stream()
                .filter(each -> each instanceof Map)
                .map((each -> (Map<String, Object>) each))
                .filter(each -> each.keySet().containsAll(expectedMap.keySet()))
                .collect(Collectors.toList());
            assertThat(
                getField() + " expected to be a list with at least one object that has keys: " +
                    expectedMap.keySet() + " but it was " + actualList,
                actualValues,
                is(not(empty()))
            );
            assertTrue(
                getField() + " expected to be a list with at least on object that matches " + expectedMap +
                    " but was " + actualValues,
                actualValues.stream()
                    .anyMatch(each -> each.entrySet().containsAll(expectedMap.entrySet()))
            );
        } else if (expectedValue.equals(actualValue) == false) {
            NotEqualMessageBuilder message = new NotEqualMessageBuilder();
            message.compare(getField(), actualValue, expectedValue);
            throw new AssertionError(getField() + " didn't match expected value:\n" + message);
        }
    }
}
