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

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.util.regex.Pattern;

import static org.elasticsearch.test.hamcrest.RegexMatcher.matches;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

/**
 * Represents a match assert section:
 *
 *   - match:   { get.fields._routing: "5" }
 *
 */
public class MatchAssertion extends Assertion {

    private static final ESLogger logger = Loggers.getLogger(MatchAssertion.class);

    public MatchAssertion(String field, Object expectedValue) {
        super(field, expectedValue);
    }

    @Override
    protected void doAssert(Object actualValue, Object expectedValue) {

        //if the value is wrapped into / it is a regexp (e.g. /s+d+/)
        if (expectedValue instanceof String) {
            String expValue = ((String) expectedValue).trim();
            if (expValue.length() > 2 && expValue.startsWith("/") && expValue.endsWith("/")) {
                assertThat("field [" + getField() + "] was expected to be of type String but is an instanceof [" + safeClass(actualValue) + "]", actualValue, instanceOf(String.class));
                String stringValue = (String) actualValue;
                String regex = expValue.substring(1, expValue.length() - 1);
                logger.trace("assert that [{}] matches [{}]", stringValue, regex);
                assertThat("field [" + getField() + "] was expected to match the provided regex but didn't",
                        stringValue, matches(regex, Pattern.COMMENTS));
                return;
            }
        }

        assertThat(errorMessage(), actualValue, notNullValue());
        logger.trace("assert that [{}] matches [{}] (field [{}])", actualValue, expectedValue, getField());
        if (!actualValue.getClass().equals(safeClass(expectedValue))) {
            if (actualValue instanceof Number && expectedValue instanceof Number) {
                //Double 1.0 is equal to Integer 1
                assertThat(errorMessage(), ((Number) actualValue).doubleValue(), equalTo(((Number) expectedValue).doubleValue()));
                return;
            }
        }

        assertThat(errorMessage(), actualValue, equalTo(expectedValue));
    }

    private String errorMessage() {
        return "field [" + getField() + "] doesn't match the expected value";
    }
}
