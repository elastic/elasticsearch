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

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;

/**
 * Represents a length assert section:
 * <p/>
 * - length:   { hits.hits: 1  }
 */
public class LengthAssertion extends Assertion {

    private static final ESLogger logger = Loggers.getLogger(LengthAssertion.class);

    public LengthAssertion(String field, Object expectedValue) {
        super(field, expectedValue);
    }

    @Override
    protected void doAssert(Object actualValue, Object expectedValue) {
        logger.trace("assert that [{}] has length [{}] (field: [{}])", actualValue, expectedValue, getField());
        assertThat("expected value of [" + getField() + "] is not numeric (got [" + expectedValue.getClass() + "]", expectedValue, instanceOf(Number.class));
        int length = ((Number) expectedValue).intValue();
        if (actualValue instanceof String) {
            assertThat(errorMessage(), ((String) actualValue).length(), equalTo(length));
        } else if (actualValue instanceof List) {
            assertThat(errorMessage(), ((List) actualValue).size(), equalTo(length));
        } else if (actualValue instanceof Map) {
            assertThat(errorMessage(), ((Map) actualValue).keySet().size(), equalTo(length));
        } else {
            throw new UnsupportedOperationException("value is of unsupported type [" + safeClass(actualValue) + "]");
        }
    }

    private String errorMessage() {
        return "field [" + getField() + "] doesn't have length [" + getExpectedValue() + "]";
    }
}
