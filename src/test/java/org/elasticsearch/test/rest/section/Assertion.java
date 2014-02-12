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

import org.elasticsearch.test.rest.RestTestExecutionContext;

import java.io.IOException;

/**
 * Base class for executable sections that hold assertions
 */
public abstract class Assertion implements ExecutableSection {

    private final String field;
    private final Object expectedValue;

    protected Assertion(String field, Object expectedValue) {
        this.field = field;
        this.expectedValue = expectedValue;
    }

    public final String getField() {
        return field;
    }

    public final Object getExpectedValue() {
        return expectedValue;
    }

    protected final Object resolveExpectedValue(RestTestExecutionContext executionContext) {
        if (executionContext.isStashed(expectedValue)) {
            return executionContext.unstash(expectedValue.toString());
        }
        return expectedValue;
    }

    protected final Object getActualValue(RestTestExecutionContext executionContext) throws IOException {
        if (executionContext.isStashed(field)) {
            return executionContext.unstash(field);
        }
        return executionContext.response(field);
    }

    @Override
    public final void execute(RestTestExecutionContext executionContext) throws IOException {
        doAssert(getActualValue(executionContext), resolveExpectedValue(executionContext));
    }

    /**
     * Executes the assertion comparing the actual value (parsed from the response) with the expected one
     */
    protected abstract void doAssert(Object actualValue, Object expectedValue);
}
