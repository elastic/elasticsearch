/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.test.rest.yaml.section;

import org.elasticsearch.test.rest.yaml.ClientYamlTestExecutionContext;
import org.elasticsearch.xcontent.XContentLocation;

import java.io.IOException;
import java.util.Map;

/**
 * Base class for executable sections that hold assertions
 */
public abstract class Assertion implements ExecutableSection {
    private final XContentLocation location;
    private final String field;
    private final Object expectedValue;

    protected Assertion(XContentLocation location, String field, Object expectedValue) {
        this.location = location;
        this.field = field;
        this.expectedValue = expectedValue;
    }

    public final String getField() {
        return field;
    }

    public final Object getExpectedValue() {
        return expectedValue;
    }

    protected final Object resolveExpectedValue(ClientYamlTestExecutionContext executionContext) throws IOException {
        if (expectedValue instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) expectedValue;
            return executionContext.stash().replaceStashedValues(map);
        }

        if (executionContext.stash().containsStashedValue(expectedValue)) {
            return executionContext.stash().getValue(expectedValue.toString());
        }
        return expectedValue;
    }

    protected final Object getActualValue(ClientYamlTestExecutionContext executionContext) throws IOException {
        // If the "field" name contains only a simple stashed value reference, such as "$body", just get that value from the stash.
        if (executionContext.stash().isStashedValue(field)) {
            return executionContext.stash().getValue(field);
        }
        // Otherwise, get the value from the response. The field name will be subject to expansion of embedded ${...} references.
        return executionContext.response(field);
    }

    @Override
    public XContentLocation getLocation() {
        return location;
    }

    @Override
    public final void execute(ClientYamlTestExecutionContext executionContext) throws IOException {
        doAssert(getActualValue(executionContext), resolveExpectedValue(executionContext));
    }

    /**
     * Executes the assertion comparing the actual value (parsed from the response) with the expected one
     */
    protected abstract void doAssert(Object actualValue, Object expectedValue);

    /**
     * a utility to get the class of an object, protecting for null (i.e., returning null if the input is null)
     */
    protected Class<?> safeClass(Object o) {
        return o == null ? null : o.getClass();
    }
}
