/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.preprocessing;

import org.elasticsearch.test.AbstractSerializingTestCase;
import org.hamcrest.Matcher;
import org.junit.Before;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.equalTo;

public abstract class PreProcessingTests<T extends PreProcessor> extends AbstractSerializingTestCase<T> {

    protected boolean lenient;

    @Before
    public void chooseStrictOrLenient() {
        lenient = randomBoolean();
    }

    @Override
    protected boolean supportsUnknownFields() {
        return lenient;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> !field.isEmpty();
    }

    void testProcess(PreProcessor preProcessor, Map<String, Object> fieldValues, Map<String, Matcher<? super Object>> assertions) {
        preProcessor.process(fieldValues);
        assertions.forEach((fieldName, matcher) ->
            assertThat(fieldValues.get(fieldName), matcher)
        );
    }

    public void testWithMissingField() {
        Map<String, Object> fields = randomFieldValues();
        PreProcessor preProcessor = this.createTestInstance();
        Map<String, Object> fieldsCopy = new HashMap<>(fields);
        preProcessor.process(fields);
        assertThat(fieldsCopy, equalTo(fields));
    }

    Map<String, Object> randomFieldValues() {
        int numFields = randomIntBetween(1, 5);
        Map<String, Object> fieldValues = new HashMap<>(numFields);
        for (int k = 0; k < numFields; k++) {
            fieldValues.put(randomAlphaOfLength(10), randomAlphaOfLength(10));
        }
        return fieldValues;
    }

    Map<String, Object> randomFieldValues(String categoricalField, Object categoricalValue) {
        Map<String, Object> fieldValues = randomFieldValues();
        fieldValues.put(categoricalField, categoricalValue);
        return fieldValues;
    }

}
