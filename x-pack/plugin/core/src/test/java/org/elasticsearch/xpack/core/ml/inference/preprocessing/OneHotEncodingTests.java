/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.preprocessing;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;

public class OneHotEncodingTests extends AbstractSerializingTestCase<OneHotEncoding> {

    private boolean lenient;

    @Before
    public void chooseStrictOrLenient() {
        lenient = randomBoolean();
    }

    @Override
    protected OneHotEncoding doParseInstance(XContentParser parser) throws IOException {
        return lenient ? OneHotEncoding.fromXContentLenient(parser) : OneHotEncoding.fromXContentStrict(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return lenient;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> !field.isEmpty();
    }

    @Override
    protected OneHotEncoding createTestInstance() {
        return createRandom();
    }

    public static OneHotEncoding createRandom() {
        int valuesSize = randomIntBetween(1, 10);
        Map<String, String> valueMap = new HashMap<>();
        for (int i = 0; i < valuesSize; i++) {
            valueMap.put(randomAlphaOfLength(10), randomAlphaOfLength(10));
        }
        return new OneHotEncoding(randomAlphaOfLength(10), valueMap);
    }

    @Override
    protected Writeable.Reader<OneHotEncoding> instanceReader() {
        return OneHotEncoding::new;
    }

    public void testProcess() {
        String field = "categorical";
        List<String> values = Arrays.asList("foo", "bar", "foobar", "baz", "farequote");
        Map<String, String> valueMap = values.stream().collect(Collectors.toMap(Function.identity(), v -> "Column_" + v));
        OneHotEncoding encoding = new OneHotEncoding(field, valueMap);
        for(int i = 0; i < NUMBER_OF_TEST_RUNS; i++) {
            int numFields = randomIntBetween(1, 5);
            Map<String, Object> fieldValues = new HashMap<>(numFields);
            for (int k = 0; k < numFields; k++) {
                fieldValues.put(randomAlphaOfLength(10), randomAlphaOfLength(10));
            }
            boolean addedField = randomBoolean();
            String addedValue = null;
            if (addedField) {
                addedValue = randomBoolean() ? "unknownValue" : randomFrom(values);
                fieldValues.put(field, addedValue);
            }
            Map<String, Object> resultFields = encoding.process(fieldValues);
            if (addedField) {
                for (String value : values) {
                    if (value.equals(addedValue)) {
                        assertThat(resultFields.get("Column_" + value), equalTo(1));
                    } else {
                        assertThat(resultFields.get("Column_" + value), equalTo(0));
                    }
                }
            } else {
                for (String value : values) {
                    assertThat(resultFields, not(hasKey("Column_"+value)));
                }
            }
        }
    }
}
