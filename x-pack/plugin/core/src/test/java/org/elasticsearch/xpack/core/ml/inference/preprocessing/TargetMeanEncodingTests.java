/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference.preprocessing;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.XContentParser;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

public class TargetMeanEncodingTests extends PreProcessingTests<TargetMeanEncoding> {

    @Override
    protected TargetMeanEncoding doParseInstance(XContentParser parser) throws IOException {
        return lenient
            ? TargetMeanEncoding.fromXContentLenient(parser, PreProcessor.PreProcessorParseContext.DEFAULT)
            : TargetMeanEncoding.fromXContentStrict(parser, PreProcessor.PreProcessorParseContext.DEFAULT);
    }

    @Override
    protected TargetMeanEncoding createTestInstance() {
        return createRandom();
    }

    @Override
    protected TargetMeanEncoding mutateInstance(TargetMeanEncoding instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    public static TargetMeanEncoding createRandom() {
        return createRandom(randomBoolean() ? randomBoolean() : null);
    }

    public static TargetMeanEncoding createRandom(Boolean isCustom) {
        return createRandom(isCustom, randomAlphaOfLength(10));
    }

    public static TargetMeanEncoding createRandom(Boolean isCustom, String inputField) {
        int valuesSize = randomIntBetween(1, 10);
        Map<String, Double> valueMap = new HashMap<>();
        for (int i = 0; i < valuesSize; i++) {
            valueMap.put(randomAlphaOfLength(10), randomDoubleBetween(0.0, 1.0, false));
        }
        return new TargetMeanEncoding(inputField, randomAlphaOfLength(10), valueMap, randomDoubleBetween(0.0, 1.0, false), isCustom);
    }

    @Override
    protected Writeable.Reader<TargetMeanEncoding> instanceReader() {
        return TargetMeanEncoding::new;
    }

    public void testProcessWithFieldPresent() {
        String field = "categorical";
        List<Object> values = Arrays.asList("foo", "bar", "foobar", "baz", "farequote", 1.0);
        Map<String, Double> valueMap = values.stream()
            .collect(Collectors.toMap(Object::toString, v -> randomDoubleBetween(0.0, 1.0, false)));
        String encodedFeatureName = "encoded";
        Double defaultvalue = randomDouble();
        TargetMeanEncoding encoding = new TargetMeanEncoding(field, encodedFeatureName, valueMap, defaultvalue, false);
        Object fieldValue = randomFrom(values);
        Map<String, Matcher<? super Object>> matchers = Collections.singletonMap(
            encodedFeatureName,
            equalTo(valueMap.get(fieldValue.toString()))
        );
        Map<String, Object> fieldValues = randomFieldValues(field, fieldValue);
        testProcess(encoding, fieldValues, matchers);

        // Test where the value is some unknown Value
        fieldValues = randomFieldValues(field, "unknownValue");
        matchers = Collections.singletonMap(encodedFeatureName, equalTo(defaultvalue));
        testProcess(encoding, fieldValues, matchers);
    }

    public void testInputOutputFields() {
        String field = randomAlphaOfLength(10);
        String encodedFeatureName = randomAlphaOfLength(10);
        Double defaultvalue = randomDouble();
        List<Object> values = Arrays.asList("foo", "bar", "foobar", "baz", "farequote", 1.0);
        Map<String, Double> valueMap = values.stream()
            .collect(Collectors.toMap(Object::toString, v -> randomDoubleBetween(0.0, 1.0, false)));
        TargetMeanEncoding encoding = new TargetMeanEncoding(field, encodedFeatureName, valueMap, defaultvalue, false);
        assertThat(encoding.inputFields(), containsInAnyOrder(field));
        assertThat(encoding.outputFields(), containsInAnyOrder(encodedFeatureName));
    }

}
