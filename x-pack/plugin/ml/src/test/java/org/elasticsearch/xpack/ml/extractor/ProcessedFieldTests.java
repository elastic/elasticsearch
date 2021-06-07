/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.extractor;

import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.FrequencyEncoding;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.NGram;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.OneHotEncoding;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.PreProcessor;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.TargetMeanEncoding;
import org.elasticsearch.xpack.ml.test.SearchHitBuilder;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ProcessedFieldTests extends ESTestCase {

    public void testOneHotGetters() {
        String inputField = "foo";
        ProcessedField processedField = new ProcessedField(makeOneHotPreProcessor(inputField, "bar", "baz"));
        assertThat(processedField.getInputFieldNames(), hasItems(inputField));
        assertThat(processedField.getOutputFieldNames(), hasItems("bar_column", "baz_column"));
        assertThat(processedField.getOutputFieldType("bar_column"), equalTo(Collections.singleton("integer")));
        assertThat(processedField.getOutputFieldType("baz_column"), equalTo(Collections.singleton("integer")));
        assertThat(processedField.getProcessorName(), equalTo(OneHotEncoding.NAME.getPreferredName()));
    }

    public void testMissingExtractor() {
        ProcessedField processedField = new ProcessedField(makeOneHotPreProcessor(randomAlphaOfLength(10), "bar", "baz"));
        assertThat(processedField.value(makeHit(), (s) -> null), emptyArray());
    }

    public void testMissingInputValues() {
        ExtractedField extractedField = makeExtractedField(new Object[0]);
        ProcessedField processedField = new ProcessedField(makeOneHotPreProcessor(randomAlphaOfLength(10), "bar", "baz"));
        assertThat(processedField.value(makeHit(), (s) -> extractedField), arrayContaining(is(nullValue()), is(nullValue())));
    }

    public void testProcessedFieldFrequencyEncoding() {
        testProcessedField(
            new FrequencyEncoding(randomAlphaOfLength(10),
                randomAlphaOfLength(10),
                MapBuilder.<String, Double>newMapBuilder().put("bar", 1.0).put("1", 0.5).put("false", 0.0).map(),
                randomBoolean()),
            new Object[]{"bar", 1, false},
            new Object[][]{
                new Object[]{1.0},
                new Object[]{0.5},
                new Object[]{0.0},
            });
    }

    public void testProcessedFieldTargetMeanEncoding() {
        testProcessedField(
            new TargetMeanEncoding(randomAlphaOfLength(10),
                randomAlphaOfLength(10),
                MapBuilder.<String, Double>newMapBuilder().put("bar", 1.0).put("1", 0.5).put("false", 0.0).map(),
                0.8,
                randomBoolean()),
            new Object[]{"bar", 1, false, "unknown"},
            new Object[][]{
                new Object[]{1.0},
                new Object[]{0.5},
                new Object[]{0.0},
                new Object[]{0.8},
            });
    }

    public void testProcessedFieldNGramEncoding() {
        testProcessedField(
            new NGram(randomAlphaOfLength(10),
                randomAlphaOfLength(10),
                new int[]{1},
                0,
                3,
                randomBoolean()),
            new Object[]{"bar", 1, false},
            new Object[][]{
                new Object[]{"b", "a", "r"},
                new Object[]{"1", null, null},
                new Object[]{"f", "a", "l"}
            });
    }

    public void testProcessedFieldOneHot() {
        testProcessedField(
            makeOneHotPreProcessor(randomAlphaOfLength(10), "bar", "1", "false"),
            new Object[]{"bar", 1, false},
            new Object[][]{
                new Object[]{0, 1, 0},
                new Object[]{1, 0, 0},
                new Object[]{0, 0, 1},
            });
    }

    public void testProcessedField(PreProcessor preProcessor, Object[] inputs, Object[][] expectedOutputs) {
        ProcessedField processedField = new ProcessedField(preProcessor);
        assert inputs.length == expectedOutputs.length;
        for (int i = 0; i < inputs.length; i++) {
            Object input = inputs[i];
            Object[] result = processedField.value(makeHit(input), (s) -> makeExtractedField(new Object[] { input }));
            assertThat(
                "Input [" + input + "] Expected " + Arrays.toString(expectedOutputs[i]) + " but received " + Arrays.toString(result),
                result,
                equalTo(expectedOutputs[i]));
        }
    }

    private static PreProcessor makeOneHotPreProcessor(String inputField, String... expectedExtractedValues) {
        Map<String, String> map = new LinkedHashMap<>();
        for (String v : expectedExtractedValues) {
            map.put(v, v + "_column");
        }
        return new OneHotEncoding(inputField, map,true);
    }

    private static ExtractedField makeExtractedField(Object[] value) {
        ExtractedField extractedField = mock(ExtractedField.class);
        when(extractedField.value(any())).thenReturn(value);
        return extractedField;
    }

    private static SearchHit makeHit() {
        return makeHit("bar");
    }

    private static SearchHit makeHit(Object value) {
        return new SearchHitBuilder(42).addField("a_keyword", value).build();
    }

}
