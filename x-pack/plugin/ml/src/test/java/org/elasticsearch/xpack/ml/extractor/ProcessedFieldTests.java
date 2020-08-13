/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.extractor;

import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.OneHotEncoding;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.PreProcessor;
import org.elasticsearch.xpack.ml.test.SearchHitBuilder;

import java.util.Arrays;
import java.util.Collections;
import java.util.function.Function;
import java.util.stream.Collectors;

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
        ProcessedField processedField = new ProcessedField(makePreProcessor(inputField, "bar", "baz"));
        assertThat(processedField.getInputFieldNames(), hasItems(inputField));
        assertThat(processedField.getOutputFieldNames(), hasItems("bar_column", "baz_column"));
        assertThat(processedField.getOutputFieldType("bar_column"), equalTo(Collections.singleton("integer")));
        assertThat(processedField.getOutputFieldType("baz_column"), equalTo(Collections.singleton("integer")));
        assertThat(processedField.getProcessorName(), equalTo(OneHotEncoding.NAME.getPreferredName()));
    }

    public void testMissingExtractor() {
        String inputField = "foo";
        ProcessedField processedField = new ProcessedField(makePreProcessor(inputField, "bar", "baz"));
        assertThat(processedField.value(makeHit(), (s) -> null), emptyArray());
    }

    public void testMissingInputValues() {
        String inputField = "foo";
        ExtractedField extractedField = makeExtractedField(new Object[0]);
        ProcessedField processedField = new ProcessedField(makePreProcessor(inputField, "bar", "baz"));
        assertThat(processedField.value(makeHit(), (s) -> extractedField), arrayContaining(is(nullValue()), is(nullValue())));
    }

    public void testProcessedField() {
        ProcessedField processedField = new ProcessedField(makePreProcessor("foo", "bar", "baz"));
        assertThat(processedField.value(makeHit(), (s) -> makeExtractedField(new Object[] { "bar" })), arrayContaining(1, 0));
        assertThat(processedField.value(makeHit(), (s) -> makeExtractedField(new Object[] { "baz" })), arrayContaining(0, 1));
    }

    private static PreProcessor makePreProcessor(String inputField, String... expectedExtractedValues) {
        return new OneHotEncoding(inputField,
            Arrays.stream(expectedExtractedValues).collect(Collectors.toMap(Function.identity(), (s) -> s + "_column")),
            true);
    }

    private static ExtractedField makeExtractedField(Object[] value) {
        ExtractedField extractedField = mock(ExtractedField.class);
        when(extractedField.value(any())).thenReturn(value);
        return extractedField;
    }

    private static SearchHit makeHit() {
        return new SearchHitBuilder(42).addField("a_keyword", "bar").build();
    }

}
