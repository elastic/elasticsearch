/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.extractor;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.PreProcessor;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import static org.elasticsearch.xpack.ml.dataframe.extractor.DataFrameDataExtractor.isValidValue;

public class ProcessedField {
    private final PreProcessor preProcessor;

    public ProcessedField(PreProcessor processor) {
        this.preProcessor = Objects.requireNonNull(processor);
    }

    public List<String> getInputFieldNames() {
        return preProcessor.inputFields();
    }

    public List<String> getOutputFieldNames() {
        return preProcessor.outputFields();
    }

    public Set<String> getOutputFieldType(String outputField) {
        return Collections.singleton(preProcessor.getOutputFieldType(outputField));
    }

    public Object[] value(SearchHit hit, Function<String, ExtractedField> fieldExtractor) {
        List<String> inputFields = getInputFieldNames();
        Map<String, Object> inputs = Maps.newMapWithExpectedSize(inputFields.size());
        for (String field : inputFields) {
            ExtractedField extractedField = fieldExtractor.apply(field);
            if (extractedField == null) {
                return new Object[0];
            }
            Object[] values = extractedField.value(hit);
            if (values == null || values.length == 0) {
                continue;
            }
            final Object value = values[0];
            if (values.length == 1 && (isValidValue(value))) {
                inputs.put(field, value);
            }
        }
        preProcessor.process(inputs);
        return preProcessor.outputFields().stream().map(inputs::get).toArray();
    }

    public String getProcessorName() {
        return preProcessor.getName();
    }

}
