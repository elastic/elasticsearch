/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.deployment;

import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelInput;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.util.Map;

/**
 * Input is either a string or a document map
 */
public class NlpInferenceInput {

    public static NlpInferenceInput fromText(String inputText) {
        return new NlpInferenceInput(inputText);
    }

    public static NlpInferenceInput fromDoc(Map<String, Object> doc) {
        return new NlpInferenceInput(doc);
    }

    private final String inputText;
    private final Map<String, Object> doc;

    private NlpInferenceInput(String inputText) {
        this.inputText = ExceptionsHelper.requireNonNull(inputText, "input_text");
        doc = null;
    }

    private NlpInferenceInput(Map<String, Object> doc) {
        this.doc = ExceptionsHelper.requireNonNull(doc, "doc");
        this.inputText = null;
    }

    public boolean isTextInput() {
        return inputText != null;
    }

    public String getInputText() {
        return inputText;
    }

    public String extractInput(TrainedModelInput input) {
        if (isTextInput()) {
            return getInputText();
        }

        assert input.getFieldNames().size() == 1;
        String inputField = input.getFieldNames().get(0);
        Object inputValue = XContentMapValues.extractValue(inputField, doc);
        if (inputValue == null) {
            throw ExceptionsHelper.badRequestException("Input field [{}] does not exist in the source document", inputField);
        }
        if (inputValue instanceof String) {
            return (String) inputValue;
        }
        throw ExceptionsHelper.badRequestException("Input value [{}] for field [{}] must be a string", inputValue, inputField);
    }

}
