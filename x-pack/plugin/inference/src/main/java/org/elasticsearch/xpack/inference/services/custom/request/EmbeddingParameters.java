/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom.request;

import org.elasticsearch.inference.InputType;
import org.elasticsearch.xpack.inference.external.http.sender.EmbeddingsInput;
import org.elasticsearch.xpack.inference.services.custom.InputTypeTranslator;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.common.JsonUtils.toJson;

public class EmbeddingParameters extends RequestParameters {
    private static final String INPUT_TYPE = "input_type";

    public static EmbeddingParameters of(EmbeddingsInput embeddingsInput, InputTypeTranslator inputTypeTranslator) {
        return new EmbeddingParameters(Objects.requireNonNull(embeddingsInput), Objects.requireNonNull(inputTypeTranslator));
    }

    private final InputType inputType;
    private final InputTypeTranslator translator;

    private EmbeddingParameters(EmbeddingsInput embeddingsInput, InputTypeTranslator translator) {
        super(embeddingsInput.getStringInputs());
        this.inputType = embeddingsInput.getInputType();
        this.translator = translator;
    }

    @Override
    protected Map<String, String> taskTypeParameters() {
        var additionalParameters = new HashMap<String, String>();

        if (inputType != null && translator.getTranslation().containsKey(inputType)) {
            var inputTypeTranslation = translator.getTranslation().get(inputType);

            additionalParameters.put(INPUT_TYPE, toJson(inputTypeTranslation, INPUT_TYPE));
        } else {
            additionalParameters.put(INPUT_TYPE, toJson(translator.getDefaultValue(), INPUT_TYPE));
        }

        return additionalParameters;
    }
}
