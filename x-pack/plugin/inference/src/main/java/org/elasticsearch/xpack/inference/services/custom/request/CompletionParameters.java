/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom.request;

import org.elasticsearch.xpack.inference.external.http.sender.ChatCompletionInput;

import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.common.JsonUtils.toJson;

public class CompletionParameters extends RequestParameters {

    public static CompletionParameters of(ChatCompletionInput completionInput) {
        return new CompletionParameters(Objects.requireNonNull(completionInput));
    }

    private CompletionParameters(ChatCompletionInput completionInput) {
        super(completionInput.getInputs());
    }

    @Override
    public Map<String, String> jsonParameters() {
        String jsonRep;

        if (inputs.isEmpty() == false) {
            jsonRep = toJson(inputs.get(0), INPUT);
        } else {
            jsonRep = toJson("", INPUT);
        }

        return Map.of(INPUT, jsonRep);
    }

}
