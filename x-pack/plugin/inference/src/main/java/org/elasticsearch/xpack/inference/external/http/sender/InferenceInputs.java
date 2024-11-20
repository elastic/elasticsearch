/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.elasticsearch.common.Strings;

import java.util.List;

public class InferenceInputs {
    public InferenceInputs(String query, List<String> input, boolean stream) {
        this.query = query;
        this.input = input;
        this.stream = stream;
    }

    public static IllegalArgumentException createUnsupportedTypeException(InferenceInputs inferenceInputs) {
        return new IllegalArgumentException(Strings.format("Unsupported inference inputs type: [%s]", inferenceInputs.getClass()));
    }

    private final String query;
    private final List<String> input;
    private final boolean stream;

    public String query() {
        return query;
    }

    public List<String> input() {
        return input;
    }

    public boolean stream() {
        return stream;
    }
}
