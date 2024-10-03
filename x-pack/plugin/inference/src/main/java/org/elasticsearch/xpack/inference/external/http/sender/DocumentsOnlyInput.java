/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import java.util.List;
import java.util.Objects;

public class DocumentsOnlyInput extends InferenceInputs {

    public static DocumentsOnlyInput of(InferenceInputs inferenceInputs) {
        if (inferenceInputs instanceof DocumentsOnlyInput == false) {
            throw createUnsupportedTypeException(inferenceInputs);
        }

        return (DocumentsOnlyInput) inferenceInputs;
    }

    private final List<String> input;
    private final boolean stream;

    public DocumentsOnlyInput(List<String> input) {
        this(input, false);
    }

    public DocumentsOnlyInput(List<String> input, boolean stream) {
        super();
        this.input = Objects.requireNonNull(input);
        this.stream = stream;
    }

    public List<String> getInputs() {
        return this.input;
    }

    public boolean stream() {
        return stream;
    }
}
