/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.InferenceString;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.lucene.search.uhighlight.CustomUnifiedHighlighter.MULTIVAL_SEP_CHAR;

public class SemanticFieldContent {
    private final String concatenatedTextValues;
    private final Map<Integer, InferenceString> inferenceStringValues;

    public SemanticFieldContent(List<String> textValues, Map<Integer, InferenceString> inferenceStringValues) {
        Objects.requireNonNull(textValues);
        Objects.requireNonNull(inferenceStringValues);
        this.concatenatedTextValues = Strings.collectionToDelimitedString(textValues, String.valueOf(MULTIVAL_SEP_CHAR));
        this.inferenceStringValues = Collections.unmodifiableMap(inferenceStringValues);
    }

    public String getChunkText(int startOffset, int endOffset) {
        return concatenatedTextValues.substring(startOffset, endOffset);
    }

    public InferenceString getInferenceString(int inputIndex) {
        return inferenceStringValues.get(inputIndex);
    }
}
