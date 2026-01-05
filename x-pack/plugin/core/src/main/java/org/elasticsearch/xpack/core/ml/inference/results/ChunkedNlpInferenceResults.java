/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

public abstract class ChunkedNlpInferenceResults extends NlpInferenceResults {

    public static final String TEXT = "text";
    public static final String INFERENCE = "inference";

    ChunkedNlpInferenceResults(boolean isTruncated) {
        super(isTruncated);
    }

    ChunkedNlpInferenceResults(StreamInput in) throws IOException {
        super(in);
    }
}
