/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.pipelines.nlp;

import org.elasticsearch.xpack.ml.inference.pipelines.nlp.tokenizers.BertTokenizer;

import java.io.IOException;
import java.util.Locale;

public enum TaskType {

    TOKEN_CLASSIFICATION {
        public NlpPipeline.Processor createProcessor(BertTokenizer tokenizer) throws IOException {
            return new NerProcessor(tokenizer);
        }
    };

    public NlpPipeline.Processor createProcessor(BertTokenizer tokenizer) throws IOException {
        throw new UnsupportedOperationException("json request must be specialised for task type [" + this.name() + "]");
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }

    public static TaskType fromString(String name) {
        return valueOf(name.trim().toUpperCase(Locale.ROOT));
    }
}
