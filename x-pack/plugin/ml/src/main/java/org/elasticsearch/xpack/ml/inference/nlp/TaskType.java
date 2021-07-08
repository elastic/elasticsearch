/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp;

import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.BertTokenizer;

import java.util.Locale;

public enum TaskType {

    NER {
        @Override
        public NlpTask.Processor createProcessor(BertTokenizer tokenizer, NlpTaskConfig config) {
            return new NerProcessor(tokenizer, config);
        }
    },
    SENTIMENT_ANALYSIS {
        @Override
        public NlpTask.Processor createProcessor(BertTokenizer tokenizer, NlpTaskConfig config) {
            return new SentimentAnalysisProcessor(tokenizer, config);
        }
    },
    FILL_MASK {
        @Override
        public NlpTask.Processor createProcessor(BertTokenizer tokenizer, NlpTaskConfig config) {
            return new FillMaskProcessor(tokenizer, config);
        }
    },
    BERT_PASS_THROUGH {
        @Override
        public NlpTask.Processor createProcessor(BertTokenizer tokenizer, NlpTaskConfig config) {
            return new PassThroughProcessor(tokenizer, config);
        }
    };

    public NlpTask.Processor createProcessor(BertTokenizer tokenizer, NlpTaskConfig config) {
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
