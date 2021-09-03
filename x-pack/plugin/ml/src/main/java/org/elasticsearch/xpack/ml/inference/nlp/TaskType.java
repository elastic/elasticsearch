/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp;

import org.elasticsearch.xpack.core.ml.inference.trainedmodel.BertPassThroughConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.FillMaskConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.NerConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.NlpConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextClassificationConfig;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.NlpTokenizer;

import java.util.Locale;

public enum TaskType {

    NER {
        @Override
        public NlpTask.Processor createProcessor(NlpTokenizer tokenizer, NlpConfig config) {
            return new NerProcessor(tokenizer, (NerConfig) config);
        }
    },
    TEXT_CLASSIFICATION {
        @Override
        public NlpTask.Processor createProcessor(NlpTokenizer tokenizer, NlpConfig config) {
            return new TextClassificationProcessor(tokenizer, (TextClassificationConfig) config);
        }
    },
    FILL_MASK {
        @Override
        public NlpTask.Processor createProcessor(NlpTokenizer tokenizer, NlpConfig config) {
            return new FillMaskProcessor(tokenizer, (FillMaskConfig) config);
        }
    },
    BERT_PASS_THROUGH {
        @Override
        public NlpTask.Processor createProcessor(NlpTokenizer tokenizer, NlpConfig config) {
            return new PassThroughProcessor(tokenizer, (BertPassThroughConfig) config);
        }
    };

    public NlpTask.Processor createProcessor(NlpTokenizer tokenizer, NlpConfig config) {
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
