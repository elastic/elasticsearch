/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.common.xcontent.ParseField;

public interface NlpConfig extends LenientlyParsedInferenceConfig, StrictlyParsedInferenceConfig {

    ParseField VOCABULARY = new ParseField("vocabulary");
    ParseField TOKENIZATION_PARAMS = new ParseField("tokenization_params");
    ParseField CLASSIFICATION_LABELS = new ParseField("classification_labels");

    /**
     * @return the vocabulary configuration that allows retrieving it
     */
    VocabularyConfig getVocabularyConfig();

    /**
     * @return the model tokenization parameters
     */
    TokenizationParams getTokenizationParams();
}
