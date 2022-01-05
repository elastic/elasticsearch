/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.Version;
import org.elasticsearch.xcontent.ParseField;

public interface NlpConfig extends LenientlyParsedInferenceConfig, StrictlyParsedInferenceConfig {

    ParseField VOCABULARY = new ParseField("vocabulary");
    ParseField TOKENIZATION = new ParseField("tokenization");
    ParseField CLASSIFICATION_LABELS = new ParseField("classification_labels");
    ParseField RESULTS_FIELD = new ParseField("results_field");
    ParseField NUM_TOP_CLASSES = new ParseField("num_top_classes");

    Version MINIMUM_NLP_SUPPORTED_VERSION = Version.V_8_0_0;

    /**
     * @return the vocabulary configuration that allows retrieving it
     */
    VocabularyConfig getVocabularyConfig();

    /**
     * @return the model tokenization parameters
     */
    Tokenization getTokenization();
}
