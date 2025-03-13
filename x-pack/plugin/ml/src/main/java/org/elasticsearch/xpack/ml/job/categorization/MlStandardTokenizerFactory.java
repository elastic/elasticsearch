/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.job.categorization;

import org.apache.lucene.analysis.Tokenizer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenizerFactory;

/**
 * Factory for the standard ML categorization tokenizer.
 *
 * This differs from the "classic" ML tokenizer in that it treats URLs and paths as a single token.
 *
 * In common with the original ML C++ code, there are no configuration options.
 */
public class MlStandardTokenizerFactory extends AbstractTokenizerFactory {

    public MlStandardTokenizerFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(name);
    }

    @Override
    public Tokenizer create() {
        return new MlStandardTokenizer();
    }
}
