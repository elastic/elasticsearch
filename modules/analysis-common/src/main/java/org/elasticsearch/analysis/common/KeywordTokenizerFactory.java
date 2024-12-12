/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenizerFactory;

public class KeywordTokenizerFactory extends AbstractTokenizerFactory {

    private final int bufferSize;

    KeywordTokenizerFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(name);
        bufferSize = settings.getAsInt("buffer_size", 256);
    }

    @Override
    public Tokenizer create() {
        return new KeywordTokenizer(bufferSize);
    }
}
