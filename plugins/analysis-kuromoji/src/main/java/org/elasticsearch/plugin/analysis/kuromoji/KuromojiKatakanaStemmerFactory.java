/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugin.analysis.kuromoji;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ja.JapaneseKatakanaStemFilter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;

public class KuromojiKatakanaStemmerFactory extends AbstractTokenFilterFactory {

    private final int minimumLength;

    public KuromojiKatakanaStemmerFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(indexSettings, name, settings);
        minimumLength = settings.getAsInt("minimum_length", JapaneseKatakanaStemFilter.DEFAULT_MINIMUM_LENGTH);
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return new JapaneseKatakanaStemFilter(tokenStream, minimumLength);
    }
}
