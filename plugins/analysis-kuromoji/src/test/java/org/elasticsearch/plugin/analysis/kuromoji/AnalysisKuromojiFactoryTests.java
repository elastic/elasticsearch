/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.analysis.kuromoji;

import org.apache.lucene.analysis.ja.JapaneseTokenizerFactory;
import org.elasticsearch.indices.analysis.AnalysisFactoryTestCase;

import java.util.HashMap;
import java.util.Map;

public class AnalysisKuromojiFactoryTests extends AnalysisFactoryTestCase {
    public AnalysisKuromojiFactoryTests() {
        super(new AnalysisKuromojiPlugin());
    }

    @Override
    protected Map<String, Class<?>> getTokenizers() {
        Map<String, Class<?>> tokenizers = new HashMap<>(super.getTokenizers());
        tokenizers.put("japanese", JapaneseTokenizerFactory.class);
        return tokenizers;
    }

    @Override
    protected Map<String, Class<?>> getTokenFilters() {
        Map<String, Class<?>> filters = new HashMap<>(super.getTokenFilters());
        filters.put("japanesebaseform", KuromojiBaseFormFilterFactory.class);
        filters.put("japanesepartofspeechstop", KuromojiPartOfSpeechFilterFactory.class);
        filters.put("japanesereadingform", KuromojiReadingFormFilterFactory.class);
        filters.put("japanesekatakanastem", KuromojiKatakanaStemmerFactory.class);
        filters.put("japanesenumber", KuromojiNumberFilterFactory.class);
        filters.put("japanesehiraganauppercase", HiraganaUppercaseFilterFactory.class);
        filters.put("japanesekatakanauppercase", KatakanaUppercaseFilterFactory.class);
        return filters;
    }

    @Override
    protected Map<String, Class<?>> getCharFilters() {
        Map<String, Class<?>> filters = new HashMap<>(super.getCharFilters());
        filters.put("japaneseiterationmark", KuromojiIterationMarkCharFilterFactory.class);
        return filters;
    }
}
