/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.analysis.nori;

import org.apache.lucene.analysis.ko.KoreanAnalyzer;
import org.apache.lucene.analysis.ko.KoreanPartOfSpeechStopFilter;
import org.apache.lucene.analysis.ko.KoreanTokenizer;
import org.apache.lucene.analysis.ko.POS;
import org.apache.lucene.analysis.ko.dict.UserDictionary;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractIndexAnalyzerProvider;
import org.elasticsearch.index.analysis.Analysis;

import java.util.List;
import java.util.Set;

import static org.elasticsearch.plugin.analysis.nori.NoriPartOfSpeechStopFilterFactory.resolvePOSList;

public class NoriAnalyzerProvider extends AbstractIndexAnalyzerProvider<KoreanAnalyzer> {
    private final KoreanAnalyzer analyzer;

    public NoriAnalyzerProvider(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(name);
        final KoreanTokenizer.DecompoundMode mode = NoriTokenizerFactory.getMode(settings);
        final UserDictionary userDictionary = NoriTokenizerFactory.getUserDictionary(env, settings, indexSettings);
        final List<String> tagList = Analysis.getWordList(env, settings, "stoptags");
        final Set<POS.Tag> stopTags = tagList != null ? resolvePOSList(tagList) : KoreanPartOfSpeechStopFilter.DEFAULT_STOP_TAGS;
        analyzer = new KoreanAnalyzer(userDictionary, mode, stopTags, false);
    }

    @Override
    public KoreanAnalyzer get() {
        return analyzer;
    }

}
