/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugin.analysis.nori;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ko.KoreanPartOfSpeechStopFilter;
import org.apache.lucene.analysis.ko.POS;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;
import org.elasticsearch.index.analysis.Analysis;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class NoriPartOfSpeechStopFilterFactory extends AbstractTokenFilterFactory {
    private final Set<POS.Tag> stopTags;

    public NoriPartOfSpeechStopFilterFactory(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(name, settings);
        List<String> tagList = Analysis.getWordList(env, settings, "stoptags");
        this.stopTags = tagList != null ? resolvePOSList(tagList) : KoreanPartOfSpeechStopFilter.DEFAULT_STOP_TAGS;
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return new KoreanPartOfSpeechStopFilter(tokenStream, stopTags);
    }

    static Set<POS.Tag> resolvePOSList(List<String> tagList) {
        Set<POS.Tag> stopTags = new HashSet<>();
        for (String tag : tagList) {
            stopTags.add(POS.resolveTag(tag));
        }
        return stopTags;
    }
}
