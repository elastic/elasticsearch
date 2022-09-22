/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugin.analysis.nori;

import org.apache.lucene.analysis.TokenStream;
import org.elasticsearch.plugin.analysis.api.TokenFilterFactory;
import org.elasticsearch.plugin.api.NamedComponent;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

@NamedComponent(name = "nori_part_of_speech")
public class NoriPartOfSpeechStopFilterFactory implements TokenFilterFactory {

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return tokenStream;
    }

//    public NoriPartOfSpeechStopFilterFactory(IndexSettings indexSettings, Environment env, String name, Settings settings) {
//        super(name, settings);
//        List<String> tagList = Analysis.getWordList(env, settings, "stoptags");
//        this.stopTags = tagList != null ? resolvePOSList(tagList) : KoreanPartOfSpeechStopFilter.DEFAULT_STOP_TAGS;
//    }

}
