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
import org.elasticsearch.sp.api.analysis.TokenFilterFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class NoriPartOfSpeechStopFilterFactory implements TokenFilterFactory {
    private final Set<POS.Tag> stopTags;
    private String name;

    public NoriPartOfSpeechStopFilterFactory() {
        this.stopTags = KoreanPartOfSpeechStopFilter.DEFAULT_STOP_TAGS;
    }

    @Override
    public String name() {
        return name;
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
