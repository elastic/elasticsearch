/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.da.DanishAnalyzer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractIndexAnalyzerProvider;
import org.elasticsearch.index.analysis.Analysis;

public class DanishAnalyzerProvider extends AbstractIndexAnalyzerProvider<DanishAnalyzer> {

    private final DanishAnalyzer analyzer;

    private final Object sharingKey;

    DanishAnalyzerProvider(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(name);
        CharArraySet stopWords = Analysis.parseStopWords(env, settings, DanishAnalyzer.getDefaultStopSet());
        CharArraySet stemExclusions = Analysis.parseStemExclusion(settings, CharArraySet.EMPTY_SET);
        analyzer = new DanishAnalyzer(stopWords, stemExclusions);
        this.sharingKey = new Key(new Analysis.StableCharArraySet(stopWords), new Analysis.StableCharArraySet(stemExclusions));
    }

    @Override
    public DanishAnalyzer get() {
        return this.analyzer;
    }

    @Override
    public Object sharingKey() {
        return sharingKey;
    }

    private record Key(Analysis.StableCharArraySet stopWords, Analysis.StableCharArraySet stemExclusions) {}
}
