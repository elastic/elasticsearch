/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.th.ThaiAnalyzer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.analysis.AbstractIndexAnalyzerProvider;
import org.elasticsearch.index.analysis.Analysis;

/**
 * Provider for the {@code thai} analyzer type. Indices created before {@link IndexVersions#THAI_ANALYSIS_LUCENE_10_6} get
 * {@link LegacyThaiAnalyzer} (the Lucene 10.5 chain and default stopwords); newer indices get Lucene's {@link ThaiAnalyzer}.
 */
public class ThaiAnalyzerProvider extends AbstractIndexAnalyzerProvider<Analyzer> {

    private final Analyzer analyzer;
    private final Object sharingKey;

    ThaiAnalyzerProvider(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(name);
        IndexVersion created = indexSettings.getIndexVersionCreated();
        boolean legacy = created.before(IndexVersions.THAI_ANALYSIS_LUCENE_10_6);
        CharArraySet defaults = legacy ? LegacyThaiAnalyzer.getDefaultStopSet() : ThaiAnalyzer.getDefaultStopSet();
        CharArraySet stopWords = Analysis.parseStopWords(created, env, settings, defaults);
        analyzer = legacy ? new LegacyThaiAnalyzer(stopWords) : new ThaiAnalyzer(stopWords);
        // The stop set alone does not distinguish the two chains (a user may supply the same explicit list on both sides of the
        // gate), so the gate side must be part of the key.
        this.sharingKey = new Key(legacy, Analysis.stableStopWords(settings, stopWords));
    }

    @Override
    public Analyzer get() {
        return this.analyzer;
    }

    @Override
    public Object sharingKey() {
        return sharingKey;
    }

    private record Key(boolean legacy, Analysis.StableCharArraySet stopWords) {}
}
