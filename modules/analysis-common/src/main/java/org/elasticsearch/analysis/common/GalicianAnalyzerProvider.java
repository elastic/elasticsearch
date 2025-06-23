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
import org.apache.lucene.analysis.gl.GalicianAnalyzer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractIndexAnalyzerProvider;
import org.elasticsearch.index.analysis.Analysis;

public class GalicianAnalyzerProvider extends AbstractIndexAnalyzerProvider<GalicianAnalyzer> {

    private final GalicianAnalyzer analyzer;

    GalicianAnalyzerProvider(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(name);
        analyzer = new GalicianAnalyzer(
            Analysis.parseStopWords(env, settings, GalicianAnalyzer.getDefaultStopSet()),
            Analysis.parseStemExclusion(settings, CharArraySet.EMPTY_SET)
        );
    }

    @Override
    public GalicianAnalyzer get() {
        return this.analyzer;
    }
}
