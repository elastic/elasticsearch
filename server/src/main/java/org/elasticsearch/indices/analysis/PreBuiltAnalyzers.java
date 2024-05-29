/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.indices.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.classic.ClassicAnalyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.analysis.PreBuiltCacheFactory.CachingStrategy;

import java.util.Locale;

public enum PreBuiltAnalyzers {

    STANDARD(CachingStrategy.INDEX) {
        @Override
        protected Analyzer create(IndexVersion version) {
            return new StandardAnalyzer(CharArraySet.EMPTY_SET);
        }
    },

    DEFAULT(CachingStrategy.INDEX) {
        @Override
        protected Analyzer create(IndexVersion version) {
            // by calling get analyzer we are ensuring reuse of the same STANDARD analyzer for DEFAULT!
            // this call does not create a new instance
            return STANDARD.getAnalyzer(version);
        }
    },

    KEYWORD(CachingStrategy.ONE) {
        @Override
        protected Analyzer create(IndexVersion version) {
            return new KeywordAnalyzer();
        }
    },

    STOP {
        @Override
        protected Analyzer create(IndexVersion version) {
            return new StopAnalyzer(EnglishAnalyzer.ENGLISH_STOP_WORDS_SET);
        }
    },

    WHITESPACE {
        @Override
        protected Analyzer create(IndexVersion version) {
            return new WhitespaceAnalyzer();
        }
    },

    SIMPLE {
        @Override
        protected Analyzer create(IndexVersion version) {
            return new SimpleAnalyzer();
        }
    },

    CLASSIC {
        @Override
        protected Analyzer create(IndexVersion version) {
            return new ClassicAnalyzer();
        }
    };

    protected abstract Analyzer create(IndexVersion version);

    protected final PreBuiltCacheFactory.PreBuiltCache<Analyzer> cache;

    PreBuiltAnalyzers() {
        this(PreBuiltCacheFactory.CachingStrategy.LUCENE);
    }

    PreBuiltAnalyzers(PreBuiltCacheFactory.CachingStrategy cachingStrategy) {
        cache = PreBuiltCacheFactory.getCache(cachingStrategy);
    }

    public PreBuiltCacheFactory.PreBuiltCache<Analyzer> getCache() {
        return cache;
    }

    public synchronized Analyzer getAnalyzer(IndexVersion version) {
        Analyzer analyzer = cache.get(version);
        if (analyzer == null) {
            analyzer = this.create(version);
            cache.put(version, analyzer);
        }

        return analyzer;
    }

    /**
     * Get a pre built Analyzer by its name or fallback to the default one
     * @param name Analyzer name
     * @param defaultAnalyzer default Analyzer if name not found
     */
    public static PreBuiltAnalyzers getOrDefault(String name, PreBuiltAnalyzers defaultAnalyzer) {
        try {
            return valueOf(name.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            return defaultAnalyzer;
        }
    }

}
