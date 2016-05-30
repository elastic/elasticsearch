/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.indices.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.ar.ArabicAnalyzer;
import org.apache.lucene.analysis.bg.BulgarianAnalyzer;
import org.apache.lucene.analysis.br.BrazilianAnalyzer;
import org.apache.lucene.analysis.ca.CatalanAnalyzer;
import org.apache.lucene.analysis.cjk.CJKAnalyzer;
import org.apache.lucene.analysis.ckb.SoraniAnalyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.cz.CzechAnalyzer;
import org.apache.lucene.analysis.da.DanishAnalyzer;
import org.apache.lucene.analysis.de.GermanAnalyzer;
import org.apache.lucene.analysis.el.GreekAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.es.SpanishAnalyzer;
import org.apache.lucene.analysis.eu.BasqueAnalyzer;
import org.apache.lucene.analysis.fa.PersianAnalyzer;
import org.apache.lucene.analysis.fi.FinnishAnalyzer;
import org.apache.lucene.analysis.fr.FrenchAnalyzer;
import org.apache.lucene.analysis.ga.IrishAnalyzer;
import org.apache.lucene.analysis.gl.GalicianAnalyzer;
import org.apache.lucene.analysis.hi.HindiAnalyzer;
import org.apache.lucene.analysis.hu.HungarianAnalyzer;
import org.apache.lucene.analysis.hy.ArmenianAnalyzer;
import org.apache.lucene.analysis.id.IndonesianAnalyzer;
import org.apache.lucene.analysis.it.ItalianAnalyzer;
import org.apache.lucene.analysis.lt.LithuanianAnalyzer;
import org.apache.lucene.analysis.lv.LatvianAnalyzer;
import org.apache.lucene.analysis.nl.DutchAnalyzer;
import org.apache.lucene.analysis.no.NorwegianAnalyzer;
import org.apache.lucene.analysis.pt.PortugueseAnalyzer;
import org.apache.lucene.analysis.ro.RomanianAnalyzer;
import org.apache.lucene.analysis.ru.RussianAnalyzer;
import org.apache.lucene.analysis.standard.ClassicAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.sv.SwedishAnalyzer;
import org.apache.lucene.analysis.th.ThaiAnalyzer;
import org.apache.lucene.analysis.tr.TurkishAnalyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.elasticsearch.Version;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.index.analysis.PatternAnalyzer;
import org.elasticsearch.index.analysis.SnowballAnalyzer;
import org.elasticsearch.index.analysis.StandardHtmlStripAnalyzer;
import org.elasticsearch.indices.analysis.PreBuiltCacheFactory.CachingStrategy;

import java.util.Locale;

/**
 *
 */
public enum PreBuiltAnalyzers {

    STANDARD(CachingStrategy.ELASTICSEARCH) {
        @Override
        protected Analyzer create(Version version) {
            final Analyzer a = new StandardAnalyzer(CharArraySet.EMPTY_SET);
            a.setVersion(version.luceneVersion);
            return a;
        }
    },

    DEFAULT(CachingStrategy.ELASTICSEARCH){
        @Override
        protected Analyzer create(Version version) {
            // by calling get analyzer we are ensuring reuse of the same STANDARD analyzer for DEFAULT!
            // this call does not create a new instance
            return STANDARD.getAnalyzer(version);
        }
    },

    KEYWORD(CachingStrategy.ONE) {
        @Override
        protected Analyzer create(Version version) {
            return new KeywordAnalyzer();
        }
    },

    STOP {
        @Override
        protected Analyzer create(Version version) {
            Analyzer a = new StopAnalyzer();
            a.setVersion(version.luceneVersion);
            return a;
        }
    },

    WHITESPACE {
        @Override
        protected Analyzer create(Version version) {
            Analyzer a = new WhitespaceAnalyzer();
            a.setVersion(version.luceneVersion);
            return a;
        }
    },

    SIMPLE {
        @Override
        protected Analyzer create(Version version) {
            Analyzer a = new SimpleAnalyzer();
            a.setVersion(version.luceneVersion);
            return a;
        }
    },

    CLASSIC {
        @Override
        protected Analyzer create(Version version) {
            Analyzer a = new ClassicAnalyzer();
            a.setVersion(version.luceneVersion);
            return a;
        }
    },

    SNOWBALL {
        @Override
        protected Analyzer create(Version version) {
            Analyzer analyzer = new SnowballAnalyzer("English", StopAnalyzer.ENGLISH_STOP_WORDS_SET);
            analyzer.setVersion(version.luceneVersion);
            return analyzer;
        }
    },

    PATTERN(CachingStrategy.ELASTICSEARCH) {
        @Override
        protected Analyzer create(Version version) {
            return new PatternAnalyzer(Regex.compile("\\W+" /*PatternAnalyzer.NON_WORD_PATTERN*/, null), true, CharArraySet.EMPTY_SET);
        }
    },

    STANDARD_HTML_STRIP(CachingStrategy.ELASTICSEARCH) {
        @Override
        protected Analyzer create(Version version) {
            final Analyzer analyzer = new StandardHtmlStripAnalyzer(CharArraySet.EMPTY_SET);
            analyzer.setVersion(version.luceneVersion);
            return analyzer;
        }
    },

    ARABIC {
        @Override
        protected Analyzer create(Version version) {
            Analyzer a = new ArabicAnalyzer();
            a.setVersion(version.luceneVersion);
            return a;
        }
    },

    ARMENIAN {
        @Override
        protected Analyzer create(Version version) {
            Analyzer a = new ArmenianAnalyzer();
            a.setVersion(version.luceneVersion);
            return a;
        }
    },

    BASQUE {
        @Override
        protected Analyzer create(Version version) {
            Analyzer a = new BasqueAnalyzer();
            a.setVersion(version.luceneVersion);
            return a;
        }
    },

    BRAZILIAN {
        @Override
        protected Analyzer create(Version version) {
            Analyzer a = new BrazilianAnalyzer();
            a.setVersion(version.luceneVersion);
            return a;
        }
    },

    BULGARIAN {
        @Override
        protected Analyzer create(Version version) {
            Analyzer a = new BulgarianAnalyzer();
            a.setVersion(version.luceneVersion);
            return a;
        }
    },

    CATALAN {
        @Override
        protected Analyzer create(Version version) {
            Analyzer a = new CatalanAnalyzer();
            a.setVersion(version.luceneVersion);
            return a;
        }
    },

    CHINESE(CachingStrategy.ONE) {
        @Override
        protected Analyzer create(Version version) {
            Analyzer a = new StandardAnalyzer();
            a.setVersion(version.luceneVersion);
            return a;
        }
    },

    CJK {
        @Override
        protected Analyzer create(Version version) {
            Analyzer a = new CJKAnalyzer();
            a.setVersion(version.luceneVersion);
            return a;
        }
    },

    CZECH {
        @Override
        protected Analyzer create(Version version) {
            Analyzer a = new CzechAnalyzer();
            a.setVersion(version.luceneVersion);
            return a;
        }
    },

    DUTCH {
        @Override
        protected Analyzer create(Version version) {
            Analyzer a = new DutchAnalyzer();
            a.setVersion(version.luceneVersion);
            return a;
        }
    },

    DANISH {
        @Override
        protected Analyzer create(Version version) {
            Analyzer a = new DanishAnalyzer();
            a.setVersion(version.luceneVersion);
            return a;
        }
    },

    ENGLISH {
        @Override
        protected Analyzer create(Version version) {
            Analyzer a = new EnglishAnalyzer();
            a.setVersion(version.luceneVersion);
            return a;
        }
    },

    FINNISH {
        @Override
        protected Analyzer create(Version version) {
            Analyzer a = new FinnishAnalyzer();
            a.setVersion(version.luceneVersion);
            return a;
        }
    },

    FRENCH {
        @Override
        protected Analyzer create(Version version) {
            Analyzer a = new FrenchAnalyzer();
            a.setVersion(version.luceneVersion);
            return a;
        }
    },

    GALICIAN {
        @Override
        protected Analyzer create(Version version) {
            Analyzer a = new GalicianAnalyzer();
            a.setVersion(version.luceneVersion);
            return a;
        }
    },

    GERMAN {
        @Override
        protected Analyzer create(Version version) {
            Analyzer a = new GermanAnalyzer();
            a.setVersion(version.luceneVersion);
            return a;
        }
    },

    GREEK {
        @Override
        protected Analyzer create(Version version) {
            Analyzer a = new GreekAnalyzer();
            a.setVersion(version.luceneVersion);
            return a;
        }
    },

    HINDI {
        @Override
        protected Analyzer create(Version version) {
            Analyzer a = new HindiAnalyzer();
            a.setVersion(version.luceneVersion);
            return a;
        }
    },

    HUNGARIAN {
        @Override
        protected Analyzer create(Version version) {
            Analyzer a = new HungarianAnalyzer();
            a.setVersion(version.luceneVersion);
            return a;
        }
    },

    INDONESIAN {
        @Override
        protected Analyzer create(Version version) {
            Analyzer a = new IndonesianAnalyzer();
            a.setVersion(version.luceneVersion);
            return a;
        }
    },

    IRISH {
        @Override
        protected Analyzer create(Version version) {
            Analyzer a = new IrishAnalyzer();
            a.setVersion(version.luceneVersion);
            return a;
        }
    },

    ITALIAN {
        @Override
        protected Analyzer create(Version version) {
            Analyzer a = new ItalianAnalyzer();
            a.setVersion(version.luceneVersion);
            return a;
        }
    },

    LATVIAN {
        @Override
        protected Analyzer create(Version version) {
            Analyzer a = new LatvianAnalyzer();
            a.setVersion(version.luceneVersion);
            return a;
        }
    },

    LITHUANIAN {
        @Override
        protected Analyzer create(Version version) {
            Analyzer a = new LithuanianAnalyzer();
            a.setVersion(version.luceneVersion);
            return a;
        }
    },

    NORWEGIAN {
        @Override
        protected Analyzer create(Version version) {
            Analyzer a = new NorwegianAnalyzer();
            a.setVersion(version.luceneVersion);
            return a;
        }
    },

    PERSIAN {
        @Override
        protected Analyzer create(Version version) {
            Analyzer a = new PersianAnalyzer();
            a.setVersion(version.luceneVersion);
            return a;
        }
    },

    PORTUGUESE {
        @Override
        protected Analyzer create(Version version) {
            Analyzer a = new PortugueseAnalyzer();
            a.setVersion(version.luceneVersion);
            return a;
        }
    },

    ROMANIAN {
        @Override
        protected Analyzer create(Version version) {
            Analyzer a = new RomanianAnalyzer();
            a.setVersion(version.luceneVersion);
            return a;
        }
    },

    RUSSIAN {
        @Override
        protected Analyzer create(Version version) {
            Analyzer a = new RussianAnalyzer();
            a.setVersion(version.luceneVersion);
            return a;
        }
    },

    SORANI {
        @Override
        protected Analyzer create(Version version) {
            Analyzer a = new SoraniAnalyzer();
            a.setVersion(version.luceneVersion);
            return a;
        }
    },

    SPANISH {
        @Override
        protected Analyzer create(Version version) {
            Analyzer a = new SpanishAnalyzer();
            a.setVersion(version.luceneVersion);
            return a;
        }
    },

    SWEDISH {
        @Override
        protected Analyzer create(Version version) {
            Analyzer a = new SwedishAnalyzer();
            a.setVersion(version.luceneVersion);
            return a;
        }
    },

    TURKISH {
        @Override
        protected Analyzer create(Version version) {
            Analyzer a = new TurkishAnalyzer();
            a.setVersion(version.luceneVersion);
            return a;
        }
    },

    THAI {
        @Override
        protected Analyzer create(Version version) {
            Analyzer a = new ThaiAnalyzer();
            a.setVersion(version.luceneVersion);
            return a;
        }
    };

    abstract protected Analyzer create(Version version);

    protected final PreBuiltCacheFactory.PreBuiltCache<Analyzer> cache;

    PreBuiltAnalyzers() {
        this(PreBuiltCacheFactory.CachingStrategy.LUCENE);
    }

    PreBuiltAnalyzers(PreBuiltCacheFactory.CachingStrategy cachingStrategy) {
        cache = PreBuiltCacheFactory.getCache(cachingStrategy);
    }

    PreBuiltCacheFactory.PreBuiltCache<Analyzer> getCache() {
        return cache;
    }

    public synchronized Analyzer getAnalyzer(Version version) {
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
