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
import org.apache.lucene.analysis.cn.ChineseAnalyzer;
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
import org.apache.lucene.analysis.lv.LatvianAnalyzer;
import org.apache.lucene.analysis.miscellaneous.PatternAnalyzer;
import org.apache.lucene.analysis.nl.DutchAnalyzer;
import org.apache.lucene.analysis.no.NorwegianAnalyzer;
import org.apache.lucene.analysis.pt.PortugueseAnalyzer;
import org.apache.lucene.analysis.ro.RomanianAnalyzer;
import org.apache.lucene.analysis.ru.RussianAnalyzer;
import org.apache.lucene.analysis.snowball.SnowballAnalyzer;
import org.apache.lucene.analysis.standard.ClassicAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.sv.SwedishAnalyzer;
import org.apache.lucene.analysis.th.ThaiAnalyzer;
import org.apache.lucene.analysis.tr.TurkishAnalyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.elasticsearch.Version;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.index.analysis.StandardHtmlStripAnalyzer;
import org.elasticsearch.indices.analysis.PreBuiltCacheFactory.CachingStrategy;

import java.util.Locale;

/**
 *
 */
public enum PreBuiltAnalyzers {

    STANDARD(CachingStrategy.ELASTICSEARCH) { // we don't do stopwords anymore from 1.0Beta on
        @Override
        protected Analyzer create(Version version) {
            if (version.onOrAfter(Version.V_1_0_0_Beta1)) {
                return new StandardAnalyzer(version.luceneVersion, CharArraySet.EMPTY_SET);
            }
            return new StandardAnalyzer(version.luceneVersion);
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
            return new StopAnalyzer(version.luceneVersion);
        }
    },

    WHITESPACE {
        @Override
        protected Analyzer create(Version version) {
            return new WhitespaceAnalyzer(version.luceneVersion);
        }
    },

    SIMPLE {
        @Override
        protected Analyzer create(Version version) {
            return new SimpleAnalyzer(version.luceneVersion);
        }
    },

    CLASSIC {
        @Override
        protected Analyzer create(Version version) {
            return new ClassicAnalyzer(version.luceneVersion);
        }
    },

    SNOWBALL {
        @Override
        protected Analyzer create(Version version) {
            return new SnowballAnalyzer(version.luceneVersion, "English", StopAnalyzer.ENGLISH_STOP_WORDS_SET);
        }
    },

    PATTERN(CachingStrategy.ELASTICSEARCH) {
        @Override
        protected Analyzer create(Version version) {
            if (version.onOrAfter(Version.V_1_0_0_RC1)) {
                return new PatternAnalyzer(version.luceneVersion, Regex.compile("\\W+" /*PatternAnalyzer.NON_WORD_PATTERN*/, null), true, CharArraySet.EMPTY_SET);
            }
            return new PatternAnalyzer(version.luceneVersion, Regex.compile("\\W+" /*PatternAnalyzer.NON_WORD_PATTERN*/, null), true, StopAnalyzer.ENGLISH_STOP_WORDS_SET);
        }
    },

    STANDARD_HTML_STRIP(CachingStrategy.ELASTICSEARCH) {
        @Override
        protected Analyzer create(Version version) {
            if (version.onOrAfter(Version.V_1_0_0_RC1)) {
                return new StandardHtmlStripAnalyzer(version.luceneVersion, CharArraySet.EMPTY_SET);
            }
            return new StandardHtmlStripAnalyzer(version.luceneVersion);
        }
    },

    ARABIC {
        @Override
        protected Analyzer create(Version version) {
            return new ArabicAnalyzer(version.luceneVersion);
        }
    },

    ARMENIAN {
        @Override
        protected Analyzer create(Version version) {
            return new ArmenianAnalyzer(version.luceneVersion);
        }
    },

    BASQUE {
        @Override
        protected Analyzer create(Version version) {
            return new BasqueAnalyzer(version.luceneVersion);
        }
    },

    BRAZILIAN {
        @Override
        protected Analyzer create(Version version) {
            return new BrazilianAnalyzer(version.luceneVersion);
        }
    },

    BULGARIAN {
        @Override
        protected Analyzer create(Version version) {
            return new BulgarianAnalyzer(version.luceneVersion);
        }
    },

    CATALAN {
        @Override
        protected Analyzer create(Version version) {
            return new CatalanAnalyzer(version.luceneVersion);
        }
    },

    CHINESE(CachingStrategy.ONE) {
        @Override
        protected Analyzer create(Version version) {
            return new ChineseAnalyzer();
        }
    },

    CJK {
        @Override
        protected Analyzer create(Version version) {
            return new CJKAnalyzer(version.luceneVersion);
        }
    },

    CZECH {
        @Override
        protected Analyzer create(Version version) {
            return new CzechAnalyzer(version.luceneVersion);
        }
    },

    DUTCH {
        @Override
        protected Analyzer create(Version version) {
            return new DutchAnalyzer(version.luceneVersion);
        }
    },

    DANISH {
        @Override
        protected Analyzer create(Version version) {
            return new DanishAnalyzer(version.luceneVersion);
        }
    },

    ENGLISH {
        @Override
        protected Analyzer create(Version version) {
            return new EnglishAnalyzer(version.luceneVersion);
        }
    },

    FINNISH {
        @Override
        protected Analyzer create(Version version) {
            return new FinnishAnalyzer(version.luceneVersion);
        }
    },

    FRENCH {
        @Override
        protected Analyzer create(Version version) {
            return new FrenchAnalyzer(version.luceneVersion);
        }
    },

    GALICIAN {
        @Override
        protected Analyzer create(Version version) {
            return new GalicianAnalyzer(version.luceneVersion);
        }
    },

    GERMAN {
        @Override
        protected Analyzer create(Version version) {
            return new GermanAnalyzer(version.luceneVersion);
        }
    },

    GREEK {
        @Override
        protected Analyzer create(Version version) {
            return new GreekAnalyzer(version.luceneVersion);
        }
    },

    HINDI {
        @Override
        protected Analyzer create(Version version) {
            return new HindiAnalyzer(version.luceneVersion);
        }
    },

    HUNGARIAN {
        @Override
        protected Analyzer create(Version version) {
            return new HungarianAnalyzer(version.luceneVersion);
        }
    },

    INDONESIAN {
        @Override
        protected Analyzer create(Version version) {
            return new IndonesianAnalyzer(version.luceneVersion);
        }
    },

    IRISH {
        @Override
        protected Analyzer create(Version version) {
            return new IrishAnalyzer(version.luceneVersion);
        }
    },

    ITALIAN {
        @Override
        protected Analyzer create(Version version) {
            return new ItalianAnalyzer(version.luceneVersion);
        }
    },

    LATVIAN {
        @Override
        protected Analyzer create(Version version) {
            return new LatvianAnalyzer(version.luceneVersion);
        }
    },

    NORWEGIAN {
        @Override
        protected Analyzer create(Version version) {
            return new NorwegianAnalyzer(version.luceneVersion);
        }
    },

    PERSIAN {
        @Override
        protected Analyzer create(Version version) {
            return new PersianAnalyzer(version.luceneVersion);
        }
    },

    PORTUGUESE {
        @Override
        protected Analyzer create(Version version) {
            return new PortugueseAnalyzer(version.luceneVersion);
        }
    },

    ROMANIAN {
        @Override
        protected Analyzer create(Version version) {
            return new RomanianAnalyzer(version.luceneVersion);
        }
    },

    RUSSIAN {
        @Override
        protected Analyzer create(Version version) {
            return new RussianAnalyzer(version.luceneVersion);
        }
    },

    SPANISH {
        @Override
        protected Analyzer create(Version version) {
            return new SpanishAnalyzer(version.luceneVersion);
        }
    },

    SWEDISH {
        @Override
        protected Analyzer create(Version version) {
            return new SwedishAnalyzer(version.luceneVersion);
        }
    },

    TURKISH {
        @Override
        protected Analyzer create(Version version) {
            return new TurkishAnalyzer(version.luceneVersion);
        }
    },

    THAI {
        @Override
        protected Analyzer create(Version version) {
            return new ThaiAnalyzer(version.luceneVersion);
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
