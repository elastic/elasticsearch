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

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ar.ArabicNormalizationFilter;
import org.apache.lucene.analysis.ar.ArabicStemFilter;
import org.apache.lucene.analysis.br.BrazilianStemFilter;
import org.apache.lucene.analysis.commongrams.CommonGramsFilter;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.core.UpperCaseFilter;
import org.apache.lucene.analysis.cz.CzechStemFilter;
import org.apache.lucene.analysis.de.GermanStemFilter;
import org.apache.lucene.analysis.en.KStemFilter;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.fa.PersianNormalizationFilter;
import org.apache.lucene.analysis.fr.FrenchAnalyzer;
import org.apache.lucene.analysis.fr.FrenchStemFilter;
import org.apache.lucene.analysis.miscellaneous.*;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenFilter;
import org.apache.lucene.analysis.ngram.NGramTokenFilter;
import org.apache.lucene.analysis.nl.DutchStemFilter;
import org.apache.lucene.analysis.payloads.TypeAsPayloadTokenFilter;
import org.apache.lucene.analysis.reverse.ReverseStringFilter;
import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.apache.lucene.analysis.snowball.SnowballFilter;
import org.apache.lucene.analysis.standard.ClassicFilter;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.util.ElisionFilter;
import org.elasticsearch.Version;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.indices.analysis.PreBuiltCacheFactory.CachingStrategy;

import java.util.Locale;

/**
 *
 */
public enum PreBuiltTokenFilters {

    WORD_DELIMITER(CachingStrategy.ONE) {
        @Override
        public TokenStream create(TokenStream tokenStream, Version version) {
            if (version.luceneVersion.onOrAfter(org.apache.lucene.util.Version.LUCENE_4_8)) {
                return new WordDelimiterFilter(version.luceneVersion, tokenStream,
                           WordDelimiterFilter.GENERATE_WORD_PARTS |
                           WordDelimiterFilter.GENERATE_NUMBER_PARTS |
                           WordDelimiterFilter.SPLIT_ON_CASE_CHANGE |
                           WordDelimiterFilter.SPLIT_ON_NUMERICS |
                           WordDelimiterFilter.STEM_ENGLISH_POSSESSIVE, null);
            } else {
                return new Lucene47WordDelimiterFilter(tokenStream,
                           WordDelimiterFilter.GENERATE_WORD_PARTS |
                           WordDelimiterFilter.GENERATE_NUMBER_PARTS |
                           WordDelimiterFilter.SPLIT_ON_CASE_CHANGE |
                           WordDelimiterFilter.SPLIT_ON_NUMERICS |
                           WordDelimiterFilter.STEM_ENGLISH_POSSESSIVE, null);
            }
        }
    },

    STOP(CachingStrategy.LUCENE) {
        @Override
        public TokenStream create(TokenStream tokenStream, Version version) {
            return new StopFilter(version.luceneVersion, tokenStream, StopAnalyzer.ENGLISH_STOP_WORDS_SET);
        }
    },

    TRIM(CachingStrategy.LUCENE) {
        @Override
        public TokenStream create(TokenStream tokenStream, Version version) {
            return new TrimFilter(version.luceneVersion, tokenStream);
        }
    },

    REVERSE(CachingStrategy.LUCENE) {
        @Override
        public TokenStream create(TokenStream tokenStream, Version version) {
            return new ReverseStringFilter(version.luceneVersion, tokenStream);
        }
    },

    ASCIIFOLDING(CachingStrategy.ONE) {
        @Override
        public TokenStream create(TokenStream tokenStream, Version version) {
            return new ASCIIFoldingFilter(tokenStream);
        }
    },

    LENGTH(CachingStrategy.LUCENE) {
        @Override
        public TokenStream create(TokenStream tokenStream, Version version) {
            return new LengthFilter(version.luceneVersion, tokenStream, 0, Integer.MAX_VALUE);
        }
    },

    COMMON_GRAMS(CachingStrategy.LUCENE) {
        @Override
        public TokenStream create(TokenStream tokenStream, Version version) {
            return new CommonGramsFilter(version.luceneVersion, tokenStream, CharArraySet.EMPTY_SET);
        }
    },

    LOWERCASE(CachingStrategy.LUCENE) {
        @Override
        public TokenStream create(TokenStream tokenStream, Version version) {
            return new LowerCaseFilter(version.luceneVersion, tokenStream);
        }
    },

    UPPERCASE(CachingStrategy.LUCENE) {
        @Override
        public TokenStream create(TokenStream tokenStream, Version version) {
            return new UpperCaseFilter(version.luceneVersion, tokenStream);
        }
    },

    KSTEM(CachingStrategy.ONE) {
        @Override
        public TokenStream create(TokenStream tokenStream, Version version) {
            return new KStemFilter(tokenStream);
        }
    },

    PORTER_STEM(CachingStrategy.ONE) {
        @Override
        public TokenStream create(TokenStream tokenStream, Version version) {
            return new PorterStemFilter(tokenStream);
        }
    },

    STANDARD(CachingStrategy.LUCENE) {
        @Override
        public TokenStream create(TokenStream tokenStream, Version version) {
            return new StandardFilter(version.luceneVersion, tokenStream);
        }
    },

    CLASSIC(CachingStrategy.ONE) {
        @Override
        public TokenStream create(TokenStream tokenStream, Version version) {
            return new ClassicFilter(tokenStream);
        }
    },

    NGRAM(CachingStrategy.LUCENE) {
        @Override
        public TokenStream create(TokenStream tokenStream, Version version) {
            return new NGramTokenFilter(version.luceneVersion, tokenStream);
        }
    },

    EDGE_NGRAM(CachingStrategy.LUCENE) {
        @Override
        public TokenStream create(TokenStream tokenStream, Version version) {
            return new EdgeNGramTokenFilter(version.luceneVersion, tokenStream, EdgeNGramTokenFilter.DEFAULT_MIN_GRAM_SIZE, EdgeNGramTokenFilter.DEFAULT_MAX_GRAM_SIZE);
        }
    },

    UNIQUE(CachingStrategy.ONE) {
        @Override
        public TokenStream create(TokenStream tokenStream, Version version) {
            return new UniqueTokenFilter(tokenStream);
        }
    },

    TRUNCATE(CachingStrategy.ONE) {
        @Override
        public TokenStream create(TokenStream tokenStream, Version version) {
            return new TruncateTokenFilter(tokenStream, 10);
        }
    },

    // Extended Token Filters
    SNOWBALL(CachingStrategy.ONE) {
        @Override
        public TokenStream create(TokenStream tokenStream, Version version) {
            return new SnowballFilter(tokenStream, "English");
        }
    },

    STEMMER(CachingStrategy.ONE) {
        @Override
        public TokenStream create(TokenStream tokenStream, Version version) {
            return new PorterStemFilter(tokenStream);
        }
    },

    ELISION(CachingStrategy.ONE) {
        @Override
        public TokenStream create(TokenStream tokenStream, Version version) {
            return new ElisionFilter(tokenStream, FrenchAnalyzer.DEFAULT_ARTICLES);
        }
    },

    ARABIC_STEM(CachingStrategy.ONE) {
        @Override
        public TokenStream create(TokenStream tokenStream, Version version) {
            return new ArabicStemFilter(tokenStream);
        }
    },

    BRAZILIAN_STEM(CachingStrategy.ONE) {
        @Override
        public TokenStream create(TokenStream tokenStream, Version version) {
            return new BrazilianStemFilter(tokenStream);
        }
    },

    CZECH_STEM(CachingStrategy.ONE) {
        @Override
        public TokenStream create(TokenStream tokenStream, Version version) {
            return new CzechStemFilter(tokenStream);
        }
    },

    DUTCH_STEM(CachingStrategy.ONE) {
        @Override
        public TokenStream create(TokenStream tokenStream, Version version) {
            return new DutchStemFilter(tokenStream);
        }
    },

    FRENCH_STEM(CachingStrategy.ONE) {
        @Override
        public TokenStream create(TokenStream tokenStream, Version version) {
            return new FrenchStemFilter(tokenStream);
        }
    },

    GERMAN_STEM(CachingStrategy.ONE) {
        @Override
        public TokenStream create(TokenStream tokenStream, Version version) {
            return new GermanStemFilter(tokenStream);
        }
    },

    RUSSIAN_STEM(CachingStrategy.ONE) {
        @Override
        public TokenStream create(TokenStream tokenStream, Version version) {
            return new SnowballFilter(tokenStream, "Russian");
        }
    },

    KEYWORD_REPEAT(CachingStrategy.ONE) {
        @Override
        public TokenStream create(TokenStream tokenStream, Version version) {
            return new KeywordRepeatFilter(tokenStream);
        }
    },

    ARABIC_NORMALIZATION(CachingStrategy.ONE) {
        @Override
        public TokenStream create(TokenStream tokenStream, Version version) {
            return new ArabicNormalizationFilter(tokenStream);
        }
    },

    PERSIAN_NORMALIZATION(CachingStrategy.ONE) {
        @Override
        public TokenStream create(TokenStream tokenStream, Version version) {
            return new PersianNormalizationFilter(tokenStream);
        }
    },

    TYPE_AS_PAYLOAD(CachingStrategy.ONE) {
        @Override
        public TokenStream create(TokenStream tokenStream, Version version) {
            return new TypeAsPayloadTokenFilter(tokenStream);
        }
    },

    SHINGLE(CachingStrategy.ONE) {
        @Override
        public TokenStream create(TokenStream tokenStream, Version version) {
            return new ShingleFilter(tokenStream);
        }
    };

    abstract public TokenStream create(TokenStream tokenStream, Version version);

    protected final PreBuiltCacheFactory.PreBuiltCache<TokenFilterFactory> cache;


    PreBuiltTokenFilters(CachingStrategy cachingStrategy) {
        cache = PreBuiltCacheFactory.getCache(cachingStrategy);
    }

    public synchronized TokenFilterFactory getTokenFilterFactory(final Version version) {
        TokenFilterFactory factory = cache.get(version);
        if (factory == null) {
            final String finalName = name();
            factory = new TokenFilterFactory() {
                @Override
                public String name() {
                    return finalName.toLowerCase(Locale.ROOT);
                }

                @Override
                public TokenStream create(TokenStream tokenStream) {
                    return valueOf(finalName).create(tokenStream, version);
                }
            };
            cache.put(version, factory);
        }

        return factory;
    }

    /**
     * Get a pre built TokenFilter by its name or fallback to the default one
     * @param name TokenFilter name
     * @param defaultTokenFilter default TokenFilter if name not found
     */
    public static PreBuiltTokenFilters getOrDefault(String name, PreBuiltTokenFilters defaultTokenFilter) {
        try {
            return valueOf(name.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            return defaultTokenFilter;
        }
    }
}
