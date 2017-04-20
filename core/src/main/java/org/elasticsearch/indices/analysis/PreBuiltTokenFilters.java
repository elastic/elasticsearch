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

import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ar.ArabicNormalizationFilter;
import org.apache.lucene.analysis.ar.ArabicStemFilter;
import org.apache.lucene.analysis.br.BrazilianStemFilter;
import org.apache.lucene.analysis.cjk.CJKBigramFilter;
import org.apache.lucene.analysis.cjk.CJKWidthFilter;
import org.apache.lucene.analysis.ckb.SoraniNormalizationFilter;
import org.apache.lucene.analysis.core.DecimalDigitFilter;
import org.apache.lucene.analysis.cz.CzechStemFilter;
import org.apache.lucene.analysis.de.GermanNormalizationFilter;
import org.apache.lucene.analysis.de.GermanStemFilter;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.fa.PersianNormalizationFilter;
import org.apache.lucene.analysis.fr.FrenchAnalyzer;
import org.apache.lucene.analysis.hi.HindiNormalizationFilter;
import org.apache.lucene.analysis.in.IndicNormalizationFilter;
import org.apache.lucene.analysis.miscellaneous.KeywordRepeatFilter;
import org.apache.lucene.analysis.miscellaneous.LimitTokenCountFilter;
import org.apache.lucene.analysis.miscellaneous.ScandinavianFoldingFilter;
import org.apache.lucene.analysis.miscellaneous.ScandinavianNormalizationFilter;
import org.apache.lucene.analysis.payloads.DelimitedPayloadTokenFilter;
import org.apache.lucene.analysis.payloads.TypeAsPayloadTokenFilter;
import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.apache.lucene.analysis.snowball.SnowballFilter;
import org.apache.lucene.analysis.tr.ApostropheFilter;
import org.apache.lucene.analysis.util.ElisionFilter;
import org.elasticsearch.Version;
import org.elasticsearch.index.analysis.DelimitedPayloadTokenFilterFactory;
import org.elasticsearch.index.analysis.LimitTokenCountFilterFactory;
import org.elasticsearch.index.analysis.MultiTermAwareComponent;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.indices.analysis.PreBuiltCacheFactory.CachingStrategy;
import org.tartarus.snowball.ext.DutchStemmer;
import org.tartarus.snowball.ext.FrenchStemmer;

import java.util.Locale;

public enum PreBuiltTokenFilters {
    // TODO remove this entire class when PreBuiltTokenizers no longer needs it.....
    LOWERCASE(CachingStrategy.LUCENE) {
        @Override
        public TokenStream create(TokenStream tokenStream, Version version) {
            return new LowerCaseFilter(tokenStream);
        }
        @Override
        protected boolean isMultiTermAware() {
            return true;
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
        @Override
        protected boolean isMultiTermAware() {
            return true;
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
            return new SnowballFilter(tokenStream, new DutchStemmer());
        }
    },

    FRENCH_STEM(CachingStrategy.ONE) {
        @Override
        public TokenStream create(TokenStream tokenStream, Version version) {
            return new SnowballFilter(tokenStream, new FrenchStemmer());
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
        @Override
        protected boolean isMultiTermAware() {
            return true;
        }
    },

    PERSIAN_NORMALIZATION(CachingStrategy.ONE) {
        @Override
        public TokenStream create(TokenStream tokenStream, Version version) {
            return new PersianNormalizationFilter(tokenStream);
        }
        @Override
        protected boolean isMultiTermAware() {
            return true;
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
    },

    GERMAN_NORMALIZATION(CachingStrategy.ONE) {
        @Override
        public TokenStream create(TokenStream tokenStream, Version version) {
            return new GermanNormalizationFilter(tokenStream);
        }
        @Override
        protected boolean isMultiTermAware() {
            return true;
        }
    },

    HINDI_NORMALIZATION(CachingStrategy.ONE) {
        @Override
        public TokenStream create(TokenStream tokenStream, Version version) {
            return new HindiNormalizationFilter(tokenStream);
        }
        @Override
        protected boolean isMultiTermAware() {
            return true;
        }
    },

    INDIC_NORMALIZATION(CachingStrategy.ONE) {
        @Override
        public TokenStream create(TokenStream tokenStream, Version version) {
            return new IndicNormalizationFilter(tokenStream);
        }
        @Override
        protected boolean isMultiTermAware() {
            return true;
        }
    },

    SORANI_NORMALIZATION(CachingStrategy.ONE) {
        @Override
        public TokenStream create(TokenStream tokenStream, Version version) {
            return new SoraniNormalizationFilter(tokenStream);
        }
        @Override
        protected boolean isMultiTermAware() {
            return true;
        }
    },

    SCANDINAVIAN_NORMALIZATION(CachingStrategy.ONE) {
        @Override
        public TokenStream create(TokenStream tokenStream, Version version) {
            return new ScandinavianNormalizationFilter(tokenStream);
        }
        @Override
        protected boolean isMultiTermAware() {
            return true;
        }
    },

    SCANDINAVIAN_FOLDING(CachingStrategy.ONE) {
        @Override
        public TokenStream create(TokenStream tokenStream, Version version) {
            return new ScandinavianFoldingFilter(tokenStream);
        }
        @Override
        protected boolean isMultiTermAware() {
            return true;
        }
    },

    APOSTROPHE(CachingStrategy.ONE) {
        @Override
        public TokenStream create(TokenStream tokenStream, Version version) {
            return new ApostropheFilter(tokenStream);
        }
    },

    CJK_WIDTH(CachingStrategy.ONE) {
        @Override
        public TokenStream create(TokenStream tokenStream, Version version) {
            return new CJKWidthFilter(tokenStream);
        }
        @Override
        protected boolean isMultiTermAware() {
            return true;
        }
    },

    DECIMAL_DIGIT(CachingStrategy.ONE) {
        @Override
        public TokenStream create(TokenStream tokenStream, Version version) {
            return new DecimalDigitFilter(tokenStream);
        }
        @Override
        protected boolean isMultiTermAware() {
            return true;
        }
    },

    CJK_BIGRAM(CachingStrategy.ONE) {
        @Override
        public TokenStream create(TokenStream tokenStream, Version version) {
            return new CJKBigramFilter(tokenStream);
        }
    },

    DELIMITED_PAYLOAD_FILTER(CachingStrategy.ONE) {
        @Override
        public TokenStream create(TokenStream tokenStream, Version version) {
            return new DelimitedPayloadTokenFilter(tokenStream, DelimitedPayloadTokenFilterFactory.DEFAULT_DELIMITER, DelimitedPayloadTokenFilterFactory.DEFAULT_ENCODER);
        }
    },

    LIMIT(CachingStrategy.ONE) {
        @Override
        public TokenStream create(TokenStream tokenStream, Version version) {
            return new LimitTokenCountFilter(tokenStream, LimitTokenCountFilterFactory.DEFAULT_MAX_TOKEN_COUNT, LimitTokenCountFilterFactory.DEFAULT_CONSUME_ALL_TOKENS);
        }
    },

    ;

    protected boolean isMultiTermAware() {
        return false;
    }

    public abstract TokenStream create(TokenStream tokenStream, Version version);

    protected final PreBuiltCacheFactory.PreBuiltCache<TokenFilterFactory> cache;


    private final CachingStrategy cachingStrategy;
    PreBuiltTokenFilters(CachingStrategy cachingStrategy) {
        this.cachingStrategy = cachingStrategy;
        cache = PreBuiltCacheFactory.getCache(cachingStrategy);
    }

    public CachingStrategy getCachingStrategy() {
        return cachingStrategy;
    }

    private interface MultiTermAwareTokenFilterFactory extends TokenFilterFactory, MultiTermAwareComponent {}

    public synchronized TokenFilterFactory getTokenFilterFactory(final Version version) {
        TokenFilterFactory factory = cache.get(version);
        if (factory == null) {
            final String finalName = name().toLowerCase(Locale.ROOT);
            if (isMultiTermAware()) {
                factory = new MultiTermAwareTokenFilterFactory() {
                    @Override
                    public String name() {
                        return finalName;
                    }

                    @Override
                    public TokenStream create(TokenStream tokenStream) {
                        return PreBuiltTokenFilters.this.create(tokenStream, version);
                    }

                    @Override
                    public Object getMultiTermComponent() {
                        return this;
                    }
                };
            } else {
                factory = new TokenFilterFactory() {
                    @Override
                    public String name() {
                        return finalName;
                    }

                    @Override
                    public TokenStream create(TokenStream tokenStream) {
                        return PreBuiltTokenFilters.this.create(tokenStream, version);
                    }
                };
            }
            cache.put(version, factory);
        }

        return factory;
    }
}
