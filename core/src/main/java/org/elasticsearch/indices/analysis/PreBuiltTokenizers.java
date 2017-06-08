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

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.core.LetterTokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenizer;
import org.apache.lucene.analysis.ngram.NGramTokenizer;
import org.apache.lucene.analysis.path.PathHierarchyTokenizer;
import org.apache.lucene.analysis.pattern.PatternTokenizer;
import org.apache.lucene.analysis.standard.ClassicTokenizer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.standard.UAX29URLEmailTokenizer;
import org.apache.lucene.analysis.th.ThaiTokenizer;
import org.elasticsearch.Version;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.index.analysis.CustomNormalizerProvider;
import org.elasticsearch.index.analysis.MultiTermAwareComponent;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.indices.analysis.PreBuiltCacheFactory.CachingStrategy;

public enum PreBuiltTokenizers {

    STANDARD(CachingStrategy.ONE) {
        @Override
        protected Tokenizer create(Version version) {
            return new StandardTokenizer();
        }
    },

    CLASSIC(CachingStrategy.ONE) {
        @Override
        protected Tokenizer create(Version version) {
            return new ClassicTokenizer();
        }
    },

    UAX_URL_EMAIL(CachingStrategy.ONE) {
        @Override
        protected Tokenizer create(Version version) {
            return new UAX29URLEmailTokenizer();
        }
    },

    PATH_HIERARCHY(CachingStrategy.ONE) {
        @Override
        protected Tokenizer create(Version version) {
            return new PathHierarchyTokenizer();
        }
    },

    KEYWORD(CachingStrategy.ONE) {
        @Override
        protected Tokenizer create(Version version) {
            return new KeywordTokenizer();
        }
    },

    LETTER(CachingStrategy.ONE) {
        @Override
        protected Tokenizer create(Version version) {
            return new LetterTokenizer();
        }
    },

    WHITESPACE(CachingStrategy.ONE) {
        @Override
        protected Tokenizer create(Version version) {
            return new WhitespaceTokenizer();
        }
    },

    NGRAM(CachingStrategy.ONE) {
        @Override
        protected Tokenizer create(Version version) {
            return new NGramTokenizer();
        }
    },

    EDGE_NGRAM(CachingStrategy.ONE) {
        @Override
        protected Tokenizer create(Version version) {
            return new EdgeNGramTokenizer(EdgeNGramTokenizer.DEFAULT_MIN_GRAM_SIZE, EdgeNGramTokenizer.DEFAULT_MAX_GRAM_SIZE);
        }
    },

    PATTERN(CachingStrategy.ONE) {
        @Override
        protected Tokenizer create(Version version) {
            return new PatternTokenizer(Regex.compile("\\W+", null), -1);
        }
    },

    THAI(CachingStrategy.ONE) {
        @Override
        protected Tokenizer create(Version version) {
            return new ThaiTokenizer();
        }
    }

    ;

    protected abstract  Tokenizer create(Version version);

    protected TokenFilterFactory getMultiTermComponent(Version version) {
        return null;
    }

    protected final PreBuiltCacheFactory.PreBuiltCache<TokenizerFactory> cache;
    private final CachingStrategy cachingStrategy;

    PreBuiltTokenizers(CachingStrategy cachingStrategy) {
        this.cachingStrategy = cachingStrategy;
        cache = PreBuiltCacheFactory.getCache(cachingStrategy);
    }

    public CachingStrategy getCachingStrategy() {
        return cachingStrategy;
    }

    private interface MultiTermAwareTokenizerFactory extends TokenizerFactory, MultiTermAwareComponent {}

    /**
     * Old style resolution for {@link TokenizerFactory}. Exists entirely to keep
     * {@link CustomNormalizerProvider#build(java.util.Map, java.util.Map)} working during the migration.
     */
    public synchronized TokenizerFactory getTokenizerFactory(final Version version) {
            TokenizerFactory tokenizerFactory = cache.get(version);
            if (tokenizerFactory == null) {
                if (getMultiTermComponent(version) != null) {
                    tokenizerFactory = new MultiTermAwareTokenizerFactory() {
                        @Override
                        public Tokenizer create() {
                            return PreBuiltTokenizers.this.create(version);
                        }
    
                        @Override
                        public Object getMultiTermComponent() {
                            return PreBuiltTokenizers.this.getMultiTermComponent(version);
                        }
                    };
                } else {
                    tokenizerFactory = new TokenizerFactory() {
                        @Override
                        public Tokenizer create() {
                            return PreBuiltTokenizers.this.create(version);
                        }
                    };
                }
                cache.put(version, tokenizerFactory);
            }
    
            return tokenizerFactory;
        }
}
