/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.ar.ArabicNormalizationFilter;
import org.apache.lucene.analysis.ar.ArabicStemFilter;
import org.apache.lucene.analysis.br.BrazilianStemFilter;
import org.apache.lucene.analysis.charfilter.HTMLStripCharFilter;
import org.apache.lucene.analysis.commongrams.CommonGramsFilter;
import org.apache.lucene.analysis.core.*;
import org.apache.lucene.analysis.cz.CzechStemFilter;
import org.apache.lucene.analysis.de.GermanStemFilter;
import org.apache.lucene.analysis.en.KStemFilter;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.fa.PersianNormalizationFilter;
import org.apache.lucene.analysis.fr.FrenchAnalyzer;
import org.apache.lucene.analysis.fr.FrenchStemFilter;
import org.apache.lucene.analysis.miscellaneous.*;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenFilter;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenizer;
import org.apache.lucene.analysis.ngram.NGramTokenFilter;
import org.apache.lucene.analysis.ngram.NGramTokenizer;
import org.apache.lucene.analysis.nl.DutchStemFilter;
import org.apache.lucene.analysis.path.PathHierarchyTokenizer;
import org.apache.lucene.analysis.pattern.PatternTokenizer;
import org.apache.lucene.analysis.payloads.TypeAsPayloadTokenFilter;
import org.apache.lucene.analysis.reverse.ReverseStringFilter;
import org.apache.lucene.analysis.snowball.SnowballFilter;
import org.apache.lucene.analysis.standard.*;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.util.ElisionFilter;
import org.elasticsearch.Version;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.analysis.*;

import java.io.Reader;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.common.settings.ImmutableSettings.Builder.EMPTY_SETTINGS;

/**
 * A node level registry of analyzers, to be reused by different indices which use default analyzers.
 */
public class IndicesAnalysisService extends AbstractComponent {

    private final Map<String, PreBuiltAnalyzerProviderFactory> analyzerProviderFactories = ConcurrentCollections.newConcurrentMap();
    private final Map<String, PreBuiltTokenizerFactoryFactory> tokenizerFactories = ConcurrentCollections.newConcurrentMap();
    private final Map<String, PreBuiltTokenFilterFactoryFactory> tokenFilterFactories = ConcurrentCollections.newConcurrentMap();
    private final Map<String, PreBuiltCharFilterFactoryFactory> charFilterFactories = ConcurrentCollections.newConcurrentMap();

    public IndicesAnalysisService() {
        super(EMPTY_SETTINGS);
    }

    @Inject
    public IndicesAnalysisService(Settings settings) {
        super(settings);

        for (PreBuiltAnalyzers preBuiltAnalyzerEnum : PreBuiltAnalyzers.values()) {
            String name = preBuiltAnalyzerEnum.name().toLowerCase(Locale.ROOT);
            analyzerProviderFactories.put(name, new PreBuiltAnalyzerProviderFactory(name, AnalyzerScope.INDICES, preBuiltAnalyzerEnum.getAnalyzer(Version.CURRENT)));
        }

        // Base Tokenizers
        tokenizerFactories.put("standard", new PreBuiltTokenizerFactoryFactory(new TokenizerFactory() {
            @Override
            public String name() {
                return "standard";
            }

            @Override
            public Tokenizer create(Reader reader) {
                return new StandardTokenizer(Lucene.ANALYZER_VERSION, reader);
            }
        }));

        tokenizerFactories.put("classic", new PreBuiltTokenizerFactoryFactory(new TokenizerFactory() {
            @Override
            public String name() {
                return "classic";
            }

            @Override
            public Tokenizer create(Reader reader) {
                return new ClassicTokenizer(Lucene.ANALYZER_VERSION, reader);
            }
        }));

        tokenizerFactories.put("uax_url_email", new PreBuiltTokenizerFactoryFactory(new TokenizerFactory() {
            @Override
            public String name() {
                return "uax_url_email";
            }

            @Override
            public Tokenizer create(Reader reader) {
                return new UAX29URLEmailTokenizer(Lucene.ANALYZER_VERSION, reader);
            }
        }));

        tokenizerFactories.put("path_hierarchy", new PreBuiltTokenizerFactoryFactory(new TokenizerFactory() {
            @Override
            public String name() {
                return "path_hierarchy";
            }

            @Override
            public Tokenizer create(Reader reader) {
                return new PathHierarchyTokenizer(reader);
            }
        }));

        tokenizerFactories.put("keyword", new PreBuiltTokenizerFactoryFactory(new TokenizerFactory() {
            @Override
            public String name() {
                return "keyword";
            }

            @Override
            public Tokenizer create(Reader reader) {
                return new KeywordTokenizer(reader);
            }
        }));

        tokenizerFactories.put("letter", new PreBuiltTokenizerFactoryFactory(new TokenizerFactory() {
            @Override
            public String name() {
                return "letter";
            }

            @Override
            public Tokenizer create(Reader reader) {
                return new LetterTokenizer(Lucene.ANALYZER_VERSION, reader);
            }
        }));

        tokenizerFactories.put("lowercase", new PreBuiltTokenizerFactoryFactory(new TokenizerFactory() {
            @Override
            public String name() {
                return "lowercase";
            }

            @Override
            public Tokenizer create(Reader reader) {
                return new LowerCaseTokenizer(Lucene.ANALYZER_VERSION, reader);
            }
        }));

        tokenizerFactories.put("whitespace", new PreBuiltTokenizerFactoryFactory(new TokenizerFactory() {
            @Override
            public String name() {
                return "whitespace";
            }

            @Override
            public Tokenizer create(Reader reader) {
                return new WhitespaceTokenizer(Lucene.ANALYZER_VERSION, reader);
            }
        }));

        tokenizerFactories.put("nGram", new PreBuiltTokenizerFactoryFactory(new TokenizerFactory() {
            @Override
            public String name() {
                return "nGram";
            }

            @Override
            public Tokenizer create(Reader reader) {
                return new NGramTokenizer(Lucene.ANALYZER_VERSION, reader);
            }
        }));

        tokenizerFactories.put("ngram", new PreBuiltTokenizerFactoryFactory(new TokenizerFactory() {
            @Override
            public String name() {
                return "ngram";
            }

            @Override
            public Tokenizer create(Reader reader) {
                return new NGramTokenizer(Lucene.ANALYZER_VERSION, reader);
            }
        }));

        tokenizerFactories.put("edgeNGram", new PreBuiltTokenizerFactoryFactory(new TokenizerFactory() {
            @Override
            public String name() {
                return "edgeNGram";
            }

            @Override
            public Tokenizer create(Reader reader) {
                return new EdgeNGramTokenizer(Lucene.ANALYZER_VERSION, reader, EdgeNGramTokenizer.DEFAULT_MIN_GRAM_SIZE, EdgeNGramTokenizer.DEFAULT_MAX_GRAM_SIZE);
            }
        }));

        tokenizerFactories.put("edge_ngram", new PreBuiltTokenizerFactoryFactory(new TokenizerFactory() {
            @Override
            public String name() {
                return "edge_ngram";
            }

            @Override
            public Tokenizer create(Reader reader) {
                return new EdgeNGramTokenizer(Lucene.ANALYZER_VERSION, reader, EdgeNGramTokenizer.DEFAULT_MIN_GRAM_SIZE, EdgeNGramTokenizer.DEFAULT_MAX_GRAM_SIZE);
            }
        }));

        tokenizerFactories.put("pattern", new PreBuiltTokenizerFactoryFactory(new TokenizerFactory() {
            @Override
            public String name() {
                return "pattern";
            }

            @Override
            public Tokenizer create(Reader reader) {
                return new PatternTokenizer(reader, Regex.compile("\\W+", null), -1);
            }
        }));

        // Token Filters
        tokenFilterFactories.put("word_delimiter", new PreBuiltTokenFilterFactoryFactory(new TokenFilterFactory() {
            @Override
            public String name() {
                return "word_delimiter";
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                return new WordDelimiterFilter(tokenStream, 
                        WordDelimiterFilter.GENERATE_WORD_PARTS |
                        WordDelimiterFilter.GENERATE_NUMBER_PARTS |
                        WordDelimiterFilter.SPLIT_ON_CASE_CHANGE | 
                        WordDelimiterFilter.SPLIT_ON_NUMERICS |
                        WordDelimiterFilter.STEM_ENGLISH_POSSESSIVE, null);
            }
        }));
        tokenFilterFactories.put("stop", new PreBuiltTokenFilterFactoryFactory(new TokenFilterFactory() {
            @Override
            public String name() {
                return "stop";
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                return new StopFilter(Lucene.ANALYZER_VERSION, tokenStream, StopAnalyzer.ENGLISH_STOP_WORDS_SET);
            }
        }));

        tokenFilterFactories.put("trim", new PreBuiltTokenFilterFactoryFactory(new TokenFilterFactory() {
            @Override
            public String name() {
                return "trim";
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                return new TrimFilter(Lucene.ANALYZER_VERSION, tokenStream);
            }
        }));

        tokenFilterFactories.put("reverse", new PreBuiltTokenFilterFactoryFactory(new TokenFilterFactory() {
            @Override
            public String name() {
                return "reverse";
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                return new ReverseStringFilter(Lucene.ANALYZER_VERSION, tokenStream);
            }
        }));

        tokenFilterFactories.put("asciifolding", new PreBuiltTokenFilterFactoryFactory(new TokenFilterFactory() {
            @Override
            public String name() {
                return "asciifolding";
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                return new ASCIIFoldingFilter(tokenStream);
            }
        }));

        tokenFilterFactories.put("length", new PreBuiltTokenFilterFactoryFactory(new TokenFilterFactory() {
            @Override
            public String name() {
                return "length";
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                return new LengthFilter(Lucene.ANALYZER_VERSION, tokenStream, 0, Integer.MAX_VALUE);
            }
        }));

        tokenFilterFactories.put("common_grams", new PreBuiltTokenFilterFactoryFactory(new TokenFilterFactory() {
            @Override
            public String name() {
                return "common_grams";
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                return new CommonGramsFilter(Lucene.ANALYZER_VERSION, tokenStream, CharArraySet.EMPTY_SET);
            }
        }));

        tokenFilterFactories.put("lowercase", new PreBuiltTokenFilterFactoryFactory(new TokenFilterFactory() {
            @Override
            public String name() {
                return "lowercase";
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                return new LowerCaseFilter(Lucene.ANALYZER_VERSION, tokenStream);
            }
        }));

        tokenFilterFactories.put("kstem", new PreBuiltTokenFilterFactoryFactory(new TokenFilterFactory() {
            @Override
            public String name() {
                return "kstem";
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                return new KStemFilter(tokenStream);
            }
        }));

        tokenFilterFactories.put("porter_stem", new PreBuiltTokenFilterFactoryFactory(new TokenFilterFactory() {
            @Override
            public String name() {
                return "porter_stem";
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                return new PorterStemFilter(tokenStream);
            }
        }));

        tokenFilterFactories.put("standard", new PreBuiltTokenFilterFactoryFactory(new TokenFilterFactory() {
            @Override
            public String name() {
                return "standard";
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                return new StandardFilter(Lucene.ANALYZER_VERSION, tokenStream);
            }
        }));

        tokenFilterFactories.put("classic", new PreBuiltTokenFilterFactoryFactory(new TokenFilterFactory() {
            @Override
            public String name() {
                return "classic";
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                return new ClassicFilter(tokenStream);
            }
        }));

        tokenFilterFactories.put("nGram", new PreBuiltTokenFilterFactoryFactory(new TokenFilterFactory() {
            @Override
            public String name() {
                return "nGram";
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                return new NGramTokenFilter(Lucene.ANALYZER_VERSION, tokenStream);
            }
        }));

        tokenFilterFactories.put("ngram", new PreBuiltTokenFilterFactoryFactory(new TokenFilterFactory() {
            @Override
            public String name() {
                return "ngram";
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                return new NGramTokenFilter(Lucene.ANALYZER_VERSION, tokenStream);
            }
        }));

        tokenFilterFactories.put("edgeNGram", new PreBuiltTokenFilterFactoryFactory(new TokenFilterFactory() {
            @Override
            public String name() {
                return "edgeNGram";
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                return new EdgeNGramTokenFilter(Lucene.ANALYZER_VERSION, tokenStream, EdgeNGramTokenFilter.DEFAULT_MIN_GRAM_SIZE, EdgeNGramTokenFilter.DEFAULT_MAX_GRAM_SIZE);
            }
        }));

        tokenFilterFactories.put("edge_ngram", new PreBuiltTokenFilterFactoryFactory(new TokenFilterFactory() {
            @Override
            public String name() {
                return "edge_ngram";
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                return new EdgeNGramTokenFilter(Lucene.ANALYZER_VERSION, tokenStream, EdgeNGramTokenFilter.DEFAULT_MIN_GRAM_SIZE, EdgeNGramTokenFilter.DEFAULT_MAX_GRAM_SIZE);
            }
        }));

        tokenFilterFactories.put("shingle", new PreBuiltTokenFilterFactoryFactory(new ShingleTokenFilterFactory.Factory("shingle")));

        tokenFilterFactories.put("unique", new PreBuiltTokenFilterFactoryFactory(new TokenFilterFactory() {
            @Override
            public String name() {
                return "unique";
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                return new UniqueTokenFilter(tokenStream);
            }
        }));

        tokenFilterFactories.put("truncate", new PreBuiltTokenFilterFactoryFactory(new TokenFilterFactory() {
            @Override
            public String name() {
                return "truncate";
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                return new TruncateTokenFilter(tokenStream, 10);
            }
        }));

        // Extended Token Filters
        tokenFilterFactories.put("snowball", new PreBuiltTokenFilterFactoryFactory(new TokenFilterFactory() {
            @Override
            public String name() {
                return "snowball";
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                return new SnowballFilter(tokenStream, "English");
            }
        }));
        tokenFilterFactories.put("stemmer", new PreBuiltTokenFilterFactoryFactory(new TokenFilterFactory() {
            @Override
            public String name() {
                return "stemmer";
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                return new PorterStemFilter(tokenStream);
            }
        }));
        tokenFilterFactories.put("elision", new PreBuiltTokenFilterFactoryFactory(new TokenFilterFactory() {
            @Override
            public String name() {
                return "elision";
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                // LUCENE 4 UPGRADE: French default for now, make set of articles configurable
                return new ElisionFilter(tokenStream, FrenchAnalyzer.DEFAULT_ARTICLES);
            }
        }));
        tokenFilterFactories.put("arabic_stem", new PreBuiltTokenFilterFactoryFactory(new TokenFilterFactory() {
            @Override
            public String name() {
                return "arabic_stem";
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                return new ArabicStemFilter(tokenStream);
            }
        }));
        tokenFilterFactories.put("brazilian_stem", new PreBuiltTokenFilterFactoryFactory(new TokenFilterFactory() {
            @Override
            public String name() {
                return "brazilian_stem";
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                return new BrazilianStemFilter(tokenStream);
            }
        }));
        tokenFilterFactories.put("czech_stem", new PreBuiltTokenFilterFactoryFactory(new TokenFilterFactory() {
            @Override
            public String name() {
                return "czech_stem";
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                return new CzechStemFilter(tokenStream);
            }
        }));
        tokenFilterFactories.put("dutch_stem", new PreBuiltTokenFilterFactoryFactory(new TokenFilterFactory() {
            @Override
            public String name() {
                return "dutch_stem";
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                return new DutchStemFilter(tokenStream);
            }
        }));
        tokenFilterFactories.put("french_stem", new PreBuiltTokenFilterFactoryFactory(new TokenFilterFactory() {
            @Override
            public String name() {
                return "french_stem";
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                return new FrenchStemFilter(tokenStream);
            }
        }));
        tokenFilterFactories.put("german_stem", new PreBuiltTokenFilterFactoryFactory(new TokenFilterFactory() {
            @Override
            public String name() {
                return "german_stem";
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                return new GermanStemFilter(tokenStream);
            }
        }));
        tokenFilterFactories.put("russian_stem", new PreBuiltTokenFilterFactoryFactory(new TokenFilterFactory() {
            @Override
            public String name() {
                return "russian_stem";
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                return new SnowballFilter(tokenStream, "Russian");
            }
        }));
        tokenFilterFactories.put("keyword_repeat", new PreBuiltTokenFilterFactoryFactory(new TokenFilterFactory() {
            @Override
            public String name() {
                return "keyword_repeat";
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                return new KeywordRepeatFilter(tokenStream);
            }
        }));
        tokenFilterFactories.put("arabic_normalization", new PreBuiltTokenFilterFactoryFactory(new TokenFilterFactory() {
            @Override
            public String name() {
                return "arabic_normalization";
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                return new ArabicNormalizationFilter(tokenStream);
            }
        }));
        tokenFilterFactories.put("persian_normalization", new PreBuiltTokenFilterFactoryFactory(new TokenFilterFactory() {
            @Override
            public String name() {
                return "persian_normalization";
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                return new PersianNormalizationFilter(tokenStream);
            }
        }));

        tokenFilterFactories.put("type_as_payload", new PreBuiltTokenFilterFactoryFactory(new TokenFilterFactory() {
            
            @Override
            public String name() {
                return "type_as_payload";
            }
            
            @Override
            public TokenStream create(TokenStream tokenStream) {
                return new TypeAsPayloadTokenFilter(tokenStream);
            }
        }));

        // Char Filter
        charFilterFactories.put("html_strip", new PreBuiltCharFilterFactoryFactory(new CharFilterFactory() {
            @Override
            public String name() {
                return "html_strip";
            }

            @Override
            public Reader create(Reader tokenStream) {
                return new HTMLStripCharFilter(tokenStream);
            }
        }));

        charFilterFactories.put("htmlStrip", new PreBuiltCharFilterFactoryFactory(new CharFilterFactory() {
            @Override
            public String name() {
                return "htmlStrip";
            }

            @Override
            public Reader create(Reader tokenStream) {
                return new HTMLStripCharFilter(tokenStream);
            }
        }));
    }

    public boolean hasCharFilter(String name) {
        return charFilterFactoryFactory(name) != null;
    }

    public Map<String, PreBuiltCharFilterFactoryFactory> charFilterFactories() {
        return charFilterFactories;
    }

    public CharFilterFactoryFactory charFilterFactoryFactory(String name) {
        return charFilterFactories.get(name);
    }

    public boolean hasTokenFilter(String name) {
        return tokenFilterFactoryFactory(name) != null;
    }

    public Map<String, PreBuiltTokenFilterFactoryFactory> tokenFilterFactories() {
        return tokenFilterFactories;
    }

    public TokenFilterFactoryFactory tokenFilterFactoryFactory(String name) {
        return tokenFilterFactories.get(name);
    }

    public boolean hasTokenizer(String name) {
        return tokenizerFactoryFactory(name) != null;
    }

    public Map<String, PreBuiltTokenizerFactoryFactory> tokenizerFactories() {
        return tokenizerFactories;
    }

    public TokenizerFactoryFactory tokenizerFactoryFactory(String name) {
        return tokenizerFactories.get(name);
    }

    public Map<String, PreBuiltAnalyzerProviderFactory> analyzerProviderFactories() {
        return analyzerProviderFactories;
    }

    public PreBuiltAnalyzerProviderFactory analyzerProviderFactory(String name) {
        return analyzerProviderFactories.get(name);
    }

    public boolean hasAnalyzer(String name) {
        return analyzerProviderFactories.containsKey(name);
    }

    public Analyzer analyzer(String name) {
        PreBuiltAnalyzerProviderFactory analyzerProviderFactory = analyzerProviderFactory(name);
        if (analyzerProviderFactory == null) {
            return null;
        }
        return analyzerProviderFactory.analyzer();
    }

    public void close() {
        for (PreBuiltAnalyzerProviderFactory analyzerProviderFactory : analyzerProviderFactories.values()) {
            try {
                analyzerProviderFactory.analyzer().close();
            } catch (Exception e) {
                // ignore
            }
        }
    }
}
