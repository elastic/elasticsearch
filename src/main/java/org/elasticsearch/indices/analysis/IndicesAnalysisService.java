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

import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.ar.ArabicAnalyzer;
import org.apache.lucene.analysis.ar.ArabicStemFilter;
import org.apache.lucene.analysis.bg.BulgarianAnalyzer;
import org.apache.lucene.analysis.br.BrazilianAnalyzer;
import org.apache.lucene.analysis.br.BrazilianStemFilter;
import org.apache.lucene.analysis.ca.CatalanAnalyzer;
import org.apache.lucene.analysis.cjk.CJKAnalyzer;
import org.apache.lucene.analysis.cn.ChineseAnalyzer;
import org.apache.lucene.analysis.cz.CzechAnalyzer;
import org.apache.lucene.analysis.cz.CzechStemFilter;
import org.apache.lucene.analysis.da.DanishAnalyzer;
import org.apache.lucene.analysis.de.GermanAnalyzer;
import org.apache.lucene.analysis.de.GermanStemFilter;
import org.apache.lucene.analysis.el.GreekAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.en.KStemFilter;
import org.apache.lucene.analysis.es.SpanishAnalyzer;
import org.apache.lucene.analysis.eu.BasqueAnalyzer;
import org.apache.lucene.analysis.fa.PersianAnalyzer;
import org.apache.lucene.analysis.fi.FinnishAnalyzer;
import org.apache.lucene.analysis.fr.ElisionFilter;
import org.apache.lucene.analysis.fr.FrenchAnalyzer;
import org.apache.lucene.analysis.fr.FrenchStemFilter;
import org.apache.lucene.analysis.gl.GalicianAnalyzer;
import org.apache.lucene.analysis.hi.HindiAnalyzer;
import org.apache.lucene.analysis.hu.HungarianAnalyzer;
import org.apache.lucene.analysis.hy.ArmenianAnalyzer;
import org.apache.lucene.analysis.id.IndonesianAnalyzer;
import org.apache.lucene.analysis.it.ItalianAnalyzer;
import org.apache.lucene.analysis.miscellaneous.*;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenFilter;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenizer;
import org.apache.lucene.analysis.ngram.NGramTokenFilter;
import org.apache.lucene.analysis.ngram.NGramTokenizer;
import org.apache.lucene.analysis.nl.DutchAnalyzer;
import org.apache.lucene.analysis.nl.DutchStemFilter;
import org.apache.lucene.analysis.no.NorwegianAnalyzer;
import org.apache.lucene.analysis.path.PathHierarchyTokenizer;
import org.apache.lucene.analysis.pattern.PatternTokenizer;
import org.apache.lucene.analysis.pt.PortugueseAnalyzer;
import org.apache.lucene.analysis.reverse.ReverseStringFilter;
import org.apache.lucene.analysis.ro.RomanianAnalyzer;
import org.apache.lucene.analysis.ru.RussianAnalyzer;
import org.apache.lucene.analysis.ru.RussianStemFilter;
import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.apache.lucene.analysis.snowball.SnowballAnalyzer;
import org.apache.lucene.analysis.snowball.SnowballFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.standard.UAX29URLEmailTokenizer;
import org.apache.lucene.analysis.sv.SwedishAnalyzer;
import org.apache.lucene.analysis.th.ThaiAnalyzer;
import org.apache.lucene.analysis.tr.TurkishAnalyzer;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.analysis.HTMLStripCharFilter;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.analysis.*;

import java.io.IOException;
import java.io.Reader;
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

        StandardAnalyzer standardAnalyzer = new StandardAnalyzer(Lucene.ANALYZER_VERSION);
        analyzerProviderFactories.put("default", new PreBuiltAnalyzerProviderFactory("default", AnalyzerScope.INDICES, standardAnalyzer));
        analyzerProviderFactories.put("standard", new PreBuiltAnalyzerProviderFactory("standard", AnalyzerScope.INDICES, standardAnalyzer));
        analyzerProviderFactories.put("keyword", new PreBuiltAnalyzerProviderFactory("keyword", AnalyzerScope.INDICES, new KeywordAnalyzer()));
        analyzerProviderFactories.put("stop", new PreBuiltAnalyzerProviderFactory("stop", AnalyzerScope.INDICES, new StopAnalyzer(Lucene.ANALYZER_VERSION)));
        analyzerProviderFactories.put("whitespace", new PreBuiltAnalyzerProviderFactory("whitespace", AnalyzerScope.INDICES, new WhitespaceAnalyzer(Lucene.ANALYZER_VERSION)));
        analyzerProviderFactories.put("simple", new PreBuiltAnalyzerProviderFactory("simple", AnalyzerScope.INDICES, new SimpleAnalyzer(Lucene.ANALYZER_VERSION)));

        // extended ones
        analyzerProviderFactories.put("pattern", new PreBuiltAnalyzerProviderFactory("pattern", AnalyzerScope.INDICES, new PatternAnalyzer(Lucene.ANALYZER_VERSION, Regex.compile("\\W+" /*PatternAnalyzer.NON_WORD_PATTERN*/, null), true, StopAnalyzer.ENGLISH_STOP_WORDS_SET)));
        analyzerProviderFactories.put("snowball", new PreBuiltAnalyzerProviderFactory("snowball", AnalyzerScope.INDICES, new SnowballAnalyzer(Lucene.ANALYZER_VERSION, "English", StopAnalyzer.ENGLISH_STOP_WORDS_SET)));
        analyzerProviderFactories.put("standard_html_strip", new PreBuiltAnalyzerProviderFactory("standard_html_strip", AnalyzerScope.INDICES, new StandardHtmlStripAnalyzer(Lucene.ANALYZER_VERSION)));

        analyzerProviderFactories.put("arabic", new PreBuiltAnalyzerProviderFactory("arabic", AnalyzerScope.INDICES, new ArabicAnalyzer(Lucene.ANALYZER_VERSION)));
        analyzerProviderFactories.put("armenian", new PreBuiltAnalyzerProviderFactory("armenian", AnalyzerScope.INDICES, new ArmenianAnalyzer(Lucene.ANALYZER_VERSION)));
        analyzerProviderFactories.put("basque", new PreBuiltAnalyzerProviderFactory("basque", AnalyzerScope.INDICES, new BasqueAnalyzer(Lucene.ANALYZER_VERSION)));
        analyzerProviderFactories.put("brazilian", new PreBuiltAnalyzerProviderFactory("brazilian", AnalyzerScope.INDICES, new BrazilianAnalyzer(Lucene.ANALYZER_VERSION)));
        analyzerProviderFactories.put("bulgarian", new PreBuiltAnalyzerProviderFactory("bulgarian", AnalyzerScope.INDICES, new BulgarianAnalyzer(Lucene.ANALYZER_VERSION)));
        analyzerProviderFactories.put("catalan", new PreBuiltAnalyzerProviderFactory("catalan", AnalyzerScope.INDICES, new CatalanAnalyzer(Lucene.ANALYZER_VERSION)));
        analyzerProviderFactories.put("chinese", new PreBuiltAnalyzerProviderFactory("chinese", AnalyzerScope.INDICES, new ChineseAnalyzer()));
        analyzerProviderFactories.put("cjk", new PreBuiltAnalyzerProviderFactory("cjk", AnalyzerScope.INDICES, new CJKAnalyzer(Lucene.ANALYZER_VERSION)));
        analyzerProviderFactories.put("czech", new PreBuiltAnalyzerProviderFactory("czech", AnalyzerScope.INDICES, new CzechAnalyzer(Lucene.ANALYZER_VERSION)));
        analyzerProviderFactories.put("dutch", new PreBuiltAnalyzerProviderFactory("dutch", AnalyzerScope.INDICES, new DutchAnalyzer(Lucene.ANALYZER_VERSION)));
        analyzerProviderFactories.put("danish", new PreBuiltAnalyzerProviderFactory("danish", AnalyzerScope.INDICES, new DanishAnalyzer(Lucene.ANALYZER_VERSION)));
        analyzerProviderFactories.put("english", new PreBuiltAnalyzerProviderFactory("english", AnalyzerScope.INDICES, new EnglishAnalyzer(Lucene.ANALYZER_VERSION)));
        analyzerProviderFactories.put("finnish", new PreBuiltAnalyzerProviderFactory("finnish", AnalyzerScope.INDICES, new FinnishAnalyzer(Lucene.ANALYZER_VERSION)));
        analyzerProviderFactories.put("french", new PreBuiltAnalyzerProviderFactory("french", AnalyzerScope.INDICES, new FrenchAnalyzer(Lucene.ANALYZER_VERSION)));
        analyzerProviderFactories.put("galician", new PreBuiltAnalyzerProviderFactory("galician", AnalyzerScope.INDICES, new GalicianAnalyzer(Lucene.ANALYZER_VERSION)));
        analyzerProviderFactories.put("german", new PreBuiltAnalyzerProviderFactory("german", AnalyzerScope.INDICES, new GermanAnalyzer(Lucene.ANALYZER_VERSION)));
        analyzerProviderFactories.put("greek", new PreBuiltAnalyzerProviderFactory("greek", AnalyzerScope.INDICES, new GreekAnalyzer(Lucene.ANALYZER_VERSION)));
        analyzerProviderFactories.put("hindi", new PreBuiltAnalyzerProviderFactory("hindi", AnalyzerScope.INDICES, new HindiAnalyzer(Lucene.ANALYZER_VERSION)));
        analyzerProviderFactories.put("hungarian", new PreBuiltAnalyzerProviderFactory("hungarian", AnalyzerScope.INDICES, new HungarianAnalyzer(Lucene.ANALYZER_VERSION)));
        analyzerProviderFactories.put("indonesian", new PreBuiltAnalyzerProviderFactory("indonesian", AnalyzerScope.INDICES, new IndonesianAnalyzer(Lucene.ANALYZER_VERSION)));
        analyzerProviderFactories.put("italian", new PreBuiltAnalyzerProviderFactory("italian", AnalyzerScope.INDICES, new ItalianAnalyzer(Lucene.ANALYZER_VERSION)));
        analyzerProviderFactories.put("norwegian", new PreBuiltAnalyzerProviderFactory("norwegian", AnalyzerScope.INDICES, new NorwegianAnalyzer(Lucene.ANALYZER_VERSION)));
        analyzerProviderFactories.put("persian", new PreBuiltAnalyzerProviderFactory("persian", AnalyzerScope.INDICES, new PersianAnalyzer(Lucene.ANALYZER_VERSION)));
        analyzerProviderFactories.put("portuguese", new PreBuiltAnalyzerProviderFactory("portuguese", AnalyzerScope.INDICES, new PortugueseAnalyzer(Lucene.ANALYZER_VERSION)));
        analyzerProviderFactories.put("romanian", new PreBuiltAnalyzerProviderFactory("romanian", AnalyzerScope.INDICES, new RomanianAnalyzer(Lucene.ANALYZER_VERSION)));
        analyzerProviderFactories.put("russian", new PreBuiltAnalyzerProviderFactory("russian", AnalyzerScope.INDICES, new RussianAnalyzer(Lucene.ANALYZER_VERSION)));
        analyzerProviderFactories.put("spanish", new PreBuiltAnalyzerProviderFactory("spanish", AnalyzerScope.INDICES, new SpanishAnalyzer(Lucene.ANALYZER_VERSION)));
        analyzerProviderFactories.put("swedish", new PreBuiltAnalyzerProviderFactory("swedish", AnalyzerScope.INDICES, new SwedishAnalyzer(Lucene.ANALYZER_VERSION)));
        analyzerProviderFactories.put("turkish", new PreBuiltAnalyzerProviderFactory("turkish", AnalyzerScope.INDICES, new TurkishAnalyzer(Lucene.ANALYZER_VERSION)));
        analyzerProviderFactories.put("thai", new PreBuiltAnalyzerProviderFactory("thai", AnalyzerScope.INDICES, new ThaiAnalyzer(Lucene.ANALYZER_VERSION)));

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

        tokenizerFactories.put("uax_url_email", new PreBuiltTokenizerFactoryFactory(new TokenizerFactory() {
            @Override
            public String name() {
                return "uax_url_email";
            }

            @Override
            public Tokenizer create(Reader reader) {
                return new UAX29URLEmailTokenizer(reader);
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
                return new NGramTokenizer(reader);
            }
        }));

        tokenizerFactories.put("ngram", new PreBuiltTokenizerFactoryFactory(new TokenizerFactory() {
            @Override
            public String name() {
                return "ngram";
            }

            @Override
            public Tokenizer create(Reader reader) {
                return new NGramTokenizer(reader);
            }
        }));

        tokenizerFactories.put("edgeNGram", new PreBuiltTokenizerFactoryFactory(new TokenizerFactory() {
            @Override
            public String name() {
                return "edgeNGram";
            }

            @Override
            public Tokenizer create(Reader reader) {
                return new EdgeNGramTokenizer(reader, EdgeNGramTokenizer.DEFAULT_SIDE, EdgeNGramTokenizer.DEFAULT_MIN_GRAM_SIZE, EdgeNGramTokenizer.DEFAULT_MAX_GRAM_SIZE);
            }
        }));

        tokenizerFactories.put("edge_ngram", new PreBuiltTokenizerFactoryFactory(new TokenizerFactory() {
            @Override
            public String name() {
                return "edge_ngram";
            }

            @Override
            public Tokenizer create(Reader reader) {
                return new EdgeNGramTokenizer(reader, EdgeNGramTokenizer.DEFAULT_SIDE, EdgeNGramTokenizer.DEFAULT_MIN_GRAM_SIZE, EdgeNGramTokenizer.DEFAULT_MAX_GRAM_SIZE);
            }
        }));

        tokenizerFactories.put("pattern", new PreBuiltTokenizerFactoryFactory(new TokenizerFactory() {
            @Override
            public String name() {
                return "pattern";
            }

            @Override
            public Tokenizer create(Reader reader) {
                try {
                    return new PatternTokenizer(reader, Regex.compile("\\W+", null), -1);
                } catch (IOException e) {
                    throw new ElasticSearchIllegalStateException("failed to parse default pattern");
                }
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
                return new WordDelimiterFilter(tokenStream, WordDelimiterIterator.DEFAULT_WORD_DELIM_TABLE,
                        1, 1, 0, 0, 0, 1, 0, 1, 1, null);
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
                return new TrimFilter(tokenStream, false);
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
                return new LengthFilter(true, tokenStream, 0, Integer.MAX_VALUE);
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

        tokenFilterFactories.put("nGram", new PreBuiltTokenFilterFactoryFactory(new TokenFilterFactory() {
            @Override
            public String name() {
                return "nGram";
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                return new NGramTokenFilter(tokenStream);
            }
        }));

        tokenFilterFactories.put("ngram", new PreBuiltTokenFilterFactoryFactory(new TokenFilterFactory() {
            @Override
            public String name() {
                return "ngram";
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                return new NGramTokenFilter(tokenStream);
            }
        }));

        tokenFilterFactories.put("edgeNGram", new PreBuiltTokenFilterFactoryFactory(new TokenFilterFactory() {
            @Override
            public String name() {
                return "edgeNGram";
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                return new EdgeNGramTokenFilter(tokenStream, EdgeNGramTokenFilter.DEFAULT_SIDE, EdgeNGramTokenFilter.DEFAULT_MIN_GRAM_SIZE, EdgeNGramTokenFilter.DEFAULT_MAX_GRAM_SIZE);
            }
        }));

        tokenFilterFactories.put("edge_ngram", new PreBuiltTokenFilterFactoryFactory(new TokenFilterFactory() {
            @Override
            public String name() {
                return "edge_ngram";
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                return new EdgeNGramTokenFilter(tokenStream, EdgeNGramTokenFilter.DEFAULT_SIDE, EdgeNGramTokenFilter.DEFAULT_MIN_GRAM_SIZE, EdgeNGramTokenFilter.DEFAULT_MAX_GRAM_SIZE);
            }
        }));

        tokenFilterFactories.put("shingle", new PreBuiltTokenFilterFactoryFactory(new TokenFilterFactory() {
            @Override
            public String name() {
                return "shingle";
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                return new ShingleFilter(tokenStream, ShingleFilter.DEFAULT_MAX_SHINGLE_SIZE);
            }
        }));

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
                return new ElisionFilter(Lucene.ANALYZER_VERSION, tokenStream);
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
                return new RussianStemFilter(tokenStream);
            }
        }));

        // Char Filter
        charFilterFactories.put("html_strip", new PreBuiltCharFilterFactoryFactory(new CharFilterFactory() {
            @Override
            public String name() {
                return "html_strip";
            }

            @Override
            public CharStream create(CharStream tokenStream) {
                return new HTMLStripCharFilter(tokenStream);
            }
        }));

        charFilterFactories.put("htmlStrip", new PreBuiltCharFilterFactoryFactory(new CharFilterFactory() {
            @Override
            public String name() {
                return "htmlStrip";
            }

            @Override
            public CharStream create(CharStream tokenStream) {
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
        return analyzer(name) != null;
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
