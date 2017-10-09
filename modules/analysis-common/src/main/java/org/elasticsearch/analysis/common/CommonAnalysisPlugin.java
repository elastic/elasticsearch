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

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ar.ArabicNormalizationFilter;
import org.apache.lucene.analysis.ar.ArabicStemFilter;
import org.apache.lucene.analysis.bn.BengaliNormalizationFilter;
import org.apache.lucene.analysis.br.BrazilianStemFilter;
import org.apache.lucene.analysis.charfilter.HTMLStripCharFilter;
import org.apache.lucene.analysis.cjk.CJKBigramFilter;
import org.apache.lucene.analysis.cjk.CJKWidthFilter;
import org.apache.lucene.analysis.ckb.SoraniNormalizationFilter;
import org.apache.lucene.analysis.commongrams.CommonGramsFilter;
import org.apache.lucene.analysis.core.DecimalDigitFilter;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.core.LowerCaseTokenizer;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.UpperCaseFilter;
import org.apache.lucene.analysis.cz.CzechStemFilter;
import org.apache.lucene.analysis.de.GermanNormalizationFilter;
import org.apache.lucene.analysis.de.GermanStemFilter;
import org.apache.lucene.analysis.en.KStemFilter;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.fa.PersianNormalizationFilter;
import org.apache.lucene.analysis.fr.FrenchAnalyzer;
import org.apache.lucene.analysis.hi.HindiNormalizationFilter;
import org.apache.lucene.analysis.in.IndicNormalizationFilter;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;
import org.apache.lucene.analysis.miscellaneous.DisableGraphAttribute;
import org.apache.lucene.analysis.miscellaneous.KeywordRepeatFilter;
import org.apache.lucene.analysis.miscellaneous.LengthFilter;
import org.apache.lucene.analysis.miscellaneous.LimitTokenCountFilter;
import org.apache.lucene.analysis.miscellaneous.ScandinavianFoldingFilter;
import org.apache.lucene.analysis.miscellaneous.ScandinavianNormalizationFilter;
import org.apache.lucene.analysis.miscellaneous.TrimFilter;
import org.apache.lucene.analysis.miscellaneous.TruncateTokenFilter;
import org.apache.lucene.analysis.miscellaneous.WordDelimiterFilter;
import org.apache.lucene.analysis.miscellaneous.WordDelimiterGraphFilter;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenFilter;
import org.apache.lucene.analysis.ngram.NGramTokenFilter;
import org.apache.lucene.analysis.payloads.DelimitedPayloadTokenFilter;
import org.apache.lucene.analysis.payloads.TypeAsPayloadTokenFilter;
import org.apache.lucene.analysis.reverse.ReverseStringFilter;
import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.apache.lucene.analysis.snowball.SnowballFilter;
import org.apache.lucene.analysis.standard.ClassicFilter;
import org.apache.lucene.analysis.tr.ApostropheFilter;
import org.apache.lucene.analysis.util.ElisionFilter;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.index.analysis.PreConfiguredCharFilter;
import org.elasticsearch.index.analysis.PreConfiguredTokenFilter;
import org.elasticsearch.index.analysis.PreConfiguredTokenizer;
import org.elasticsearch.index.analysis.SoraniNormalizationFilterFactory;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.indices.analysis.AnalysisModule.AnalysisProvider;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.plugins.Plugin;
import org.tartarus.snowball.ext.DutchStemmer;
import org.tartarus.snowball.ext.FrenchStemmer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.elasticsearch.plugins.AnalysisPlugin.requriesAnalysisSettings;

public class CommonAnalysisPlugin extends Plugin implements AnalysisPlugin {
    @Override
    public Map<String, AnalysisProvider<TokenFilterFactory>> getTokenFilters() {
        Map<String, AnalysisProvider<TokenFilterFactory>> filters = new TreeMap<>();
        filters.put("apostrophe", ApostropheFilterFactory::new);
        filters.put("arabic_normalization", ArabicNormalizationFilterFactory::new);
        filters.put("arabic_stem", ArabicStemTokenFilterFactory::new);
        filters.put("asciifolding", ASCIIFoldingTokenFilterFactory::new);
        filters.put("bengali_normalization", BengaliNormalizationFilterFactory::new);
        filters.put("brazilian_stem", BrazilianStemTokenFilterFactory::new);
        filters.put("cjk_bigram", CJKBigramFilterFactory::new);
        filters.put("cjk_width", CJKWidthFilterFactory::new);
        filters.put("classic", ClassicFilterFactory::new);
        filters.put("czech_stem", CzechStemTokenFilterFactory::new);
        filters.put("common_grams", requriesAnalysisSettings(CommonGramsTokenFilterFactory::new));
        filters.put("decimal_digit", DecimalDigitFilterFactory::new);
        filters.put("delimited_payload_filter", DelimitedPayloadTokenFilterFactory::new);
        filters.put("dictionary_decompounder", requriesAnalysisSettings(DictionaryCompoundWordTokenFilterFactory::new));
        filters.put("dutch_stem", DutchStemTokenFilterFactory::new);
        filters.put("edge_ngram", EdgeNGramTokenFilterFactory::new);
        filters.put("edgeNGram", EdgeNGramTokenFilterFactory::new);
        filters.put("elision", ElisionTokenFilterFactory::new);
        filters.put("fingerprint", FingerprintTokenFilterFactory::new);
        filters.put("flatten_graph", FlattenGraphTokenFilterFactory::new);
        filters.put("french_stem", FrenchStemTokenFilterFactory::new);
        filters.put("german_normalization", GermanNormalizationFilterFactory::new);
        filters.put("german_stem", GermanStemTokenFilterFactory::new);
        filters.put("hindi_normalization", HindiNormalizationFilterFactory::new);
        filters.put("hyphenation_decompounder", requriesAnalysisSettings(HyphenationCompoundWordTokenFilterFactory::new));
        filters.put("indic_normalization", IndicNormalizationFilterFactory::new);
        filters.put("keep", requriesAnalysisSettings(KeepWordFilterFactory::new));
        filters.put("keep_types", requriesAnalysisSettings(KeepTypesFilterFactory::new));
        filters.put("keyword_marker", requriesAnalysisSettings(KeywordMarkerTokenFilterFactory::new));
        filters.put("kstem", KStemTokenFilterFactory::new);
        filters.put("length", LengthTokenFilterFactory::new);
        filters.put("limit", LimitTokenCountFilterFactory::new);
        filters.put("lowercase", LowerCaseTokenFilterFactory::new);
        filters.put("min_hash", MinHashTokenFilterFactory::new);
        filters.put("ngram", NGramTokenFilterFactory::new);
        filters.put("nGram", NGramTokenFilterFactory::new);
        filters.put("pattern_capture", requriesAnalysisSettings(PatternCaptureGroupTokenFilterFactory::new));
        filters.put("pattern_replace", requriesAnalysisSettings(PatternReplaceTokenFilterFactory::new));
        filters.put("persian_normalization", PersianNormalizationFilterFactory::new);
        filters.put("porter_stem", PorterStemTokenFilterFactory::new);
        filters.put("reverse", ReverseTokenFilterFactory::new);
        filters.put("russian_stem", RussianStemTokenFilterFactory::new);
        filters.put("scandinavian_folding", ScandinavianFoldingFilterFactory::new);
        filters.put("scandinavian_normalization", ScandinavianNormalizationFilterFactory::new);
        filters.put("serbian_normalization", SerbianNormalizationFilterFactory::new);
        filters.put("snowball", SnowballTokenFilterFactory::new);
        filters.put("sorani_normalization", SoraniNormalizationFilterFactory::new);
        filters.put("stemmer_override", requriesAnalysisSettings(StemmerOverrideTokenFilterFactory::new));
        filters.put("stemmer", StemmerTokenFilterFactory::new);
        filters.put("trim", TrimTokenFilterFactory::new);
        filters.put("truncate", requriesAnalysisSettings(TruncateTokenFilterFactory::new));
        filters.put("unique", UniqueTokenFilterFactory::new);
        filters.put("uppercase", UpperCaseTokenFilterFactory::new);
        filters.put("word_delimiter_graph", WordDelimiterGraphTokenFilterFactory::new);
        filters.put("word_delimiter", WordDelimiterTokenFilterFactory::new);
        return filters;
    }

    @Override
    public Map<String, AnalysisProvider<CharFilterFactory>> getCharFilters() {
        Map<String, AnalysisProvider<CharFilterFactory>> filters = new TreeMap<>();
        filters.put("html_strip", HtmlStripCharFilterFactory::new);
        filters.put("pattern_replace", requriesAnalysisSettings(PatternReplaceCharFilterFactory::new));
        filters.put("mapping", requriesAnalysisSettings(MappingCharFilterFactory::new));
        return filters;
    }

    @Override
    public Map<String, AnalysisProvider<TokenizerFactory>> getTokenizers() {
        Map<String, AnalysisProvider<TokenizerFactory>> tokenizers = new TreeMap<>();
        tokenizers.put("simple_pattern", SimplePatternTokenizerFactory::new);
        tokenizers.put("simple_pattern_split", SimplePatternSplitTokenizerFactory::new);
        return tokenizers;
    }

    @Override
    public List<PreConfiguredCharFilter> getPreConfiguredCharFilters() {
        List<PreConfiguredCharFilter> filters = new ArrayList<>();
        filters.add(PreConfiguredCharFilter.singleton("html_strip", false, HTMLStripCharFilter::new));
        // TODO deprecate htmlStrip
        filters.add(PreConfiguredCharFilter.singleton("htmlStrip", false, HTMLStripCharFilter::new));
        return filters;
    }

    @Override
    public List<PreConfiguredTokenFilter> getPreConfiguredTokenFilters() {
        List<PreConfiguredTokenFilter> filters = new ArrayList<>();
        filters.add(PreConfiguredTokenFilter.singleton("apostrophe", false, ApostropheFilter::new));
        filters.add(PreConfiguredTokenFilter.singleton("arabic_normalization", true, ArabicNormalizationFilter::new));
        filters.add(PreConfiguredTokenFilter.singleton("arabic_stem", false, ArabicStemFilter::new));
        filters.add(PreConfiguredTokenFilter.singleton("asciifolding", true, ASCIIFoldingFilter::new));
        filters.add(PreConfiguredTokenFilter.singleton("bengali_normalization", true, BengaliNormalizationFilter::new));
        filters.add(PreConfiguredTokenFilter.singleton("brazilian_stem", false, BrazilianStemFilter::new));
        filters.add(PreConfiguredTokenFilter.singleton("cjk_bigram", false, CJKBigramFilter::new));
        filters.add(PreConfiguredTokenFilter.singleton("cjk_width", true, CJKWidthFilter::new));
        filters.add(PreConfiguredTokenFilter.singleton("classic", false, ClassicFilter::new));
        filters.add(PreConfiguredTokenFilter.singleton("common_grams", false,
                input -> new CommonGramsFilter(input, CharArraySet.EMPTY_SET)));
        filters.add(PreConfiguredTokenFilter.singleton("czech_stem", false, CzechStemFilter::new));
        filters.add(PreConfiguredTokenFilter.singleton("decimal_digit", true, DecimalDigitFilter::new));
        filters.add(PreConfiguredTokenFilter.singleton("delimited_payload_filter", false, input ->
                new DelimitedPayloadTokenFilter(input,
                        DelimitedPayloadTokenFilterFactory.DEFAULT_DELIMITER,
                        DelimitedPayloadTokenFilterFactory.DEFAULT_ENCODER)));
        filters.add(PreConfiguredTokenFilter.singleton("dutch_stem", false, input -> new SnowballFilter(input, new DutchStemmer())));
        filters.add(PreConfiguredTokenFilter.singleton("edge_ngram", false, input ->
                new EdgeNGramTokenFilter(input, EdgeNGramTokenFilter.DEFAULT_MIN_GRAM_SIZE, EdgeNGramTokenFilter.DEFAULT_MAX_GRAM_SIZE)));
        // TODO deprecate edgeNGram
        filters.add(PreConfiguredTokenFilter.singleton("edgeNGram", false, input ->
                new EdgeNGramTokenFilter(input, EdgeNGramTokenFilter.DEFAULT_MIN_GRAM_SIZE, EdgeNGramTokenFilter.DEFAULT_MAX_GRAM_SIZE)));
        filters.add(PreConfiguredTokenFilter.singleton("elision", true,
                input -> new ElisionFilter(input, FrenchAnalyzer.DEFAULT_ARTICLES)));
        filters.add(PreConfiguredTokenFilter.singleton("french_stem", false, input -> new SnowballFilter(input, new FrenchStemmer())));
        filters.add(PreConfiguredTokenFilter.singleton("german_normalization", true, GermanNormalizationFilter::new));
        filters.add(PreConfiguredTokenFilter.singleton("german_stem", false, GermanStemFilter::new));
        filters.add(PreConfiguredTokenFilter.singleton("hindi_normalization", true, HindiNormalizationFilter::new));
        filters.add(PreConfiguredTokenFilter.singleton("indic_normalization", true, IndicNormalizationFilter::new));
        filters.add(PreConfiguredTokenFilter.singleton("keyword_repeat", false, KeywordRepeatFilter::new));
        filters.add(PreConfiguredTokenFilter.singleton("kstem", false, KStemFilter::new));
        filters.add(PreConfiguredTokenFilter.singleton("length", false, input ->
                new LengthFilter(input, 0, Integer.MAX_VALUE)));  // TODO this one seems useless
        filters.add(PreConfiguredTokenFilter.singleton("limit", false, input ->
                new LimitTokenCountFilter(input,
                        LimitTokenCountFilterFactory.DEFAULT_MAX_TOKEN_COUNT,
                        LimitTokenCountFilterFactory.DEFAULT_CONSUME_ALL_TOKENS)));
        filters.add(PreConfiguredTokenFilter.singleton("ngram", false, NGramTokenFilter::new));
        // TODO deprecate nGram
        filters.add(PreConfiguredTokenFilter.singleton("nGram", false, NGramTokenFilter::new));
        filters.add(PreConfiguredTokenFilter.singleton("persian_normalization", true, PersianNormalizationFilter::new));
        filters.add(PreConfiguredTokenFilter.singleton("porter_stem", false, PorterStemFilter::new));
        filters.add(PreConfiguredTokenFilter.singleton("reverse", false, ReverseStringFilter::new));
        filters.add(PreConfiguredTokenFilter.singleton("russian_stem", false, input -> new SnowballFilter(input, "Russian")));
        filters.add(PreConfiguredTokenFilter.singleton("scandinavian_folding", true, ScandinavianFoldingFilter::new));
        filters.add(PreConfiguredTokenFilter.singleton("scandinavian_normalization", true, ScandinavianNormalizationFilter::new));
        filters.add(PreConfiguredTokenFilter.singleton("shingle", false, input -> {
            TokenStream ts = new ShingleFilter(input);
            /**
             * We disable the graph analysis on this token stream
             * because it produces shingles of different size.
             * Graph analysis on such token stream is useless and dangerous as it may create too many paths
             * since shingles of different size are not aligned in terms of positions.
             */
            ts.addAttribute(DisableGraphAttribute.class);
            return ts;
        }));
        filters.add(PreConfiguredTokenFilter.singleton("snowball", false, input -> new SnowballFilter(input, "English")));
        filters.add(PreConfiguredTokenFilter.singleton("sorani_normalization", true, SoraniNormalizationFilter::new));
        filters.add(PreConfiguredTokenFilter.singleton("stemmer", false, PorterStemFilter::new));
        // The stop filter is in lucene-core but the English stop words set is in lucene-analyzers-common
        filters.add(PreConfiguredTokenFilter.singleton("stop", false, input -> new StopFilter(input, StopAnalyzer.ENGLISH_STOP_WORDS_SET)));
        filters.add(PreConfiguredTokenFilter.singleton("trim", false, TrimFilter::new));
        filters.add(PreConfiguredTokenFilter.singleton("truncate", false, input -> new TruncateTokenFilter(input, 10)));
        filters.add(PreConfiguredTokenFilter.singleton("type_as_payload", false, TypeAsPayloadTokenFilter::new));
        filters.add(PreConfiguredTokenFilter.singleton("unique", false, UniqueTokenFilter::new));
        filters.add(PreConfiguredTokenFilter.singleton("uppercase", true, UpperCaseFilter::new));
        filters.add(PreConfiguredTokenFilter.singleton("word_delimiter", false, input ->
                new WordDelimiterFilter(input,
                        WordDelimiterFilter.GENERATE_WORD_PARTS
                      | WordDelimiterFilter.GENERATE_NUMBER_PARTS
                      | WordDelimiterFilter.SPLIT_ON_CASE_CHANGE
                      | WordDelimiterFilter.SPLIT_ON_NUMERICS
                      | WordDelimiterFilter.STEM_ENGLISH_POSSESSIVE, null)));
        filters.add(PreConfiguredTokenFilter.singleton("word_delimiter_graph", false, input ->
                new WordDelimiterGraphFilter(input,
                        WordDelimiterGraphFilter.GENERATE_WORD_PARTS
                      | WordDelimiterGraphFilter.GENERATE_NUMBER_PARTS
                      | WordDelimiterGraphFilter.SPLIT_ON_CASE_CHANGE
                      | WordDelimiterGraphFilter.SPLIT_ON_NUMERICS
                      | WordDelimiterGraphFilter.STEM_ENGLISH_POSSESSIVE, null)));
        return filters;
    }

    @Override
    public List<PreConfiguredTokenizer> getPreConfiguredTokenizers() {
        List<PreConfiguredTokenizer> tokenizers = new ArrayList<>();
        tokenizers.add(PreConfiguredTokenizer.singleton("keyword", KeywordTokenizer::new, null));
        tokenizers.add(PreConfiguredTokenizer.singleton("lowercase", LowerCaseTokenizer::new, () -> new TokenFilterFactory() {
            @Override
            public String name() {
                return "lowercase";
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                return new LowerCaseFilter(tokenStream);
            }
        }));
        return tokenizers;
    }
}
