/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.charfilter.HTMLStripCharFilterFactory;
import org.apache.lucene.analysis.en.PorterStemFilterFactory;
import org.apache.lucene.analysis.miscellaneous.LimitTokenCountFilterFactory;
import org.apache.lucene.analysis.reverse.ReverseStringFilterFactory;
import org.apache.lucene.analysis.snowball.SnowballPorterFilterFactory;
import org.apache.lucene.analysis.te.TeluguNormalizationFilterFactory;
import org.apache.lucene.analysis.te.TeluguStemFilterFactory;
import org.elasticsearch.indices.analysis.AnalysisFactoryTestCase;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

public class CommonAnalysisFactoryTests extends AnalysisFactoryTestCase {
    public CommonAnalysisFactoryTests() {
        super(new CommonAnalysisPlugin());
    }

    @Override
    protected Map<String, Class<?>> getTokenizers() {
        Map<String, Class<?>> tokenizers = new TreeMap<>(super.getTokenizers());
        tokenizers.put("simplepattern", SimplePatternTokenizerFactory.class);
        tokenizers.put("simplepatternsplit", SimplePatternSplitTokenizerFactory.class);
        tokenizers.put("thai", ThaiTokenizerFactory.class);
        tokenizers.put("ngram", NGramTokenizerFactory.class);
        tokenizers.put("edgengram", EdgeNGramTokenizerFactory.class);
        tokenizers.put("classic", ClassicTokenizerFactory.class);
        tokenizers.put("letter", LetterTokenizerFactory.class);
        // tokenizers.put("lowercase", XLowerCaseTokenizerFactory.class);
        tokenizers.put("pathhierarchy", PathHierarchyTokenizerFactory.class);
        tokenizers.put("pattern", PatternTokenizerFactory.class);
        tokenizers.put("uax29urlemail", UAX29URLEmailTokenizerFactory.class);
        tokenizers.put("whitespace", WhitespaceTokenizerFactory.class);
        tokenizers.put("keyword", KeywordTokenizerFactory.class);
        return tokenizers;
    }

    @Override
    protected Map<String, Class<?>> getTokenFilters() {
        Map<String, Class<?>> filters = new TreeMap<>(super.getTokenFilters());
        filters.put("asciifolding", ASCIIFoldingTokenFilterFactory.class);
        filters.put("keywordmarker", KeywordMarkerTokenFilterFactory.class);
        filters.put("porterstem", PorterStemTokenFilterFactory.class);
        filters.put("snowballporter", SnowballTokenFilterFactory.class);
        filters.put("trim", TrimTokenFilterFactory.class);
        filters.put("worddelimiter", WordDelimiterTokenFilterFactory.class);
        filters.put("worddelimitergraph", WordDelimiterGraphTokenFilterFactory.class);
        filters.put("flattengraph", FlattenGraphTokenFilterFactory.class);
        filters.put("length", LengthTokenFilterFactory.class);
        filters.put("greeklowercase", LowerCaseTokenFilterFactory.class);
        filters.put("irishlowercase", LowerCaseTokenFilterFactory.class);
        filters.put("lowercase", LowerCaseTokenFilterFactory.class);
        filters.put("turkishlowercase", LowerCaseTokenFilterFactory.class);
        filters.put("uppercase", UpperCaseTokenFilterFactory.class);
        filters.put("ngram", NGramTokenFilterFactory.class);
        filters.put("edgengram", EdgeNGramTokenFilterFactory.class);
        filters.put("bengalistem", StemmerTokenFilterFactory.class);
        filters.put("bulgarianstem", StemmerTokenFilterFactory.class);
        filters.put("englishminimalstem", StemmerTokenFilterFactory.class);
        filters.put("englishpossessive", StemmerTokenFilterFactory.class);
        filters.put("finnishlightstem", StemmerTokenFilterFactory.class);
        filters.put("frenchlightstem", StemmerTokenFilterFactory.class);
        filters.put("frenchminimalstem", StemmerTokenFilterFactory.class);
        filters.put("galicianminimalstem", StemmerTokenFilterFactory.class);
        filters.put("galicianstem", StemmerTokenFilterFactory.class);
        filters.put("germanlightstem", StemmerTokenFilterFactory.class);
        filters.put("germanminimalstem", StemmerTokenFilterFactory.class);
        filters.put("greekstem", StemmerTokenFilterFactory.class);
        filters.put("hindistem", StemmerTokenFilterFactory.class);
        filters.put("hungarianlightstem", StemmerTokenFilterFactory.class);
        filters.put("indonesianstem", StemmerTokenFilterFactory.class);
        filters.put("italianlightstem", StemmerTokenFilterFactory.class);
        filters.put("latvianstem", StemmerTokenFilterFactory.class);
        filters.put("norwegianlightstem", StemmerTokenFilterFactory.class);
        filters.put("norwegianminimalstem", StemmerTokenFilterFactory.class);
        filters.put("norwegiannormalization", Void.class);
        filters.put("portuguesestem", StemmerTokenFilterFactory.class);
        filters.put("portugueselightstem", StemmerTokenFilterFactory.class);
        filters.put("portugueseminimalstem", StemmerTokenFilterFactory.class);
        filters.put("russianlightstem", StemmerTokenFilterFactory.class);
        filters.put("soranistem", StemmerTokenFilterFactory.class);
        filters.put("spanishlightstem", StemmerTokenFilterFactory.class);
        filters.put("swedishlightstem", StemmerTokenFilterFactory.class);
        filters.put("swedishminimalstem", Void.class);
        filters.put("stemmeroverride", StemmerOverrideTokenFilterFactory.class);
        filters.put("telugunormalization", TeluguNormalizationFilterFactory.class);
        filters.put("telugustem", TeluguStemFilterFactory.class);
        filters.put("kstem", KStemTokenFilterFactory.class);
        filters.put("synonym", SynonymTokenFilterFactory.class);
        filters.put("synonymgraph", SynonymGraphTokenFilterFactory.class);
        filters.put("dictionarycompoundword", DictionaryCompoundWordTokenFilterFactory.class);
        filters.put("hyphenationcompoundword", HyphenationCompoundWordTokenFilterFactory.class);
        filters.put("reversestring", ReverseTokenFilterFactory.class);
        filters.put("elision", ElisionTokenFilterFactory.class);
        filters.put("truncate", TruncateTokenFilterFactory.class);
        filters.put("limittokencount", LimitTokenCountFilterFactory.class);
        filters.put("commongrams", CommonGramsTokenFilterFactory.class);
        filters.put("commongramsquery", CommonGramsTokenFilterFactory.class);
        filters.put("patternreplace", PatternReplaceTokenFilterFactory.class);
        filters.put("patterncapturegroup", PatternCaptureGroupTokenFilterFactory.class);
        filters.put("arabicnormalization", ArabicNormalizationFilterFactory.class);
        filters.put("bengalinormalization", BengaliNormalizationFilterFactory.class);
        filters.put("germannormalization", GermanNormalizationFilterFactory.class);
        filters.put("hindinormalization", HindiNormalizationFilterFactory.class);
        filters.put("indicnormalization", IndicNormalizationFilterFactory.class);
        filters.put("persiannormalization", PersianNormalizationFilterFactory.class);
        filters.put("persianstem", PersianStemTokenFilterFactory.class);
        filters.put("scandinaviannormalization", ScandinavianNormalizationFilterFactory.class);
        filters.put("serbiannormalization", SerbianNormalizationFilterFactory.class);
        filters.put("soraninormalization", SoraniNormalizationFilterFactory.class);
        filters.put("cjkwidth", CJKWidthFilterFactory.class);
        filters.put("cjkbigram", CJKBigramFilterFactory.class);
        filters.put("delimitedpayload", DelimitedPayloadTokenFilterFactory.class);
        filters.put("keepword", KeepWordFilterFactory.class);
        filters.put("type", KeepTypesFilterFactory.class);
        filters.put("classic", ClassicFilterFactory.class);
        filters.put("apostrophe", ApostropheFilterFactory.class);
        filters.put("decimaldigit", DecimalDigitFilterFactory.class);
        filters.put("fingerprint", FingerprintTokenFilterFactory.class);
        filters.put("minhash", MinHashTokenFilterFactory.class);
        filters.put("scandinavianfolding", ScandinavianFoldingFilterFactory.class);
        filters.put("arabicstem", ArabicStemTokenFilterFactory.class);
        filters.put("brazilianstem", BrazilianStemTokenFilterFactory.class);
        filters.put("czechstem", CzechStemTokenFilterFactory.class);
        filters.put("germanstem", GermanStemTokenFilterFactory.class);
        // this filter is not exposed and should only be used internally
        filters.put("fixedshingle", Void.class);
        filters.put("word2vecsynonym", Void.class); // not exposed
        return filters;
    }

    @Override
    protected Map<String, Class<?>> getCharFilters() {
        Map<String, Class<?>> filters = new TreeMap<>(super.getCharFilters());
        filters.put("htmlstrip", HtmlStripCharFilterFactory.class);
        filters.put("mapping", MappingCharFilterFactory.class);
        filters.put("patternreplace", PatternReplaceCharFilterFactory.class);

        // TODO: these charfilters are not yet exposed: useful?
        // handling of zwnj for persian
        filters.put("persian", Void.class);
        return filters;
    }

    @Override
    public Map<String, Class<?>> getPreConfiguredCharFilters() {
        Map<String, Class<?>> filters = new TreeMap<>(super.getPreConfiguredCharFilters());
        filters.put("html_strip", HTMLStripCharFilterFactory.class);
        filters.put("htmlStrip", HTMLStripCharFilterFactory.class);
        return filters;
    }

    @Override
    protected Map<String, Class<?>> getPreConfiguredTokenFilters() {
        Map<String, Class<?>> filters = new TreeMap<>(super.getPreConfiguredTokenFilters());
        filters.put("apostrophe", null);
        filters.put("arabic_normalization", null);
        filters.put("arabic_stem", null);
        filters.put("asciifolding", null);
        filters.put("bengali_normalization", null);
        filters.put("brazilian_stem", null);
        filters.put("cjk_bigram", null);
        filters.put("cjk_width", null);
        filters.put("classic", null);
        filters.put("common_grams", null);
        filters.put("czech_stem", null);
        filters.put("decimal_digit", null);
        filters.put("delimited_payload_filter", org.apache.lucene.analysis.payloads.DelimitedPayloadTokenFilterFactory.class);
        filters.put("delimited_payload", org.apache.lucene.analysis.payloads.DelimitedPayloadTokenFilterFactory.class);
        filters.put("dutch_stem", SnowballPorterFilterFactory.class);
        filters.put("edge_ngram", null);
        filters.put("elision", null);
        filters.put("french_stem", SnowballPorterFilterFactory.class);
        filters.put("german_stem", null);
        filters.put("german_normalization", null);
        filters.put("hindi_normalization", null);
        filters.put("indic_normalization", null);
        filters.put("keyword_repeat", null);
        filters.put("kstem", null);
        filters.put("length", null);
        filters.put("limit", LimitTokenCountFilterFactory.class);
        filters.put("ngram", null);
        filters.put("persian_normalization", null);
        filters.put("porter_stem", null);
        filters.put("reverse", ReverseStringFilterFactory.class);
        filters.put("russian_stem", SnowballPorterFilterFactory.class);
        filters.put("scandinavian_normalization", null);
        filters.put("scandinavian_folding", null);
        filters.put("shingle", null);
        filters.put("snowball", SnowballPorterFilterFactory.class);
        filters.put("sorani_normalization", null);
        filters.put("stemmer", PorterStemFilterFactory.class);
        filters.put("stop", null);
        filters.put("trim", null);
        filters.put("truncate", null);
        filters.put("type_as_payload", null);
        filters.put("unique", Void.class);
        filters.put("uppercase", null);
        filters.put("word_delimiter", null);
        filters.put("word_delimiter_graph", null);
        return filters;
    }

    @Override
    protected Map<String, Class<?>> getPreConfiguredTokenizers() {
        Map<String, Class<?>> tokenizers = new TreeMap<>(super.getPreConfiguredTokenizers());
        tokenizers.put("keyword", null);
        tokenizers.put("lowercase", Void.class);
        tokenizers.put("classic", null);
        tokenizers.put("uax_url_email", org.apache.lucene.analysis.email.UAX29URLEmailTokenizerFactory.class);
        tokenizers.put("path_hierarchy", null);
        tokenizers.put("letter", null);
        tokenizers.put("whitespace", null);
        tokenizers.put("ngram", null);
        tokenizers.put("edge_ngram", null);
        tokenizers.put("pattern", null);
        tokenizers.put("thai", null);

        // TODO drop aliases once they are moved to module
        tokenizers.put("nGram", tokenizers.get("ngram"));
        tokenizers.put("edgeNGram", tokenizers.get("edge_ngram"));
        tokenizers.put("PathHierarchy", tokenizers.get("path_hierarchy"));

        return tokenizers;
    }

    /**
     * Fails if a tokenizer is marked in the superclass with {@link MovedToAnalysisCommon} but
     * hasn't been marked in this class with its proper factory.
     */
    public void testAllTokenizersMarked() {
        markedTestCase("char filter", getTokenizers());
    }

    /**
     * Fails if a char filter is marked in the superclass with {@link MovedToAnalysisCommon} but
     * hasn't been marked in this class with its proper factory.
     */
    public void testAllCharFiltersMarked() {
        markedTestCase("char filter", getCharFilters());
    }

    /**
     * Fails if a char filter is marked in the superclass with {@link MovedToAnalysisCommon} but
     * hasn't been marked in this class with its proper factory.
     */
    public void testAllTokenFiltersMarked() {
        markedTestCase("token filter", getTokenFilters());
    }

    private void markedTestCase(String name, Map<String, Class<?>> map) {
        List<String> unmarked = map.entrySet()
            .stream()
            .filter(e -> e.getValue() == MovedToAnalysisCommon.class)
            .map(Map.Entry::getKey)
            .sorted()
            .collect(toList());
        assertEquals(
            name + " marked in AnalysisFactoryTestCase as moved to analysis-common " + "but not mapped here",
            emptyList(),
            unmarked
        );
    }
}
