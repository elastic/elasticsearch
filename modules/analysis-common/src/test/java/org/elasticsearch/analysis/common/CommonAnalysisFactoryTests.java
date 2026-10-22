/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

    /** Language analyzers keyed on their stop words (including {@code stopwords_case}) and stem exclusions. */
    private static final List<String> STOPWORD_AND_STEM_EXCLUSION_ANALYZERS = List.of(
        "arabic",
        "armenian",
        "basque",
        "bengali",
        "brazilian",
        "bulgarian",
        "catalan",
        "czech",
        "danish",
        "dutch",
        "english",
        "estonian",
        "finnish",
        "french",
        "galician",
        "german",
        "hindi",
        "hungarian",
        "indonesian",
        "irish",
        "italian",
        "latvian",
        "lithuanian",
        "norwegian",
        "portuguese",
        "romanian",
        "russian",
        "serbian",
        "sorani",
        "spanish",
        "swedish",
        "turkish"
    );

    @Override
    protected Map<String, FactorySettings> analyzerSettings() {
        Map<String, FactorySettings> analyzers = new HashMap<>();
        for (String language : STOPWORD_AND_STEM_EXCLUSION_ANALYZERS) {
            analyzers.put(
                language,
                settings().affects("stopwords", List.of("foo")).affects("stopwords_case", "true").affects("stem_exclusion", List.of("foo"))
            );
        }
        for (String stopWordsOnly : List.of("cjk", "greek", "thai")) {
            analyzers.put(stopWordsOnly, settings().affects("stopwords", List.of("foo")).affects("stopwords_case", "true"));
        }
        for (String stateless : List.of("chinese", "keyword", "simple", "whitespace")) {
            analyzers.put(stateless, alwaysShares());
        }
        // The legacy pre-Lucene 10 persian chain is also keyed; SharingKeyTests covers that.
        for (String stopWordsOnly : List.of("persian", "stop")) {
            analyzers.put(stopWordsOnly, settings().affects("stopwords", List.of("foo")).affects("stopwords_case", "true"));
        }
        analyzers.put(
            "fingerprint",
            settings().affects("separator", "+")
                .affects("max_output_size", "10")
                .affects("stopwords", List.of("foo"))
                .affects("stopwords_case", "true")
        );
        analyzers.put(
            "pattern",
            settings().affects("pattern", "\\s+")
                .affects("flags", "CASE_INSENSITIVE")
                .affects("lowercase", "false")
                .affects("stopwords", List.of("foo"))
                .affects("stopwords_case", "true")
        );
        analyzers.put(
            "snowball",
            settings().affects("language", "German")
                .affects("name", "German")
                .affects("stopwords", List.of("foo"))
                .affects("stopwords_case", "true")
        );
        return analyzers;
    }

    @Override
    protected Map<String, FactorySettings> tokenFilterSettings() {
        Map<String, FactorySettings> filters = new HashMap<>();
        for (String stateless : List.of(
            "apostrophe",
            "arabic_normalization",
            "arabic_stem",
            "bengali_normalization",
            "cjk_width",
            "classic",
            "czech_stem",
            "decimal_digit",
            "flatten_graph",
            "german_normalization",
            "hindi_normalization",
            "indic_normalization",
            "kstem",
            "persian_normalization",
            "persian_stem",
            "porter_stem",
            "remove_duplicates",
            "reverse",
            "russian_stem",
            "scandinavian_folding",
            "scandinavian_normalization",
            "serbian_normalization",
            "sorani_normalization",
            "trim",
            "uppercase"
        )) {
            filters.put(stateless, alwaysShares());
        }
        for (String stemmer : List.of("brazilian_stem", "dutch_stem", "french_stem", "german_stem")) {
            filters.put(stemmer, settings().affects("stem_exclusion", List.of("foo")));
        }
        filters.put("asciifolding", settings().affects("preserve_original", "true"));
        filters.put("cjk_bigram", settings().affects("output_unigrams", "true").affects("ignored_scripts", List.of("han")));
        filters.put(
            "common_grams",
            settings(Map.of("common_words", List.of("the"))).affects("common_words", List.of("a"))
                .affects("ignore_case", "true")
                .affects("query_mode", "true")
        );
        // An unrecognized encoding falls back to the default float encoder, so it keys the same as "float".
        filters.put(
            "delimited_payload",
            settings().affects("delimiter", "+").affects("encoding", "int", "identity").ignored("encoding", "float", "unknown")
        );
        filters.put(
            "dictionary_decompounder",
            settings(Map.of("word_list", List.of("foo"))).affects("word_list", List.of("bar"))
                .affects("word_list_case", "true")
                .affects("min_word_size", "3")
                .affects("min_subword_size", "3")
                .affects("max_subword_size", "10")
                .affects("only_longest_match", "true")
        );
        filters.put(
            "elision",
            settings(Map.of("articles", List.of("l"))).affects("articles", List.of("d")).affects("articles_case", "true")
        );
        // side is also keyed, but setting it emits a deprecation warning the contract run would fail on.
        filters.put("edge_ngram", settings().affects("min_gram", "2").affects("max_gram", "3").affects("preserve_original", "true"));
        filters.put("fingerprint", settings().affects("separator", "+").affects("max_output_size", "10"));
        filters.put(
            "keep",
            settings(Map.of("keep_words", List.of("foo"))).affects("keep_words", List.of("bar")).affects("keep_words_case", "true")
        );
        // keywords_pattern excludes keywords, and updateable file-based keywords never share; SharingKeyTests covers both.
        filters.put(
            "keyword_marker",
            settings(Map.of("keywords", List.of("Foo"))).affects("keywords", List.of("bar"))
                .affects("ignore_case", "true")
                .affects("keywords_case", "true")
                .affects("updateable", "true")
        );
        filters.put(
            "keep_types",
            settings(Map.of("types", List.of("<NUM>"))).affects("types", List.of("<ALPHANUM>")).affects("mode", "exclude")
        );
        filters.put("length", settings().affects("min", "2").affects("max", "10"));
        filters.put("limit", settings().affects("max_token_count", "5").affects("consume_all_tokens", "true"));
        filters.put("lowercase", settings().affects("language", "greek", "irish", "turkish"));
        // with_rotation defaults to true because the default bucket_count is above 1.
        filters.put(
            "min_hash",
            settings().affects("hash_count", "2")
                .affects("bucket_count", "2")
                .affects("hash_set_size", "2")
                .affects("with_rotation", "false")
                .ignored("with_rotation", "true")
        );
        // The filters it names are resolved per index and are not covered by the analyzer key.
        filters.put("multiplexer", neverShares());
        filters.put("ngram", settings().affects("min_gram", "2").affects("max_gram", "3").affects("preserve_original", "true"));
        filters.put(
            "pattern_capture",
            settings(Map.of("patterns", List.of("(a)"))).affects("patterns", List.of("(b)")).affects("preserve_original", "false")
        );
        filters.put(
            "pattern_replace",
            settings(Map.of("pattern", "a")).affects("pattern", "b")
                .affects("flags", "CASE_INSENSITIVE")
                .affects("replacement", "x")
                .affects("all", "false")
        );
        filters.put("snowball", settings().affects("language", "German"));
        filters.put("stemmer", settings().affects("language", "german", "light_german").affects("name", "german"));
        filters.put("stemmer_override", settings(Map.of("rules", List.of("running => run"))).affects("rules", List.of("ran => run")));
        filters.put("truncate", settings(Map.of("length", "5")).affects("length", "6"));
        filters.put("unique", settings().affects("only_on_same_position", "true"));
        for (String wordDelimiter : List.of("word_delimiter", "word_delimiter_graph")) {
            // protected_words_case only applies when there are protected words.
            FactorySettings declaration = settings(Map.of("protected_words", List.of("foo"))).affects("type_table", List.of("$ => DIGIT"))
                .affects("protected_words", List.of("bar"))
                .affects("protected_words_case", "true")
                .affects("generate_word_parts", "false")
                .affects("generate_number_parts", "false")
                .affects("catenate_words", "true")
                .affects("catenate_numbers", "true")
                .affects("catenate_all", "true")
                .affects("split_on_case_change", "false")
                .affects("preserve_original", "true")
                .affects("split_on_numerics", "false")
                .affects("stem_english_possessive", "false");
            if (wordDelimiter.equals("word_delimiter_graph")) {
                declaration.affects("adjust_offsets", "false").affects("ignore_keywords", "true");
            }
            filters.put(wordDelimiter, declaration);
        }
        return filters;
    }

    @Override
    protected Map<String, FactorySettings> tokenizerSettings() {
        Map<String, FactorySettings> tokenizers = new HashMap<>();
        for (String stateless : List.of("letter", "lowercase", "thai")) {
            tokenizers.put(stateless, alwaysShares());
        }
        for (String maxTokenLength : List.of("classic", "uax_url_email", "whitespace")) {
            tokenizers.put(maxTokenLength, settings().affects("max_token_length", "10"));
        }
        for (String ngram : List.of("ngram", "edge_ngram")) {
            tokenizers.put(
                ngram,
                settings().affects("min_gram", "2")
                    .affects("max_gram", "3")
                    .affects("token_chars", List.of("letter"), List.of("letter", "digit"))
                    // switches the key to identity, so it must not share
                    .affects("custom_token_chars", "-")
            );
        }
        for (String simplePattern : List.of("simple_pattern", "simple_pattern_split")) {
            tokenizers.put(simplePattern, settings().affects("pattern", "a"));
        }
        for (String pathHierarchy : List.of("path_hierarchy", "PathHierarchy")) {
            tokenizers.put(
                pathHierarchy,
                settings().affects("buffer_size", "100")
                    .affects("delimiter", "-")
                    .affects("replacement", "-")
                    .affects("skip", "1")
                    .affects("reverse", "true")
            );
        }
        tokenizers.put(
            "char_group",
            settings(Map.of("tokenize_on_chars", List.of("whitespace"))).affects("tokenize_on_chars", List.of("-"), List.of("digit"))
                .affects("max_token_length", "10")
        );
        tokenizers.put("keyword", settings().affects("buffer_size", "100"));
        tokenizers.put(
            "pattern",
            settings(Map.of("pattern", "(a)")).affects("pattern", "(b)").affects("flags", "CASE_INSENSITIVE").affects("group", "1")
        );
        return tokenizers;
    }

    @Override
    protected Map<String, FactorySettings> charFilterSettings() {
        return Map.of(
            "html_strip",
            settings().affects("escaped_tags", List.of("b")),
            "mapping",
            settings(Map.of("mappings", List.of("a => b"))).affects("mappings", List.of("c => d")),
            "pattern_replace",
            settings(Map.of("pattern", "a")).affects("pattern", "b").affects("flags", "CASE_INSENSITIVE").affects("replacement", "x")
        );
    }

    @Override
    protected Set<FactoryRef> factorySettingsExemptions() {
        // The deprecated camel-case ngram names are rejected on indices created on or after 8.0, and the
        // contract builds current-version indices; the ngram / edge_ngram declarations cover the same factories.
        // The two filters also never share; CommonAnalysisPluginTests covers that.
        // condition and predicate_token_filter compile a script and hyphenation_decompounder reads a patterns file
        // from the config directory, neither of which the contract's registry has; SharingKeyTests covers them.
        return Set.of(
            tokenFilter("nGram"),
            tokenFilter("edgeNGram"),
            tokenizer("nGram"),
            tokenizer("edgeNGram"),
            tokenFilter("condition"),
            tokenFilter("predicate_token_filter"),
            tokenFilter("hyphenation_decompounder")
        );
    }

    @Override
    protected Set<FactoryRef> factorySettingsPending() {
        return Set.of(tokenFilter("synonym"), tokenFilter("synonym_graph"));
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
