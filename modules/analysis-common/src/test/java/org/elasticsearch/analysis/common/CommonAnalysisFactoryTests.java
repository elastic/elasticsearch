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

    /**
     * Synonyms have a dedicated end-to-end sharing test (they need wired services for some sources);
     * the hyphenation decompounder and other resource-backed factories this lightweight harness
     * cannot feed are also covered there. Listed here so the completeness gate treats them as
     * classified.
     */
    @Override
    protected Set<String> factorySettingsExemptions() {
        return Set.of(
            // identity-keyed (reference other filters by name); identity mechanism tested in FactorySharingKeyTests
            "multiplexer",
            "condition",
            // synonyms have dedicated end-to-end sharing tests (and need wired services for some sources)
            "synonym",
            "synonym_graph",
            // needs a script service this lightweight harness does not wire; covered elsewhere
            "predicate_token_filter",
            // needs a hyphenation patterns file this harness cannot supply
            "hyphenation_decompounder",
            // deprecated camelCase aliases, rejected at index creation for current index versions
            "nGram",
            "edgeNGram",
            "PathHierarchy"
        );
    }

    @Override
    protected Map<String, FactorySettings> tokenFilterSettings() {
        Map<String, FactorySettings> p = new TreeMap<>();
        // stateless — sharing key is a constant; no setting changes behavior
        for (String s : List.of(
            "apostrophe",
            "arabic_normalization",
            "arabic_stem",
            "bengali_normalization",
            "brazilian_stem",
            "cjk_width",
            "classic",
            "czech_stem",
            "decimal_digit",
            "dutch_stem",
            "flatten_graph",
            "french_stem",
            "german_normalization",
            "german_stem",
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
            p.put(s, stateless());
        }
        // value-keyed — each declared setting must produce a distinct instance
        p.put("asciifolding", settings().affects("preserve_original", "true"));
        p.put("cjk_bigram", settings().affects("output_unigrams", "true").affects("ignored_scripts", List.of("hangul")));
        p.put(
            "common_grams",
            settings(Map.of("common_words", List.of("the"))).affects("common_words", List.of("and"))
                .affects("ignore_case", "true")
                .affects("query_mode", "true")
        );
        // Base uses a non-default encoding so the "two identical configs share" baseline check exercises
        // a freshly-built encoder instance (FloatEncoder/IntegerEncoder/IdentityEncoder are identity-
        // compared); a key built from the encoder instance rather than the encoding name would fail it.
        p.put("delimited_payload", settings(Map.of("encoding", "identity")).affects("delimiter", "/").affects("encoding", "int"));
        p.put(
            "dictionary_decompounder",
            settings(Map.of("word_list", List.of("quick", "brown"))).affects("word_list", List.of("quick", "fox"))
                .affects("word_list_case", "true")
                .affects("min_subword_size", "3")
                .affects("only_longest_match", "true")
        );
        p.put(
            "edge_ngram",
            settings(Map.of("min_gram", "1", "max_gram", "2")).affects("max_gram", "3").affects("preserve_original", "true")
        );
        p.put("elision", settings(Map.of("articles", List.of("o"))).affects("articles", List.of("l")).affects("articles_case", "true"));
        p.put("fingerprint", settings().affects("separator", "+").affects("max_output_size", "2"));
        p.put(
            "keep",
            settings(Map.of("keep_words", List.of("naive"))).affects("keep_words", List.of("running")).affects("keep_words_case", "true")
        );
        p.put("keep_types", settings(Map.of("types", List.of("<ALPHANUM>"))).affects("types", List.of("<NUM>")).affects("mode", "exclude"));
        p.put(
            "keyword_marker",
            settings(Map.of("keywords", List.of("fox"))).affects("keywords", List.of("cat")).affects("ignore_case", "true")
        );
        p.put("length", settings().affects("min", "2").affects("max", "5"));
        p.put("limit", settings().affects("max_token_count", "2").affects("consume_all_tokens", "true"));
        p.put("lowercase", settings().affects("language", "greek", "turkish"));
        p.put(
            "min_hash",
            settings().affects("bucket_count", "256")
                .affects("hash_count", "2")
                .affects("hash_set_size", "2")
                .affects("with_rotation", "false")
        );
        p.put("ngram", settings(Map.of("min_gram", "1", "max_gram", "2")).affects("max_gram", "3").affects("min_gram", "2"));
        p.put(
            "pattern_capture",
            settings(Map.of("patterns", List.of("(\\d+)"))).affects("patterns", List.of("(\\w+)")).affects("preserve_original", "false")
        );
        p.put(
            "pattern_replace",
            settings(Map.of("pattern", "a")).affects("pattern", "b").affects("replacement", "x").affects("all", "false")
        );
        p.put("snowball", settings().affects("language", "French"));
        p.put("stemmer", settings().affects("language", "french", "russian"));
        p.put("stemmer_override", settings(Map.of("rules", List.of("running => run"))).affects("rules", List.of("running => sprint")));
        p.put("truncate", settings(Map.of("length", "10")).affects("length", "3"));
        p.put("unique", settings().affects("only_on_same_position", "true"));
        Map<String, Object> wdBase = Map.of("protected_words", List.of("powershot"));
        p.put(
            "word_delimiter",
            settings(wdBase).affects("preserve_original", "true")
                .affects("catenate_words", "true")
                .affects("generate_word_parts", "false")
                .affects("protected_words", List.of("wifi"))
                .affects("protected_words_case", "true")
        );
        p.put(
            "word_delimiter_graph",
            settings(wdBase).affects("preserve_original", "true")
                .affects("catenate_words", "true")
                .affects("protected_words", List.of("wifi"))
                .affects("protected_words_case", "true")
                .affects("adjust_offsets", "false")
        );
        return p;
    }

    @Override
    protected Map<String, FactorySettings> tokenizerSettings() {
        Map<String, FactorySettings> p = new TreeMap<>();
        p.put("letter", stateless());
        p.put("thai", stateless());
        p.put("lowercase", stateless()); // deprecated XLowerCaseTokenizer, constant key
        p.put(
            "char_group",
            settings(Map.of("tokenize_on_chars", List.of("whitespace"))).affects("tokenize_on_chars", List.of("letter"))
                .affects("max_token_length", "5")
        );
        p.put("classic", settings().affects("max_token_length", "5"));
        // Base uses multi-value token_chars (incl. "custom") so the "two identical configs share"
        // baseline check exercises a freshly-built CharMatcher lambda; a key built from the matcher
        // instead of the normalized token_chars settings would fail it.
        p.put(
            "edge_ngram",
            settings(
                Map.of("min_gram", "1", "max_gram", "2", "token_chars", List.of("letter", "digit", "custom"), "custom_token_chars", "+-")
            ).affects("max_gram", "3").affects("token_chars", List.of("letter")).affects("custom_token_chars", "+_")
        );
        p.put("keyword", settings().affects("buffer_size", "128"));
        p.put(
            "ngram",
            settings(
                Map.of("min_gram", "1", "max_gram", "2", "token_chars", List.of("letter", "digit", "custom"), "custom_token_chars", "+-")
            ).affects("max_gram", "3").affects("token_chars", List.of("letter")).affects("custom_token_chars", "+_")
        );
        p.put(
            "path_hierarchy",
            settings().affects("delimiter", "|")
                .affects("replacement", "_")
                .affects("buffer_size", "512")
                .affects("reverse", "true")
                .affects("skip", "1")
        );
        p.put("pattern", settings().affects("pattern", ",").affects("flags", "CASE_INSENSITIVE"));
        p.put("simple_pattern", settings(Map.of("pattern", "[0-9]+")).affects("pattern", "[a-z]+"));
        p.put("simple_pattern_split", settings(Map.of("pattern", "-")).affects("pattern", ","));
        p.put("uax_url_email", settings().affects("max_token_length", "5"));
        p.put("whitespace", settings().affects("max_token_length", "5"));
        return p;
    }

    @Override
    protected Map<String, FactorySettings> charFilterSettings() {
        Map<String, FactorySettings> p = new TreeMap<>();
        p.put("html_strip", settings().affects("escaped_tags", List.of("br")));
        p.put("mapping", settings(Map.of("mappings", List.of("a => b"))).affects("mappings", List.of("a => c")));
        p.put(
            "pattern_replace",
            settings(Map.of("pattern", "a")).affects("pattern", "b").affects("replacement", "x").affects("flags", "CASE_INSENSITIVE")
        );
        return p;
    }

    @Override
    protected Map<String, FactorySettings> analyzerSettings() {
        Map<String, FactorySettings> p = new TreeMap<>();
        p.put("keyword", stateless());
        p.put("simple", stateless());
        p.put("whitespace", stateless());
        p.put("chinese", stateless());
        p.put(
            "fingerprint",
            settings().affects("separator", "+")
                .affects("max_output_size", "2")
                .affects("stopwords", List.of("the"))
                .affects("stopwords_case", "true")
        );
        p.put(
            "pattern",
            settings().affects("pattern", "\\d+")
                .affects("lowercase", "false")
                .affects("stopwords", List.of("the"))
                .affects("stopwords_case", "true")
        );
        p.put("snowball", settings().affects("language", "French").affects("stopwords", List.of("the")).affects("stopwords_case", "true"));
        // stop and the language analyzers are keyed on their stop-word set. stopwords_case toggles the
        // case-sensitivity of stop-word matching, so it is behavior-affecting and must change the key.
        p.put("stop", settings().affects("stopwords", List.of("zzzqux")).affects("stopwords_case", "true"));
        p.put("english", settings().affects("stopwords", List.of("zzzqux")).affects("stopwords_case", "true"));
        for (String lang : List.of(
            "arabic",
            "armenian",
            "basque",
            "bengali",
            "brazilian",
            "bulgarian",
            "catalan",
            "cjk",
            "czech",
            "danish",
            "dutch",
            "estonian",
            "finnish",
            "french",
            "galician",
            "german",
            "greek",
            "hindi",
            "hungarian",
            "indonesian",
            "irish",
            "italian",
            "latvian",
            "lithuanian",
            "norwegian",
            "persian",
            "portuguese",
            "romanian",
            "russian",
            "serbian",
            "sorani",
            "spanish",
            "swedish",
            "thai",
            "turkish"
        )) {
            p.put(lang, settings().affects("stopwords", List.of("zzzqux")).affects("stopwords_case", "true"));
        }
        return p;
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
