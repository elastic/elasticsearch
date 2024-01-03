/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.analysis;

import org.apache.lucene.analysis.TokenFilterFactory;
import org.apache.lucene.analysis.TokenizerFactory;
import org.elasticsearch.index.analysis.HunspellTokenFilterFactory;
import org.elasticsearch.index.analysis.ShingleTokenFilterFactory;
import org.elasticsearch.index.analysis.StandardTokenizerFactory;
import org.elasticsearch.index.analysis.StopTokenFilterFactory;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Map.entry;

/**
 * Alerts us if new analysis components are added to Lucene, so we don't miss them.
 * <p>
 * If we don't want to expose one for a specific reason, just map it to Void.
 * The deprecated ones can be mapped to Deprecated.class.
 */
public abstract class AnalysisFactoryTestCase extends ESTestCase {

    private static final Map<String, Class<?>> KNOWN_TOKENIZERS = Map.ofEntries(
        // exposed in ES
        entry("classic", MovedToAnalysisCommon.class),
        entry("edgengram", MovedToAnalysisCommon.class),
        entry("keyword", MovedToAnalysisCommon.class),
        entry("letter", MovedToAnalysisCommon.class),
        entry("ngram", MovedToAnalysisCommon.class),
        entry("pathhierarchy", MovedToAnalysisCommon.class),
        entry("pattern", MovedToAnalysisCommon.class),
        entry("simplepattern", MovedToAnalysisCommon.class),
        entry("simplepatternsplit", MovedToAnalysisCommon.class),
        entry("standard", StandardTokenizerFactory.class),
        entry("thai", MovedToAnalysisCommon.class),
        entry("uax29urlemail", MovedToAnalysisCommon.class),
        entry("whitespace", MovedToAnalysisCommon.class),
        // this one "seems to mess up offsets". probably shouldn't be a tokenizer...
        entry("wikipedia", Void.class)
    );

    static final Map<String, Class<?>> KNOWN_TOKENFILTERS = Map.ofEntries(
        // exposed in ES
        entry("apostrophe", MovedToAnalysisCommon.class),
        entry("arabicnormalization", MovedToAnalysisCommon.class),
        entry("arabicstem", MovedToAnalysisCommon.class),
        entry("asciifolding", MovedToAnalysisCommon.class),
        entry("bengalinormalization", MovedToAnalysisCommon.class),
        entry("bengalistem", MovedToAnalysisCommon.class),
        entry("brazilianstem", MovedToAnalysisCommon.class),
        entry("bulgarianstem", MovedToAnalysisCommon.class),
        entry("cjkbigram", MovedToAnalysisCommon.class),
        entry("cjkwidth", MovedToAnalysisCommon.class),
        entry("classic", MovedToAnalysisCommon.class),
        entry("commongrams", MovedToAnalysisCommon.class),
        entry("commongramsquery", MovedToAnalysisCommon.class),
        entry("czechstem", MovedToAnalysisCommon.class),
        entry("decimaldigit", MovedToAnalysisCommon.class),
        entry("delimitedpayload", MovedToAnalysisCommon.class),
        entry("dictionarycompoundword", MovedToAnalysisCommon.class),
        entry("edgengram", MovedToAnalysisCommon.class),
        entry("elision", MovedToAnalysisCommon.class),
        entry("englishminimalstem", MovedToAnalysisCommon.class),
        entry("englishpossessive", MovedToAnalysisCommon.class),
        entry("finnishlightstem", MovedToAnalysisCommon.class),
        entry("fixedshingle", MovedToAnalysisCommon.class),
        entry("frenchlightstem", MovedToAnalysisCommon.class),
        entry("frenchminimalstem", MovedToAnalysisCommon.class),
        entry("galicianminimalstem", MovedToAnalysisCommon.class),
        entry("galicianstem", MovedToAnalysisCommon.class),
        entry("germanstem", MovedToAnalysisCommon.class),
        entry("germanlightstem", MovedToAnalysisCommon.class),
        entry("germanminimalstem", MovedToAnalysisCommon.class),
        entry("germannormalization", MovedToAnalysisCommon.class),
        entry("greeklowercase", MovedToAnalysisCommon.class),
        entry("greekstem", MovedToAnalysisCommon.class),
        entry("hindinormalization", MovedToAnalysisCommon.class),
        entry("hindistem", MovedToAnalysisCommon.class),
        entry("hungarianlightstem", MovedToAnalysisCommon.class),
        entry("hunspellstem", HunspellTokenFilterFactory.class),
        entry("hyphenationcompoundword", MovedToAnalysisCommon.class),
        entry("indicnormalization", MovedToAnalysisCommon.class),
        entry("irishlowercase", MovedToAnalysisCommon.class),
        entry("indonesianstem", MovedToAnalysisCommon.class),
        entry("italianlightstem", MovedToAnalysisCommon.class),
        entry("keepword", MovedToAnalysisCommon.class),
        entry("keywordmarker", MovedToAnalysisCommon.class),
        entry("kstem", MovedToAnalysisCommon.class),
        entry("latvianstem", MovedToAnalysisCommon.class),
        entry("length", MovedToAnalysisCommon.class),
        entry("limittokencount", MovedToAnalysisCommon.class),
        entry("lowercase", MovedToAnalysisCommon.class),
        entry("ngram", MovedToAnalysisCommon.class),
        entry("norwegianlightstem", MovedToAnalysisCommon.class),
        entry("norwegianminimalstem", MovedToAnalysisCommon.class),
        entry("norwegiannormalization", MovedToAnalysisCommon.class),
        entry("patterncapturegroup", MovedToAnalysisCommon.class),
        entry("patternreplace", MovedToAnalysisCommon.class),
        entry("persiannormalization", MovedToAnalysisCommon.class),
        entry("porterstem", MovedToAnalysisCommon.class),
        entry("portuguesestem", MovedToAnalysisCommon.class),
        entry("portugueselightstem", MovedToAnalysisCommon.class),
        entry("portugueseminimalstem", MovedToAnalysisCommon.class),
        entry("reversestring", MovedToAnalysisCommon.class),
        entry("russianlightstem", MovedToAnalysisCommon.class),
        entry("scandinavianfolding", MovedToAnalysisCommon.class),
        entry("scandinaviannormalization", MovedToAnalysisCommon.class),
        entry("serbiannormalization", MovedToAnalysisCommon.class),
        entry("shingle", ShingleTokenFilterFactory.class),
        entry("minhash", MovedToAnalysisCommon.class),
        entry("snowballporter", MovedToAnalysisCommon.class),
        entry("soraninormalization", MovedToAnalysisCommon.class),
        entry("soranistem", MovedToAnalysisCommon.class),
        entry("spanishlightstem", MovedToAnalysisCommon.class),
        entry("stemmeroverride", MovedToAnalysisCommon.class),
        entry("stop", StopTokenFilterFactory.class),
        entry("swedishlightstem", MovedToAnalysisCommon.class),
        entry("swedishminimalstem", MovedToAnalysisCommon.class),
        entry("synonym", MovedToAnalysisCommon.class),
        entry("synonymgraph", MovedToAnalysisCommon.class),
        entry("telugunormalization", MovedToAnalysisCommon.class),
        entry("telugustem", MovedToAnalysisCommon.class),
        entry("trim", MovedToAnalysisCommon.class),
        entry("truncate", MovedToAnalysisCommon.class),
        entry("turkishlowercase", MovedToAnalysisCommon.class),
        entry("type", MovedToAnalysisCommon.class),
        entry("uppercase", MovedToAnalysisCommon.class),
        entry("worddelimiter", MovedToAnalysisCommon.class),
        entry("worddelimitergraph", MovedToAnalysisCommon.class),
        entry("flattengraph", MovedToAnalysisCommon.class),
        // TODO: these tokenfilters are not yet exposed: useful?
        // suggest stop
        entry("suggeststop", Void.class),
        // capitalizes tokens
        entry("capitalization", Void.class),
        // like length filter (but codepoints)
        entry("codepointcount", Void.class),
        // puts hyphenated words back together
        entry("hyphenatedwords", Void.class),
        // repeats anything marked as keyword
        entry("keywordrepeat", Void.class),
        // like limittokencount, but by offset
        entry("limittokenoffset", Void.class),
        // like limittokencount, but by position
        entry("limittokenposition", Void.class),
        // ???
        entry("numericpayload", Void.class),
        // removes duplicates at the same position (this should be used by the existing factory)
        entry("removeduplicates", Void.class),
        // ???
        entry("tokenoffsetpayload", Void.class),
        // puts the type into the payload
        entry("typeaspayload", Void.class),
        // puts the type as a synonym
        entry("typeassynonym", Void.class),
        // fingerprint
        entry("fingerprint", Void.class),
        // for tee-sinks
        entry("daterecognizer", Void.class),
        // for token filters that generate bad offsets, which are now rejected since Lucene 7
        entry("fixbrokenoffsets", Void.class),
        // should we expose it, or maybe think about higher level integration of the
        // fake term frequency feature (LUCENE-7854)
        entry("delimitedtermfrequency", Void.class),
        // LUCENE-8273: ProtectedTermFilterFactory allows analysis chains to skip
        // particular token filters based on the attributes of the current token.
        entry("protectedterm", Void.class),
        // LUCENE-8332
        entry("concatenategraph", Void.class),
        // LUCENE-8936
        entry("spanishminimalstem", Void.class),
        entry("delimitedboost", Void.class),
        // LUCENE-9574
        entry("dropifflagged", Void.class),
        entry("japanesecompletion", Void.class),
        // LUCENE-9575
        entry("patterntyping", Void.class),
        // LUCENE-10248
        entry("spanishpluralstem", Void.class),
        // LUCENE-10352
        entry("daitchmokotoffsoundex", Void.class),
        entry("persianstem", Void.class),
        // not exposed
        entry("word2vecsynonym", Void.class)
    );

    static final Map<String, Class<?>> KNOWN_CHARFILTERS = Map.of(
        "htmlstrip",
        MovedToAnalysisCommon.class,
        "mapping",
        MovedToAnalysisCommon.class,
        "patternreplace",
        MovedToAnalysisCommon.class,
        // TODO: these charfilters are not yet exposed: useful?
        // handling of zwnj for persian
        "persian",
        Void.class,
        // LUCENE-9413 : it might useful for dictionary-based CJK analyzers
        "cjkwidth",
        Void.class
    );

    /**
     * The plugin being tested. Core uses an "empty" plugin so we don't have to throw null checks all over the place.
     */
    private final AnalysisPlugin plugin;

    public AnalysisFactoryTestCase(AnalysisPlugin plugin) {
        this.plugin = Objects.requireNonNull(plugin, "plugin is required. use an empty plugin for core");
    }

    protected Map<String, Class<?>> getCharFilters() {
        return KNOWN_CHARFILTERS;
    }

    protected Map<String, Class<?>> getTokenFilters() {
        return KNOWN_TOKENFILTERS;
    }

    protected Map<String, Class<?>> getTokenizers() {
        return KNOWN_TOKENIZERS;
    }

    /**
     * Map containing pre-configured token filters that should be available
     * after installing this plugin. The map is from the name of the token
     * filter to the class of the Lucene {@link TokenFilterFactory} that it
     * is emulating. If the Lucene {@linkplain TokenFilterFactory} is
     * {@code null} then the test will look it up for you from the name. If
     * there is no Lucene {@linkplain TokenFilterFactory} then the right
     * hand side should be {@link Void}.
     */
    protected Map<String, Class<?>> getPreConfiguredTokenFilters() {
        Map<String, Class<?>> filters = new HashMap<>();
        filters.put("lowercase", null);
        // for old indices
        filters.put("standard", Void.class);
        return filters;
    }

    /**
     * Map containing pre-configured tokenizers that should be available
     * after installing this plugin. The map is from the name of the token
     * filter to the class of the Lucene {@link TokenizerFactory} that it
     * is emulating. If the Lucene {@linkplain TokenizerFactory} is
     * {@code null} then the test will look it up for you from the name.
     * If there is no Lucene {@linkplain TokenizerFactory} then the right
     * hand side should be {@link Void}.
     */
    protected Map<String, Class<?>> getPreConfiguredTokenizers() {
        Map<String, Class<?>> tokenizers = new HashMap<>();
        // TODO drop this temporary shim when all the old style tokenizers have been migrated to new style
        for (PreBuiltTokenizers tokenizer : PreBuiltTokenizers.values()) {
            tokenizers.put(tokenizer.name().toLowerCase(Locale.ROOT), null);
        }
        return tokenizers;
    }

    public Map<String, Class<?>> getPreConfiguredCharFilters() {
        return emptyMap();
    }

    public void testTokenizers() {
        Set<String> missing = new TreeSet<String>();
        missing.addAll(
            org.apache.lucene.analysis.TokenizerFactory.availableTokenizers()
                .stream()
                .map(key -> key.toLowerCase(Locale.ROOT))
                .collect(Collectors.toSet())
        );
        missing.removeAll(getTokenizers().keySet());
        assertTrue("new tokenizers found, please update KNOWN_TOKENIZERS: " + missing.toString(), missing.isEmpty());
    }

    public void testCharFilters() {
        Set<String> missing = new TreeSet<String>();
        missing.addAll(
            org.apache.lucene.analysis.CharFilterFactory.availableCharFilters()
                .stream()
                .map(key -> key.toLowerCase(Locale.ROOT))
                .collect(Collectors.toSet())
        );
        missing.removeAll(getCharFilters().keySet());
        assertTrue("new charfilters found, please update KNOWN_CHARFILTERS: " + missing.toString(), missing.isEmpty());
    }

    public void testTokenFilters() {
        Set<String> missing = new TreeSet<>();
        missing.addAll(
            org.apache.lucene.analysis.TokenFilterFactory.availableTokenFilters()
                .stream()
                .map(key -> key.toLowerCase(Locale.ROOT))
                .collect(Collectors.toSet())
        );
        missing.removeAll(getTokenFilters().keySet());
        assertTrue("new tokenfilters found, please update KNOWN_TOKENFILTERS: " + missing, missing.isEmpty());
    }

    /**
     * Marker class for components that have moved to the analysis-common modules. This will be
     * removed when the module is complete and these analysis components aren't available to core.
     */
    protected static final class MovedToAnalysisCommon {
        private MovedToAnalysisCommon() {}
    }
}
