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

import org.apache.lucene.analysis.util.CharFilterFactory;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.analysis.util.TokenizerFactory;
import org.elasticsearch.Version;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.index.analysis.ApostropheFilterFactory;
import org.elasticsearch.index.analysis.ArabicNormalizationFilterFactory;
import org.elasticsearch.index.analysis.ArabicStemTokenFilterFactory;
import org.elasticsearch.index.analysis.BrazilianStemTokenFilterFactory;
import org.elasticsearch.index.analysis.CJKBigramFilterFactory;
import org.elasticsearch.index.analysis.CJKWidthFilterFactory;
import org.elasticsearch.index.analysis.ClassicFilterFactory;
import org.elasticsearch.index.analysis.ClassicTokenizerFactory;
import org.elasticsearch.index.analysis.CommonGramsTokenFilterFactory;
import org.elasticsearch.index.analysis.CzechStemTokenFilterFactory;
import org.elasticsearch.index.analysis.DecimalDigitFilterFactory;
import org.elasticsearch.index.analysis.DelimitedPayloadTokenFilterFactory;
import org.elasticsearch.index.analysis.EdgeNGramTokenFilterFactory;
import org.elasticsearch.index.analysis.EdgeNGramTokenizerFactory;
import org.elasticsearch.index.analysis.ElisionTokenFilterFactory;
import org.elasticsearch.index.analysis.FlattenGraphTokenFilterFactory;
import org.elasticsearch.index.analysis.GermanNormalizationFilterFactory;
import org.elasticsearch.index.analysis.GermanStemTokenFilterFactory;
import org.elasticsearch.index.analysis.HindiNormalizationFilterFactory;
import org.elasticsearch.index.analysis.HunspellTokenFilterFactory;
import org.elasticsearch.index.analysis.IndicNormalizationFilterFactory;
import org.elasticsearch.index.analysis.KStemTokenFilterFactory;
import org.elasticsearch.index.analysis.KeepTypesFilterFactory;
import org.elasticsearch.index.analysis.KeepWordFilterFactory;
import org.elasticsearch.index.analysis.KeywordTokenizerFactory;
import org.elasticsearch.index.analysis.LengthTokenFilterFactory;
import org.elasticsearch.index.analysis.LetterTokenizerFactory;
import org.elasticsearch.index.analysis.LimitTokenCountFilterFactory;
import org.elasticsearch.index.analysis.LowerCaseTokenFilterFactory;
import org.elasticsearch.index.analysis.LowerCaseTokenizerFactory;
import org.elasticsearch.index.analysis.MinHashTokenFilterFactory;
import org.elasticsearch.index.analysis.MultiTermAwareComponent;
import org.elasticsearch.index.analysis.NGramTokenFilterFactory;
import org.elasticsearch.index.analysis.NGramTokenizerFactory;
import org.elasticsearch.index.analysis.PathHierarchyTokenizerFactory;
import org.elasticsearch.index.analysis.PatternCaptureGroupTokenFilterFactory;
import org.elasticsearch.index.analysis.PatternReplaceTokenFilterFactory;
import org.elasticsearch.index.analysis.PatternTokenizerFactory;
import org.elasticsearch.index.analysis.PersianNormalizationFilterFactory;
import org.elasticsearch.index.analysis.PreConfiguredCharFilter;
import org.elasticsearch.index.analysis.PreConfiguredTokenFilter;
import org.elasticsearch.index.analysis.PreConfiguredTokenizer;
import org.elasticsearch.index.analysis.ReverseTokenFilterFactory;
import org.elasticsearch.index.analysis.ScandinavianFoldingFilterFactory;
import org.elasticsearch.index.analysis.ScandinavianNormalizationFilterFactory;
import org.elasticsearch.index.analysis.SerbianNormalizationFilterFactory;
import org.elasticsearch.index.analysis.ShingleTokenFilterFactory;
import org.elasticsearch.index.analysis.SoraniNormalizationFilterFactory;
import org.elasticsearch.index.analysis.StandardTokenFilterFactory;
import org.elasticsearch.index.analysis.StandardTokenizerFactory;
import org.elasticsearch.index.analysis.StemmerOverrideTokenFilterFactory;
import org.elasticsearch.index.analysis.StemmerTokenFilterFactory;
import org.elasticsearch.index.analysis.StopTokenFilterFactory;
import org.elasticsearch.index.analysis.SynonymGraphTokenFilterFactory;
import org.elasticsearch.index.analysis.SynonymTokenFilterFactory;
import org.elasticsearch.index.analysis.ThaiTokenizerFactory;
import org.elasticsearch.index.analysis.TruncateTokenFilterFactory;
import org.elasticsearch.index.analysis.UAX29URLEmailTokenizerFactory;
import org.elasticsearch.index.analysis.UpperCaseTokenFilterFactory;
import org.elasticsearch.index.analysis.WhitespaceTokenizerFactory;
import org.elasticsearch.index.analysis.compound.DictionaryCompoundWordTokenFilterFactory;
import org.elasticsearch.index.analysis.compound.HyphenationCompoundWordTokenFilterFactory;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.test.ESTestCase;

import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.typeCompatibleWith;

/**
 * Alerts us if new analysis components are added to Lucene, so we don't miss them.
 * <p>
 * If we don't want to expose one for a specific reason, just map it to Void.
 * The deprecated ones can be mapped to Deprecated.class.
 */
public abstract class AnalysisFactoryTestCase extends ESTestCase {

    private static final Pattern UNDERSCORE_THEN_ANYTHING = Pattern.compile("_(.)");

    private static String toCamelCase(String s) {
        Matcher m = UNDERSCORE_THEN_ANYTHING.matcher(s);
        StringBuffer sb = new StringBuffer();
        while (m.find()) {
            m.appendReplacement(sb, m.group(1).toUpperCase());
        }
        m.appendTail(sb);
        sb.setCharAt(0, Character.toUpperCase(sb.charAt(0)));
        return sb.toString();
    }

    static final Map<String,Class<?>> KNOWN_TOKENIZERS = new MapBuilder<String,Class<?>>()
        // exposed in ES
        .put("classic",       ClassicTokenizerFactory.class)
        .put("edgengram",     EdgeNGramTokenizerFactory.class)
        .put("keyword",       KeywordTokenizerFactory.class)
        .put("letter",        LetterTokenizerFactory.class)
        .put("lowercase",     LowerCaseTokenizerFactory.class)
        .put("ngram",         NGramTokenizerFactory.class)
        .put("pathhierarchy", PathHierarchyTokenizerFactory.class)
        .put("pattern",       PatternTokenizerFactory.class)
        .put("standard",      StandardTokenizerFactory.class)
        .put("thai",          ThaiTokenizerFactory.class)
        .put("uax29urlemail", UAX29URLEmailTokenizerFactory.class)
        .put("whitespace",    WhitespaceTokenizerFactory.class)

        // this one "seems to mess up offsets". probably shouldn't be a tokenizer...
        .put("wikipedia",     Void.class)

        // TODO: expose these
        .put("simplepattern",    Void.class)
        .put("simplepatternsplit",    Void.class)
        .immutableMap();

    static final Map<String,Class<?>> KNOWN_TOKENFILTERS = new MapBuilder<String,Class<?>>()
        // exposed in ES
        .put("apostrophe",                ApostropheFilterFactory.class)
        .put("arabicnormalization",       ArabicNormalizationFilterFactory.class)
        .put("arabicstem",                ArabicStemTokenFilterFactory.class)
        .put("asciifolding",              MovedToAnalysisCommon.class)
        .put("brazilianstem",             BrazilianStemTokenFilterFactory.class)
        .put("bulgarianstem",             StemmerTokenFilterFactory.class)
        .put("cjkbigram",                 CJKBigramFilterFactory.class)
        .put("cjkwidth",                  CJKWidthFilterFactory.class)
        .put("classic",                   ClassicFilterFactory.class)
        .put("commongrams",               CommonGramsTokenFilterFactory.class)
        .put("commongramsquery",          CommonGramsTokenFilterFactory.class)
        .put("czechstem",                 CzechStemTokenFilterFactory.class)
        .put("decimaldigit",              DecimalDigitFilterFactory.class)
        .put("delimitedpayload",          DelimitedPayloadTokenFilterFactory.class)
        .put("dictionarycompoundword",    DictionaryCompoundWordTokenFilterFactory.class)
        .put("edgengram",                 EdgeNGramTokenFilterFactory.class)
        .put("elision",                   ElisionTokenFilterFactory.class)
        .put("englishminimalstem",        StemmerTokenFilterFactory.class)
        .put("englishpossessive",         StemmerTokenFilterFactory.class)
        .put("finnishlightstem",          StemmerTokenFilterFactory.class)
        .put("frenchlightstem",           StemmerTokenFilterFactory.class)
        .put("frenchminimalstem",         StemmerTokenFilterFactory.class)
        .put("galicianminimalstem",       StemmerTokenFilterFactory.class)
        .put("galicianstem",              StemmerTokenFilterFactory.class)
        .put("germanstem",                GermanStemTokenFilterFactory.class)
        .put("germanlightstem",           StemmerTokenFilterFactory.class)
        .put("germanminimalstem",         StemmerTokenFilterFactory.class)
        .put("germannormalization",       GermanNormalizationFilterFactory.class)
        .put("greeklowercase",            LowerCaseTokenFilterFactory.class)
        .put("greekstem",                 StemmerTokenFilterFactory.class)
        .put("hindinormalization",        HindiNormalizationFilterFactory.class)
        .put("hindistem",                 StemmerTokenFilterFactory.class)
        .put("hungarianlightstem",        StemmerTokenFilterFactory.class)
        .put("hunspellstem",              HunspellTokenFilterFactory.class)
        .put("hyphenationcompoundword",   HyphenationCompoundWordTokenFilterFactory.class)
        .put("indicnormalization",        IndicNormalizationFilterFactory.class)
        .put("irishlowercase",            LowerCaseTokenFilterFactory.class)
        .put("indonesianstem",            StemmerTokenFilterFactory.class)
        .put("italianlightstem",          StemmerTokenFilterFactory.class)
        .put("keepword",                  KeepWordFilterFactory.class)
        .put("keywordmarker",             MovedToAnalysisCommon.class)
        .put("kstem",                     KStemTokenFilterFactory.class)
        .put("latvianstem",               StemmerTokenFilterFactory.class)
        .put("length",                    LengthTokenFilterFactory.class)
        .put("limittokencount",           LimitTokenCountFilterFactory.class)
        .put("lowercase",                 LowerCaseTokenFilterFactory.class)
        .put("ngram",                     NGramTokenFilterFactory.class)
        .put("norwegianlightstem",        StemmerTokenFilterFactory.class)
        .put("norwegianminimalstem",      StemmerTokenFilterFactory.class)
        .put("patterncapturegroup",       PatternCaptureGroupTokenFilterFactory.class)
        .put("patternreplace",            PatternReplaceTokenFilterFactory.class)
        .put("persiannormalization",      PersianNormalizationFilterFactory.class)
        .put("porterstem",                MovedToAnalysisCommon.class)
        .put("portuguesestem",            StemmerTokenFilterFactory.class)
        .put("portugueselightstem",       StemmerTokenFilterFactory.class)
        .put("portugueseminimalstem",     StemmerTokenFilterFactory.class)
        .put("reversestring",             ReverseTokenFilterFactory.class)
        .put("russianlightstem",          StemmerTokenFilterFactory.class)
        .put("scandinavianfolding",       ScandinavianFoldingFilterFactory.class)
        .put("scandinaviannormalization", ScandinavianNormalizationFilterFactory.class)
        .put("serbiannormalization",      SerbianNormalizationFilterFactory.class)
        .put("shingle",                   ShingleTokenFilterFactory.class)
        .put("minhash",                   MinHashTokenFilterFactory.class)
        .put("snowballporter",            MovedToAnalysisCommon.class)
        .put("soraninormalization",       SoraniNormalizationFilterFactory.class)
        .put("soranistem",                StemmerTokenFilterFactory.class)
        .put("spanishlightstem",          StemmerTokenFilterFactory.class)
        .put("standard",                  StandardTokenFilterFactory.class)
        .put("stemmeroverride",           StemmerOverrideTokenFilterFactory.class)
        .put("stop",                      StopTokenFilterFactory.class)
        .put("swedishlightstem",          StemmerTokenFilterFactory.class)
        .put("synonym",                   SynonymTokenFilterFactory.class)
        .put("synonymgraph",              SynonymGraphTokenFilterFactory.class)
        .put("trim",                      MovedToAnalysisCommon.class)
        .put("truncate",                  TruncateTokenFilterFactory.class)
        .put("turkishlowercase",          LowerCaseTokenFilterFactory.class)
        .put("type",                      KeepTypesFilterFactory.class)
        .put("uppercase",                 UpperCaseTokenFilterFactory.class)
        .put("worddelimiter",             MovedToAnalysisCommon.class)
        .put("worddelimitergraph",        MovedToAnalysisCommon.class)
        .put("flattengraph",              FlattenGraphTokenFilterFactory.class)

        // TODO: these tokenfilters are not yet exposed: useful?

        // suggest stop
        .put("suggeststop",               Void.class)
        // capitalizes tokens
        .put("capitalization",            Void.class)
        // like length filter (but codepoints)
        .put("codepointcount",            Void.class)
        // puts hyphenated words back together
        .put("hyphenatedwords",           Void.class)
        // repeats anything marked as keyword
        .put("keywordrepeat",             Void.class)
        // like limittokencount, but by offset
        .put("limittokenoffset",          Void.class)
        // like limittokencount, but by position
        .put("limittokenposition",        Void.class)
        // ???
        .put("numericpayload",            Void.class)
        // removes duplicates at the same position (this should be used by the existing factory)
        .put("removeduplicates",          Void.class)
        // ???
        .put("tokenoffsetpayload",        Void.class)
        // puts the type into the payload
        .put("typeaspayload",             Void.class)
        // fingerprint
        .put("fingerprint",               Void.class)
        // for tee-sinks
        .put("daterecognizer",            Void.class)
        // for token filters that generate bad offsets, which are now rejected since Lucene 7
        .put("fixbrokenoffsets",          Void.class)

        .immutableMap();

    static final Map<String,Class<?>> KNOWN_CHARFILTERS = new MapBuilder<String,Class<?>>()
        // exposed in ES
        .put("htmlstrip",      MovedToAnalysisCommon.class)
        .put("mapping",        MovedToAnalysisCommon.class)
        .put("patternreplace", MovedToAnalysisCommon.class)

        // TODO: these charfilters are not yet exposed: useful?
        // handling of zwnj for persian
        .put("persian",        Void.class)
        .immutableMap();

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
        filters.put("standard", null);
        filters.put("lowercase", null);
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
            final Class<?> luceneFactoryClazz;
            switch (tokenizer) {
            case UAX_URL_EMAIL:
                luceneFactoryClazz = org.apache.lucene.analysis.standard.UAX29URLEmailTokenizerFactory.class;
                break;
            case PATH_HIERARCHY:
                luceneFactoryClazz = Void.class;
                break;
            default:
                luceneFactoryClazz = null;
            }
            tokenizers.put(tokenizer.name().toLowerCase(Locale.ROOT), luceneFactoryClazz);
        }
        // TODO drop aliases once they are moved to module
        tokenizers.put("nGram", tokenizers.get("ngram"));
        tokenizers.put("edgeNGram", tokenizers.get("edge_ngram"));
        tokenizers.put("PathHierarchy", tokenizers.get("path_hierarchy"));
        return tokenizers;
    }

    public Map<String, Class<?>> getPreConfiguredCharFilters() {
        return emptyMap();
    }

    public void testTokenizers() {
        Set<String> missing = new TreeSet<String>(org.apache.lucene.analysis.util.TokenizerFactory.availableTokenizers());
        missing.removeAll(getTokenizers().keySet());
        assertTrue("new tokenizers found, please update KNOWN_TOKENIZERS: " + missing.toString(), missing.isEmpty());
    }

    public void testCharFilters() {
        Set<String> missing = new TreeSet<String>(org.apache.lucene.analysis.util.CharFilterFactory.availableCharFilters());
        missing.removeAll(getCharFilters().keySet());
        assertTrue("new charfilters found, please update KNOWN_CHARFILTERS: " + missing.toString(), missing.isEmpty());
    }

    public void testTokenFilters() {
        Set<String> missing = new TreeSet<String>(org.apache.lucene.analysis.util.TokenFilterFactory.availableTokenFilters());
        missing.removeAll(getTokenFilters().keySet());
        assertTrue("new tokenfilters found, please update KNOWN_TOKENFILTERS: " + missing.toString(), missing.isEmpty());
    }

    public void testMultiTermAware() {
        Collection<Class<?>> expected = new HashSet<>();
        for (Map.Entry<String, Class<?>> entry : getTokenizers().entrySet()) {
            if (org.apache.lucene.analysis.util.MultiTermAwareComponent.class.isAssignableFrom(
                    org.apache.lucene.analysis.util.TokenizerFactory.lookupClass(entry.getKey()))) {
                expected.add(entry.getValue());
            }
        }
        for (Map.Entry<String, Class<?>> entry : getTokenFilters().entrySet()) {
            if (org.apache.lucene.analysis.util.MultiTermAwareComponent.class.isAssignableFrom(
                    org.apache.lucene.analysis.util.TokenFilterFactory.lookupClass(entry.getKey()))) {
                expected.add(entry.getValue());
            }
        }
        for (Map.Entry<String, Class<?>> entry : getCharFilters().entrySet()) {
            if (org.apache.lucene.analysis.util.MultiTermAwareComponent.class.isAssignableFrom(
                    org.apache.lucene.analysis.util.CharFilterFactory.lookupClass(entry.getKey()))) {
                expected.add(entry.getValue());
            }
        }
        expected.remove(Void.class);
        expected.remove(MovedToAnalysisCommon.class);
        expected.remove(Deprecated.class);

        Collection<Class<?>> actual = new HashSet<>();
        for (Class<?> clazz : getTokenizers().values()) {
            if (MultiTermAwareComponent.class.isAssignableFrom(clazz)) {
                actual.add(clazz);
            }
        }
        for (Class<?> clazz : getTokenFilters().values()) {
            if (MultiTermAwareComponent.class.isAssignableFrom(clazz)) {
                actual.add(clazz);
            }
        }
        for (Class<?> clazz : getCharFilters().values()) {
            if (MultiTermAwareComponent.class.isAssignableFrom(clazz)) {
                actual.add(clazz);
            }
        }

        Set<Class<?>> classesMissingMultiTermSupport = new HashSet<>(expected);
        classesMissingMultiTermSupport.removeAll(actual);
        assertTrue("Classes are missing multi-term support: " + classesMissingMultiTermSupport,
                classesMissingMultiTermSupport.isEmpty());

        Set<Class<?>> classesThatShouldNotHaveMultiTermSupport = new HashSet<>(actual);
        classesThatShouldNotHaveMultiTermSupport.removeAll(expected);
        assertTrue("Classes should not have multi-term support: " + classesThatShouldNotHaveMultiTermSupport,
                classesThatShouldNotHaveMultiTermSupport.isEmpty());
    }

    public void testPreBuiltMultiTermAware() {
        Collection<Object> expected = new HashSet<>();
        Collection<Object> actual = new HashSet<>();

        Map<String, PreConfiguredTokenFilter> preConfiguredTokenFilters =
                new HashMap<>(AnalysisModule.setupPreConfiguredTokenFilters(singletonList(plugin)));
        for (Map.Entry<String, Class<?>> entry : getPreConfiguredTokenFilters().entrySet()) {
            String name = entry.getKey();
            Class<?> luceneFactory = entry.getValue();
            PreConfiguredTokenFilter filter = preConfiguredTokenFilters.remove(name);
            assertNotNull("test claims pre built token filter [" + name + "] should be available but it wasn't", filter);
            if (luceneFactory == Void.class) {
                continue;
            }
            if (luceneFactory == null) {
                luceneFactory = TokenFilterFactory.lookupClass(toCamelCase(name));
            }
            assertThat(luceneFactory, typeCompatibleWith(TokenFilterFactory.class));
            if (filter.shouldUseFilterForMultitermQueries()) {
                actual.add("token filter [" + name + "]");
            }
            if (org.apache.lucene.analysis.util.MultiTermAwareComponent.class.isAssignableFrom(luceneFactory)) {
                expected.add("token filter [" + name + "]");
            }
        }
        assertThat("pre configured token filter not registered with test", preConfiguredTokenFilters.keySet(), empty());

        Map<String, PreConfiguredTokenizer> preConfiguredTokenizers = new HashMap<>(
                AnalysisModule.setupPreConfiguredTokenizers(singletonList(plugin)));
        for (Map.Entry<String, Class<?>> entry : getPreConfiguredTokenizers().entrySet()) {
            String name = entry.getKey();
            Class<?> luceneFactory = entry.getValue();
            PreConfiguredTokenizer tokenizer = preConfiguredTokenizers.remove(name);
            assertNotNull("test claims pre built tokenizer [" + name + "] should be available but it wasn't", tokenizer);
            if (luceneFactory == Void.class) {
                continue;
            }
            if (luceneFactory == null) {
                luceneFactory = TokenizerFactory.lookupClass(toCamelCase(name));
            }
            assertThat(luceneFactory, typeCompatibleWith(TokenizerFactory.class));
            if (tokenizer.hasMultiTermComponent()) {
                actual.add(tokenizer);
            }
            if (org.apache.lucene.analysis.util.MultiTermAwareComponent.class.isAssignableFrom(luceneFactory)) {
                expected.add(tokenizer);
            }
        }
        assertThat("pre configured tokenizer not registered with test", preConfiguredTokenizers.keySet(), empty());

        Map<String, PreConfiguredCharFilter> preConfiguredCharFilters = new HashMap<>(
                AnalysisModule.setupPreConfiguredCharFilters(singletonList(plugin)));
        for (Map.Entry<String, Class<?>> entry : getPreConfiguredCharFilters().entrySet()) {
            String name = entry.getKey();
            Class<?> luceneFactory = entry.getValue();
            PreConfiguredCharFilter filter = preConfiguredCharFilters.remove(name);
            assertNotNull("test claims pre built char filter [" + name + "] should be available but it wasn't", filter);
            if (luceneFactory == Void.class) {
                continue;
            }
            if (luceneFactory == null) {
                luceneFactory = TokenFilterFactory.lookupClass(toCamelCase(name));
            }
            assertThat(luceneFactory, typeCompatibleWith(CharFilterFactory.class));
            if (filter.shouldUseFilterForMultitermQueries()) {
                actual.add(filter);
            }
            if (org.apache.lucene.analysis.util.MultiTermAwareComponent.class.isAssignableFrom(luceneFactory)) {
                expected.add("token filter [" + name + "]");
            }
        }
        assertThat("pre configured char filter not registered with test", preConfiguredCharFilters.keySet(), empty());

        Set<Object> classesMissingMultiTermSupport = new HashSet<>(expected);
        classesMissingMultiTermSupport.removeAll(actual);
        assertTrue("Pre-built components are missing multi-term support: " + classesMissingMultiTermSupport,
                classesMissingMultiTermSupport.isEmpty());

        Set<Object> classesThatShouldNotHaveMultiTermSupport = new HashSet<>(actual);
        classesThatShouldNotHaveMultiTermSupport.removeAll(expected);
        assertTrue("Pre-built components should not have multi-term support: " + classesThatShouldNotHaveMultiTermSupport,
                classesThatShouldNotHaveMultiTermSupport.isEmpty());
    }

    /**
     * Marker class for components that have moved to the analysis-common modules. This will be
     * removed when the module is complete and these analysis components aren't available to core.
     */
    protected static final class MovedToAnalysisCommon {
        private MovedToAnalysisCommon() {}
    }
}
