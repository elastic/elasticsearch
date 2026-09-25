/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenFilterFactory;
import org.apache.lucene.analysis.TokenizerFactory;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.analysis.HunspellTokenFilterFactory;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.ShingleTokenFilterFactory;
import org.elasticsearch.index.analysis.StandardTokenizerFactory;
import org.elasticsearch.index.analysis.StopTokenFilterFactory;
import org.elasticsearch.indices.analysis.AnalysisModule.AnalysisProvider;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.plugins.scanners.StablePluginsRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
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
        entry("word2vecsynonym", Void.class),
        // not exposed
        entry("romaniannormalization", Void.class),
        // TODO: expose this one
        entry("casefolding", Void.class)
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

    // Analyzer-sharing contract. Indices with identical analysis recipes share one cached
    // NamedAnalyzer, which is only safe if every factory folds all behavior-affecting settings into
    // its sharingKey(). Each subclass declares, per factory it registers, which settings distinguish
    // that key; the base checks those declarations against real index builds, and fails if a
    // registered factory is left unclassified.
    //
    // Only the plugin under test is probed. Core's factories come from AnalysisModule rather than a
    // plugin, so CoreAnalysisFactoryTests has nothing here; FactorySharingKeyTests covers those keys.

    /** Component slot a factory occupies, used to wire it into a single-component analyzer chain. */
    public enum ComponentKind {
        TOKEN_FILTER,
        TOKENIZER,
        CHAR_FILTER,
        ANALYZER
    }

    /**
     * A registered factory. Keyed by slot as well as name because one name may be registered in
     * several slots — {@code icu_normalizer} is both a char filter and a token filter.
     */
    public record FactoryRef(ComponentKind kind, String name) {}

    protected static FactoryRef tokenFilter(String name) {
        return new FactoryRef(ComponentKind.TOKEN_FILTER, name);
    }

    protected static FactoryRef tokenizer(String name) {
        return new FactoryRef(ComponentKind.TOKENIZER, name);
    }

    protected static FactoryRef charFilter(String name) {
        return new FactoryRef(ComponentKind.CHAR_FILTER, name);
    }

    protected static FactoryRef analyzer(String name) {
        return new FactoryRef(ComponentKind.ANALYZER, name);
    }

    /** Which settings a factory's sharing key must distinguish. */
    public static final class FactorySettings {
        final boolean neverShares;
        final Map<String, Object> base;
        final List<SettingCase> settings = new ArrayList<>();

        private FactorySettings(boolean neverShares, Map<String, Object> base) {
            this.neverShares = neverShares;
            this.base = base;
        }

        /** Changing this setting MUST produce a distinct instance. Each value is probed against the base. */
        public FactorySettings affects(String name, Object... values) {
            settings.add(new SettingCase(name, true, List.of(values)));
            return this;
        }

        /** This setting is read but MUST NOT change the instance. Each value is probed against the base. */
        public FactorySettings ignored(String name, Object... values) {
            settings.add(new SettingCase(name, false, List.of(values)));
            return this;
        }
    }

    /** One setting to probe, and whether changing it should produce a distinct instance. */
    public record SettingCase(String name, boolean affectsInstance, List<Object> values) {}

    /**
     * No setting changes the created instance, so all instances are interchangeable and the key is a
     * constant. Asserts only that identical configurations share; to prove a specific setting is
     * disregarded, use {@link #settings()} with {@link FactorySettings#ignored}.
     */
    protected static FactorySettings alwaysShares() {
        return new FactorySettings(false, Map.of());
    }

    /** Key is identity or by-name, so the factory never shares. */
    protected static FactorySettings neverShares() {
        return new FactorySettings(true, Map.of());
    }

    /** Settings follow via {@link FactorySettings#affects} / {@link FactorySettings#ignored}. */
    protected static FactorySettings settings() {
        return new FactorySettings(false, Map.of());
    }

    /** As {@link #settings()}, with the settings the factory needs in order to build at all. */
    protected static FactorySettings settings(Map<String, Object> base) {
        return new FactorySettings(false, base);
    }

    /** Per-kind declarations, by registered name. Override in each plugin's factory test. */
    protected Map<String, FactorySettings> tokenFilterSettings() {
        return Map.of();
    }

    protected Map<String, FactorySettings> tokenizerSettings() {
        return Map.of();
    }

    protected Map<String, FactorySettings> charFilterSettings() {
        return Map.of();
    }

    protected Map<String, FactorySettings> analyzerSettings() {
        return Map.of();
    }

    /**
     * Factories permanently outside the contract: identity-keyed ones (covered generically by
     * {@code FactorySharingKeyTests}), ones needing resources this harness cannot supply, and ones
     * with a dedicated test. Give each entry a reason.
     */
    protected Set<FactoryRef> factorySettingsExemptions() {
        return Set.of();
    }

    /**
     * Declarations still to be written — a shrinking migration list, not a judgement that coverage is
     * unnecessary (that is {@link #factorySettingsExemptions()}). Lets a module too large to convert
     * at once keep the gate live over the part already declared. Removed once empty.
     */
    protected Set<FactoryRef> factorySettingsPending() {
        return Set.of();
    }

    public void testTokenFilterSettings() throws IOException {
        runSettingsContract(ComponentKind.TOKEN_FILTER, plugin.getTokenFilters(), tokenFilterSettings());
    }

    public void testTokenizerSettings() throws IOException {
        runSettingsContract(ComponentKind.TOKENIZER, plugin.getTokenizers(), tokenizerSettings());
    }

    public void testCharFilterSettings() throws IOException {
        runSettingsContract(ComponentKind.CHAR_FILTER, plugin.getCharFilters(), charFilterSettings());
    }

    public void testAnalyzerSettings() throws IOException {
        runSettingsContract(ComponentKind.ANALYZER, plugin.getAnalyzers(), analyzerSettings());
    }

    /** Runs the completeness gate and the per-setting propagation assertions for one component kind. */
    private void runSettingsContract(
        ComponentKind kind,
        Map<String, ? extends AnalysisProvider<?>> registered,
        Map<String, FactorySettings> declarations
    ) throws IOException {
        if (registered.isEmpty()) {
            return;
        }
        // A subclass that has not started declaring this kind is skipped with a notice rather than
        // failed. Adding one declaration arms the gate for all of them.
        if (declarations.isEmpty()) {
            logger.warn(
                "{}: no factory settings declared in {} for {} registered factories — coverage PENDING",
                kind,
                getClass().getSimpleName(),
                registered.size()
            );
            return;
        }
        // Enforced regardless of the feature flag, so classifying a new factory is always required.
        Set<String> unclassified = new TreeSet<>(registered.keySet());
        unclassified.removeAll(declarations.keySet());
        unclassified.removeAll(namesIn(kind, factorySettingsExemptions()));
        unclassified.removeAll(namesIn(kind, factorySettingsPending()));
        assertTrue(
            kind
                + " factories missing a settings declaration (declare them in "
                + getClass().getSimpleName()
                + ", or list them in factorySettingsExemptions() with a reason / factorySettingsPending() while converting): "
                + unclassified,
            unclassified.isEmpty()
        );

        assumeTrue(
            "analyzer sharing feature flag disabled (release build); instance propagation is not observable",
            AnalysisRegistry.SHARED_ANALYZERS_FEATURE_FLAG.isEnabled()
        );

        AnalysisRegistry registry = buildSharingRegistry();
        List<IndexAnalyzers> tracked = new ArrayList<>();
        try {
            for (String type : registered.keySet()) {
                FactorySettings decl = declarations.get(type);
                if (decl == null) {
                    continue; // exempted or pending
                }
                // The per-index NamedAnalyzer wrapper is allocated per build and carries that index's
                // local name, so compare the wrapped analyzer rather than wrapper identity.
                Analyzer reference = build(registry, chainSettings(kind, type, decl.base), tracked).get("a").analyzer();
                Analyzer again = build(registry, chainSettings(kind, type, decl.base), tracked).get("a").analyzer();
                if (decl.neverShares) {
                    assertNotSame(kind + " [" + type + "] is declared neverShares but two identical builds shared", reference, again);
                    continue;
                }
                assertSame(kind + " [" + type + "] two identical configurations must produce the same shared instance", reference, again);
                for (SettingCase setting : decl.settings) {
                    for (Object value : setting.values()) {
                        Map<String, Object> varied = new LinkedHashMap<>(decl.base);
                        varied.put(setting.name(), value);
                        Analyzer other = build(registry, chainSettings(kind, type, varied), tracked).get("a").analyzer();
                        String where = kind + " [" + type + "] setting [" + setting.name() + "]=" + value;
                        if (setting.affectsInstance()) {
                            assertNotSame(
                                where + " must produce a distinct instance; it is not folded into sharingKey()",
                                reference,
                                other
                            );
                        } else {
                            assertSame(where + " is declared ignored and must not change the instance, but it did", reference, other);
                        }
                    }
                }
            }
        } finally {
            IOUtils.close(tracked);
            registry.assertNoCachedEntries();
            registry.close();
        }
    }

    private static Set<String> namesIn(ComponentKind kind, Set<FactoryRef> refs) {
        return refs.stream().filter(ref -> ref.kind() == kind).map(FactoryRef::name).collect(Collectors.toSet());
    }

    private AnalysisRegistry buildSharingRegistry() throws IOException {
        Settings node = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir()).build();
        return new AnalysisModule(TestEnvironment.newEnvironment(node), List.of(plugin), new StablePluginsRegistry()).getAnalysisRegistry();
    }

    private static IndexAnalyzers build(AnalysisRegistry registry, Settings analysis, List<IndexAnalyzers> tracked) throws IOException {
        Settings s = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            // Room for the larger gram/shingle spans some probes use.
            .put("index.max_ngram_diff", 10)
            .put("index.max_shingle_diff", 10)
            .put(analysis)
            .build();
        IndexAnalyzers ia = registry.build(IndexService.IndexCreationContext.CREATE_INDEX, IndexSettingsModule.newIndexSettings("test", s));
        tracked.add(ia);
        return ia;
    }

    /** Wires one component into an analyzer named {@code a}; filter chains use the standard tokenizer. */
    private static Settings chainSettings(ComponentKind kind, String type, Map<String, Object> componentSettings) {
        Settings.Builder b = Settings.builder();
        switch (kind) {
            case TOKEN_FILTER -> {
                b.put("index.analysis.analyzer.a.tokenizer", "standard");
                b.putList("index.analysis.analyzer.a.filter", "f");
                b.put("index.analysis.filter.f.type", type);
                applyComponentSettings(b, "index.analysis.filter.f.", componentSettings);
            }
            case TOKENIZER -> {
                b.put("index.analysis.analyzer.a.tokenizer", "t");
                b.put("index.analysis.tokenizer.t.type", type);
                applyComponentSettings(b, "index.analysis.tokenizer.t.", componentSettings);
            }
            case CHAR_FILTER -> {
                b.put("index.analysis.analyzer.a.tokenizer", "standard");
                b.putList("index.analysis.analyzer.a.char_filter", "c");
                b.put("index.analysis.char_filter.c.type", type);
                applyComponentSettings(b, "index.analysis.char_filter.c.", componentSettings);
            }
            case ANALYZER -> {
                b.put("index.analysis.analyzer.a.type", type);
                applyComponentSettings(b, "index.analysis.analyzer.a.", componentSettings);
            }
        }
        return b.build();
    }

    @SuppressWarnings("unchecked")
    private static void applyComponentSettings(Settings.Builder b, String prefix, Map<String, Object> componentSettings) {
        for (Map.Entry<String, Object> e : componentSettings.entrySet()) {
            if (e.getValue() instanceof List<?> list) {
                b.putList(prefix + e.getKey(), (List<String>) list);
            } else {
                b.put(prefix + e.getKey(), e.getValue().toString());
            }
        }
    }

    /**
     * Marker class for components that have moved to the analysis-common modules. This will be
     * removed when the module is complete and these analysis components aren't available to core.
     */
    protected static final class MovedToAnalysisCommon {
        private MovedToAnalysisCommon() {}
    }
}
