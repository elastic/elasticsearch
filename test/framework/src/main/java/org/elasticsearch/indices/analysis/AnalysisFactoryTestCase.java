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
        entry("romaniannormalization", Void.class)
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

    // ------------------------------------------------------------------------------------------------
    // Analyzer-sharing contract.
    //
    // Indices with identical analysis recipes share a single cached NamedAnalyzer (see
    // AnalysisRegistry). That is only safe if every factory folds all behavior-affecting settings
    // into its sharingKey(): if a factory reads a setting but omits it from the key, two indices
    // differing only in that setting collapse onto one shared analyzer and silently tokenize one
    // index's data with the other's recipe.
    //
    // Rather than rely on whether a random input happens to surface a behavioral difference, each
    // subclass declares, per factory it registers, the settings that distinguish the sharing key.
    // The base then verifies deterministically:
    // - two builds from identical settings share one instance (the dedup precondition);
    // - each declared distinguishing setting lands on a DIFFERENT instance (the key changed);
    // - identity-keyed factories never share, even for identical settings.
    // A completeness gate fails the build if a registered factory is neither declared nor exempted,
    // so a newly added factory must be classified. Because this lives on the base every plugin's
    // factory test already extends, the obligation travels with code people already touch.
    // ------------------------------------------------------------------------------------------------

    /** Component slot a factory occupies, used to wire it into a single-component analyzer chain. */
    public enum ComponentKind {
        TOKEN_FILTER,
        TOKENIZER,
        CHAR_FILTER,
        ANALYZER
    }

    /**
     * Declares the settings a factory supports and whether each one propagates to the created
     * instance. Build it with {@link #stateless()} (no setting changes the instance) or
     * {@link #settings()} and then describe each setting with {@link FactorySettings#affects}
     * (changing it must produce a distinct instance) or {@link FactorySettings#ignored} (it is read
     * but must NOT change the instance — e.g. a stop-word case flag on an analyzer that lower-cases
     * first). The test builds a reference instance from the base settings and, for each declared
     * setting, rebuilds with that setting changed and asserts the created instance was / was not a
     * distinct one.
     *
     * <p>Propagation is observed through the node-level analyzer cache: a setting that the factory
     * folds into its {@code sharingKey()} yields a separate cached instance, one that does not yields
     * the same shared instance. {@code base} carries any settings required just to build the factory;
     * setting values are scalars or {@code List<String>} and are merged on top of {@code base}.
     */
    public static final class FactorySettings {
        final boolean identityKeyed;
        final Map<String, Object> base;
        final List<SettingCase> settings = new ArrayList<>();

        private FactorySettings(boolean identityKeyed, Map<String, Object> base) {
            this.identityKeyed = identityKeyed;
            this.base = base;
        }

        /** A setting whose change MUST produce a distinct instance; each value is tested against the base. */
        public FactorySettings affects(String name, Object... values) {
            settings.add(new SettingCase(name, true, List.of(values)));
            return this;
        }

        /** A setting that is read but MUST NOT change the created instance (it does not propagate to identity). */
        public FactorySettings ignored(String name, Object... values) {
            settings.add(new SettingCase(name, false, List.of(values)));
            return this;
        }
    }

    /** One configuration setting and whether changing it should produce a distinct instance. */
    public record SettingCase(String name, boolean affectsInstance, List<Object> values) {}

    /** A factory with no settings that change the created instance. */
    protected static FactorySettings stateless() {
        return new FactorySettings(false, Map.of());
    }

    /**
     * A factory whose sharing key is identity / by-name (e.g. it wraps an opaque Lucene object with
     * no structural equality, or references other filters by name). It never shares, so no setting
     * propagation is meaningful; the identity mechanism itself is tested generically in
     * {@code FactorySharingKeyTests}. This is purely a per-kind classification so the completeness
     * gate passes — the factory is not built or asserted here.
     */
    protected static FactorySettings identity() {
        return new FactorySettings(true, Map.of());
    }

    /** A factory whose settings are described via {@link FactorySettings#affects}/{@code ignored}. */
    protected static FactorySettings settings() {
        return new FactorySettings(false, Map.of());
    }

    /** As {@link #settings()} but with the base settings the factory needs in order to build. */
    protected static FactorySettings settings(Map<String, Object> base) {
        return new FactorySettings(false, base);
    }

    /** Per-kind settings declarations. Override in each plugin's factory test to cover its factories. */
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
     * Factories not exercised by the settings contract here. Two kinds belong here, with a comment
     * saying which: (1) factories whose key is identity / by-name so no setting propagates to a
     * distinct instance — the identity mechanism itself is tested generically in
     * {@code FactorySharingKeyTests}, so it need not be re-tested per factory; (2) factories needing
     * resources this lightweight harness cannot supply (a hunspell dictionary, a hyphenation file) or
     * covered by a dedicated test (synonyms). Listed by registered name; the completeness gate accepts
     * these as classified.
     */
    protected Set<String> factorySettingsExemptions() {
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

    private void runSettingsContract(
        ComponentKind kind,
        Map<String, ? extends AnalysisProvider<?>> registered,
        Map<String, FactorySettings> declarations
    ) throws IOException {
        if (registered.isEmpty()) {
            return;
        }
        // Interim: a subclass that has not started declaring settings for this kind is skipped with a
        // notice rather than failed, so plugin factory tests stay green until filled in. The moment a
        // single declaration is added, the completeness gate enforces full coverage.
        if (declarations.isEmpty()) {
            logger.warn(
                "{}: no factory settings declared in {} for {} registered factories — coverage PENDING",
                kind,
                getClass().getSimpleName(),
                registered.size()
            );
            return;
        }
        // Completeness: every registered factory must be declared or explicitly exempted. Runs
        // regardless of the feature flag so the classification obligation is always enforced.
        Set<String> unclassified = new TreeSet<>(registered.keySet());
        unclassified.removeAll(declarations.keySet());
        unclassified.removeAll(factorySettingsExemptions());
        assertTrue(
            kind
                + " factories missing a settings declaration (declare them in "
                + getClass().getSimpleName()
                + ", or list in factorySettingsExemptions() with a reason): "
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
            for (Map.Entry<String, ? extends AnalysisProvider<?>> e : registered.entrySet()) {
                String type = e.getKey();
                if (factorySettingsExemptions().contains(type)) {
                    continue;
                }
                FactorySettings decl = declarations.get(type); // non-null by the completeness check
                if (decl.identityKeyed) {
                    // Identity / by-name key: never shares, no setting propagation to assert. The
                    // mechanism is covered once in FactorySharingKeyTests; here it is just classified.
                    continue;
                }
                // Sharing deduplicates the underlying analyzer instance; the per-index NamedAnalyzer
                // wrapper is allocated per build and carries that index's local name, so compare the
                // wrapped (shared) analyzer rather than wrapper identity.
                Analyzer reference = build(registry, chainSettings(kind, type, decl.base), tracked).get("a").analyzer();
                Analyzer referenceAgain = build(registry, chainSettings(kind, type, decl.base), tracked).get("a").analyzer();
                assertSame(
                    kind + " [" + type + "] two identical configurations must produce the same shared instance",
                    reference,
                    referenceAgain
                );
                for (SettingCase setting : decl.settings) {
                    for (Object value : setting.values()) {
                        Map<String, Object> varied = new LinkedHashMap<>(decl.base);
                        varied.put(setting.name(), value);
                        Analyzer other = build(registry, chainSettings(kind, type, varied), tracked).get("a").analyzer();
                        if (setting.affectsInstance()) {
                            assertNotSame(
                                kind
                                    + " ["
                                    + type
                                    + "] setting ["
                                    + setting.name()
                                    + "]="
                                    + value
                                    + " must produce a distinct instance but did not — the factory does not propagate it to sharingKey()",
                                reference,
                                other
                            );
                        } else {
                            assertSame(
                                kind
                                    + " ["
                                    + type
                                    + "] setting ["
                                    + setting.name()
                                    + "]="
                                    + value
                                    + " is declared ignored and must NOT change the instance, but it did",
                                reference,
                                other
                            );
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

    private AnalysisRegistry buildSharingRegistry() throws IOException {
        Settings node = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir()).build();
        return new AnalysisModule(TestEnvironment.newEnvironment(node), List.of(plugin), new StablePluginsRegistry()).getAnalysisRegistry();
    }

    private static IndexAnalyzers build(AnalysisRegistry registry, Settings analysis, List<IndexAnalyzers> tracked) throws IOException {
        Settings s = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            // Allow the larger gram/shingle spans some distinguishing probes use.
            .put("index.max_ngram_diff", 10)
            .put("index.max_shingle_diff", 10)
            .put(analysis)
            .build();
        IndexAnalyzers ia = registry.build(IndexService.IndexCreationContext.CREATE_INDEX, IndexSettingsModule.newIndexSettings("test", s));
        tracked.add(ia);
        return ia;
    }

    /** Wire a single component into an analyzer named {@code a}; token-filter/char-filter chains use the standard tokenizer. */
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
