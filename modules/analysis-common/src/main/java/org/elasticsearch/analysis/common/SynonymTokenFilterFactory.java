/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.analysis.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.synonym.SynonymFilter;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexService.IndexCreationContext;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;
import org.elasticsearch.index.analysis.Analysis;
import org.elasticsearch.index.analysis.AnalysisMode;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.index.analysis.CustomAnalyzer;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.synonyms.SynonymsManagementAPIService;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

public class SynonymTokenFilterFactory extends AbstractTokenFilterFactory {
    private static final Logger LOGGER = LogManager.getLogger(SynonymTokenFilterFactory.class);

    private static final SynonymMap EMPTY_SYNONYM_MAP = buildEmptySynonymMap();

    static final int MAX_SYNONYM_SETS_PER_FILTER = 100;

    protected enum SynonymsSource {
        INLINE("synonyms") {
            @Override
            public ReaderWithOrigin getRulesReader(SynonymTokenFilterFactory factory, IndexCreationContext context) {
                String fixedRules = factory.fixedRules();
                String rules = fixedRules != null ? fixedRules : factory.readInlineRules();
                return new ReaderWithOrigin(new StringReader(rules), "'" + factory.name() + "' analyzer settings");
            }
        },
        INDEX("synonyms_set") {
            @Override
            public ReaderWithOrigin getRulesReader(SynonymTokenFilterFactory factory, IndexCreationContext context) {
                if (factory.analysisMode != AnalysisMode.SEARCH_TIME) {
                    throw new IllegalArgumentException(
                        "Can't apply ["
                            + SynonymsSource.INDEX.getSettingName()
                            + "]! Loading synonyms from index is supported only for search time synonyms!"
                    );
                }
                Set<String> synonymsSets = new LinkedHashSet<>();
                Set<String> duplicates = new LinkedHashSet<>();
                for (String s : factory.settings.getAsList(SynonymsSource.INDEX.getSettingName())) {
                    if (synonymsSets.add(s) == false) {
                        duplicates.add(s);
                    }
                }
                if (duplicates.isEmpty() == false) {
                    LOGGER.warn(
                        "Duplicate synonym set names in [{}] for filter [{}]; duplicates will be ignored: {}",
                        SynonymsSource.INDEX.getSettingName(),
                        factory.name(),
                        duplicates
                    );
                }
                if (synonymsSets.isEmpty()) {
                    return new ReaderWithOrigin(new StringReader(""), "empty synonyms_set " + synonymsSets, synonymsSets);
                }

                // Reject multi-set configs on partially-upgraded clusters at index creation time only.
                // Do NOT check during METADATA_VERIFICATION (recovery) or RELOAD_ANALYZERS — throwing
                // there would prevent shards from loading.
                if (synonymsSets.size() > 1 && context == IndexCreationContext.CREATE_INDEX) {
                    factory.synonymsManagementAPIService.checkClusterSupportsMultipleSynonymSets();
                }
                if (synonymsSets.size() > MAX_SYNONYM_SETS_PER_FILTER) {
                    throw new IllegalArgumentException(
                        "At most "
                            + MAX_SYNONYM_SETS_PER_FILTER
                            + " synonym sets may be specified in ["
                            + SynonymsSource.INDEX.getSettingName()
                            + "]"
                    );
                }
                // provide empty synonyms on index creation and index metadata checks to ensure that we
                // don't block a master thread
                if (context != IndexCreationContext.RELOAD_ANALYZERS) {
                    return new ReaderWithOrigin(
                        new StringReader(""),
                        "fake empty " + synonymsSets + " synonyms_set in .synonyms index",
                        synonymsSets
                    );
                }

                Analysis.IndexRules rules = Analysis.getRulesFromIndex(synonymsSets, factory.synonymsManagementAPIService, factory.lenient);
                return new ReaderWithOrigin(
                    rules.reader(),
                    synonymsSets + " synonyms_sets in .synonyms index",
                    synonymsSets,
                    rules.degraded()
                );
            }
        },
        LOCAL_FILE("synonyms_path") {
            @Override
            public ReaderWithOrigin getRulesReader(SynonymTokenFilterFactory factory, IndexCreationContext context) {
                String synonymsPath = factory.settings.get(SynonymsSource.LOCAL_FILE.getSettingName(), null);
                String fixedRules = factory.fixedRules();
                if (fixedRules != null) {
                    return new ReaderWithOrigin(new StringReader(fixedRules), synonymsPath);
                }
                return new ReaderWithOrigin(
                    // Pass the inline setting name because "_path" is appended by getReaderFromFile
                    Analysis.getReaderFromFile(factory.environment, synonymsPath, SynonymsSource.INLINE.getSettingName()),
                    synonymsPath
                );
            }
        };

        private final String settingName;

        SynonymsSource(String settingName) {
            this.settingName = settingName;
        }

        public abstract ReaderWithOrigin getRulesReader(SynonymTokenFilterFactory factory, IndexCreationContext context);

        public String getSettingName() {
            return settingName;
        }

        public static SynonymsSource fromSettings(Settings settings) {
            SynonymsSource synonymsSource;
            if (settings.hasValue(SynonymsSource.INLINE.getSettingName())) {
                synonymsSource = SynonymsSource.INLINE;
            } else if (settings.hasValue(SynonymsSource.INDEX.getSettingName())) {
                synonymsSource = SynonymsSource.INDEX;
            } else if (settings.hasValue(SynonymsSource.LOCAL_FILE.getSettingName())) {
                synonymsSource = SynonymsSource.LOCAL_FILE;
            } else {
                throw new IllegalArgumentException(
                    "synonym requires either `"
                        + SynonymsSource.INLINE.getSettingName()
                        + "`, `"
                        + SynonymsSource.INDEX.getSettingName()
                        + "` or `"
                        + SynonymsSource.LOCAL_FILE.getSettingName()
                        + "` to be configured"
                );
            }

            return synonymsSource;
        }
    }

    private final String format;
    private final boolean expand;
    private final boolean lenient;
    protected final Settings settings;
    protected final Environment environment;
    protected final AnalysisMode analysisMode;
    private final SynonymsManagementAPIService synonymsManagementAPIService;
    protected final SynonymsSource synonymsSource;
    protected final CircuitBreaker circuitBreaker;
    // Read on first use by sharingKey() or the chain build, so a filter no analyzer is built from is never read.
    private boolean fixedRulesRead;
    private String fixedRules;
    private Object sharingKey;

    SynonymTokenFilterFactory(
        IndexSettings indexSettings,
        Environment env,
        String name,
        Settings settings,
        SynonymsManagementAPIService synonymsManagementAPIService,
        CircuitBreaker circuitBreaker
    ) {
        super(name);
        this.settings = settings;

        this.synonymsSource = SynonymsSource.fromSettings(settings);
        this.expand = settings.getAsBoolean("expand", true);
        this.format = settings.get("format", "");
        boolean updateable = settings.getAsBoolean("updateable", false);
        this.lenient = settings.getAsBoolean(
            "lenient",
            indexSettings.getIndexVersionCreated().onOrAfter(IndexVersions.LENIENT_UPDATEABLE_SYNONYMS) && updateable
        );
        this.analysisMode = updateable ? AnalysisMode.SEARCH_TIME : AnalysisMode.ALL;
        this.environment = env;
        this.synonymsManagementAPIService = synonymsManagementAPIService;
        this.circuitBreaker = circuitBreaker;
    }

    private String readInlineRules() {
        // Reads synonyms_path instead when it is also set.
        StringBuilder sb = new StringBuilder();
        for (String line : Analysis.getWordList(environment, settings, SynonymsSource.INLINE.getSettingName())) {
            sb.append(line).append(System.lineSeparator());
        }
        return sb.toString();
    }

    /**
     * The rule text of a source that cannot change while this factory exists (inline rules, or a non-updateable file), read once
     * so that the sharing key and the {@link SynonymMap} come from the same text. {@code null} for rules read at load time, and
     * when reading fails: building the chain reads the rules again and reports the failure.
     */
    private synchronized String fixedRules() {
        if (fixedRulesRead == false) {
            fixedRules = readFixedRules();
            fixedRulesRead = true;
        }
        return fixedRules;
    }

    private String readFixedRules() {
        boolean fromFile = settings.get(SynonymsSource.LOCAL_FILE.getSettingName()) != null;
        if (synonymsSource == SynonymsSource.INDEX || (fromFile && analysisMode == AnalysisMode.SEARCH_TIME)) {
            return null;
        }
        try {
            return switch (synonymsSource) {
                case INLINE -> readInlineRules();
                case LOCAL_FILE -> {
                    String path = settings.get(SynonymsSource.LOCAL_FILE.getSettingName());
                    try (Reader reader = Analysis.getReaderFromFile(environment, path, SynonymsSource.INLINE.getSettingName())) {
                        StringWriter text = new StringWriter();
                        reader.transferTo(text);
                        yield text.toString();
                    }
                }
                case INDEX -> throw new AssertionError("synonyms_set rules are read at load time");
            };
        } catch (IOException | IllegalArgumentException e) {
            return null;
        }
    }

    /**
     * Keys on the inputs that determine which rules this factory loads. The chain they are parsed through is covered by
     * the analyzer key.
     * <ul>
     *   <li>{@code synonyms} and non-updateable {@code synonyms_path}: a SHA-256 of the rule text that is parsed. When both
     *       are set the file's rules are used, so the file is what is hashed.</li>
     *   <li>{@code synonyms_set}: the set names. These filters are always updateable, and a synonyms API
     *       update reloads the one shared instance for every sharer. An update made with
     *       {@code refresh=false} is not seen by an index that joins an already-loaded instance until the
     *       next reload.</li>
     *   <li>updateable {@code synonyms_path}: never shares. Nothing reloads the file on change, and a shared
     *       instance loads it only once per node, so an index joining it could see rules the file no
     *       longer has.</li>
     * </ul>
     */
    private Object buildSharingKey() {
        Object rules = switch (synonymsSource) {
            case INLINE, LOCAL_FILE -> {
                String fixedRules = fixedRules();
                yield fixedRules == null ? null : Analysis.sha256Hex(fixedRules);
            }
            // Rules are fetched with one query sorted by rule id, so the order the sets are named in does
            // not affect the loaded rules.
            case INDEX -> settings.getAsList(SynonymsSource.INDEX.getSettingName()).stream().distinct().sorted().toList();
        };
        if (rules == null) {
            return this;
        }
        return new Key(synonymsSource, "wordnet".equalsIgnoreCase(format), expand, lenient, analysisMode, rules);
    }

    private record Key(SynonymsSource source, boolean wordnet, boolean expand, boolean lenient, AnalysisMode mode, Object rules) {}

    @Override
    public synchronized Object sharingKey() {
        if (sharingKey == null) {
            sharingKey = buildSharingKey();
        }
        return sharingKey;
    }

    @Override
    public AnalysisMode getAnalysisMode() {
        return this.analysisMode;
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        throw new IllegalStateException("Call createPerAnalyzerSynonymFactory to specialize this factory for an analysis chain first");
    }

    @Override
    public TokenFilterFactory getChainAwareTokenFilterFactory(
        IndexCreationContext context,
        TokenizerFactory tokenizer,
        List<CharFilterFactory> charFilters,
        List<TokenFilterFactory> previousTokenFilters,
        Function<String, TokenFilterFactory> allFilters
    ) {
        final Analyzer analyzer = buildSynonymAnalyzer(tokenizer, charFilters, previousTokenFilters);
        ReaderWithOrigin rulesReader = synonymsSource.getRulesReader(this, context);
        final SynonymMap synonyms = buildSynonyms(analyzer, rulesReader);
        return buildChainedFactory(name(), synonyms, analysisMode, rulesReader, ts -> new SynonymFilter(ts, synonyms, false));
    }

    /**
     * Static so the returned factory does not hold a reference to the outer instance,
     * allowing the outer factory's raw synonym rule strings to be GC'd after {@link SynonymMap} construction.
     */
    protected static TokenFilterFactory buildChainedFactory(
        String name,
        SynonymMap synonyms,
        AnalysisMode analysisMode,
        ReaderWithOrigin rules,
        Function<TokenStream, TokenStream> createFilter
    ) {
        Set<String> resourceNames = rules.resources();
        // buildSynonyms only returns EMPTY_SYNONYM_MAP when lenient swallowed a failure to build the rules.
        boolean degraded = rules.degraded() || synonyms == EMPTY_SYNONYM_MAP;
        return new TokenFilterFactory() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                return synonyms.fst == null ? tokenStream : createFilter.apply(tokenStream);
            }

            @Override
            public TokenFilterFactory getSynonymFilter() {
                // When building a synonym filter, we must prevent previous synonym filters in the chain
                // from being active, as this would cause recursive synonym expansion during the building phase.
                //
                // Without this fix, when chaining multiple synonym filters (e.g., synonym_A → synonym_B → synonym_C),
                // building synonym_C would use an analyzer containing active synonym_A and synonym_B filters.
                // This causes:
                // 1. Recursive synonym expansion when parsing synonym rules (e.g., synonyms are expanded via previous filters)
                // 2. Each SynonymMap inflates since it applies all previous synonym rules again
                // 3. Triggering O(n²) operations in SynonymGraphFilter.bufferOutputTokens()
                // 4. Massive memory allocation during analyzer reload → OutOfMemoryError
                //
                // This matches the behavior of SynonymTokenFilterFactory and prevents OOM with chained
                // synonym filters (critical for users with many chained synonym sets).
                return IDENTITY_FILTER;
            }

            @Override
            public AnalysisMode getAnalysisMode() {
                return analysisMode;
            }

            @Override
            public Set<String> getResourceNames() {
                return resourceNames;
            }

            @Override
            public boolean isDegraded() {
                return degraded;
            }
        };
    }

    static Analyzer buildSynonymAnalyzer(
        TokenizerFactory tokenizer,
        List<CharFilterFactory> charFilters,
        List<TokenFilterFactory> tokenFilters
    ) {
        return new CustomAnalyzer(
            tokenizer,
            charFilters.toArray(new CharFilterFactory[0]),
            tokenFilters.stream().map(TokenFilterFactory::getSynonymFilter).toArray(TokenFilterFactory[]::new)
        );
    }

    SynonymMap buildSynonyms(Analyzer analyzer, ReaderWithOrigin rules) {
        try {
            SynonymMap.Builder parser;
            if ("wordnet".equalsIgnoreCase(format)) {
                parser = new ESWordnetSynonymParser(true, expand, lenient, analyzer, circuitBreaker);
                ((ESWordnetSynonymParser) parser).parse(rules.reader);
            } else {
                parser = new ESSolrSynonymParser(true, expand, lenient, analyzer, circuitBreaker);
                ((ESSolrSynonymParser) parser).parse(rules.reader);
            }
            return parser.build();
        } catch (Exception e) {
            String message = "failed to build synonyms from [" + rules.origin + "]";
            if (lenient && e instanceof CircuitBreakingException) {
                LOGGER.error(message + ". Using an empty synonyms map in its place because lenient=true.", e);
                return EMPTY_SYNONYM_MAP;
            }

            throw new IllegalArgumentException(message, e);
        }
    }

    /** {@code degraded}: lenient loading skipped a missing set or swallowed a failure, so {@code reader} lacks some of the rules. */
    record ReaderWithOrigin(Reader reader, String origin, Set<String> resources, boolean degraded) {
        ReaderWithOrigin(Reader reader, String origin, Set<String> resources) {
            this(reader, origin, resources, false);
        }

        ReaderWithOrigin(Reader reader, String origin) {
            this(reader, origin, Set.of());
        }
    }

    private static SynonymMap buildEmptySynonymMap() {
        try {
            return new SynonymMap.Builder().build();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
