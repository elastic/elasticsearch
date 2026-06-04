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
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
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
                List<String> rulesList = Analysis.getWordList(
                    factory.environment,
                    factory.settings,
                    SynonymsSource.INLINE.getSettingName()
                );
                StringBuilder sb = new StringBuilder();
                for (String line : rulesList) {
                    sb.append(line).append(System.lineSeparator());
                }
                return new ReaderWithOrigin(new StringReader(sb.toString()), "'" + factory.name() + "' analyzer settings");
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

                return new ReaderWithOrigin(
                    Analysis.getReaderFromIndex(synonymsSets, factory.synonymsManagementAPIService, factory.lenient),
                    synonymsSets + " synonyms_sets in .synonyms index",
                    synonymsSets
                );
            }
        },
        LOCAL_FILE("synonyms_path") {
            @Override
            public ReaderWithOrigin getRulesReader(SynonymTokenFilterFactory factory, IndexCreationContext context) {
                String synonymsPath = factory.settings.get(SynonymsSource.LOCAL_FILE.getSettingName(), null);
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
    private final Object sharingKey;

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
        this.sharingKey = buildSharingKey();
    }

    /**
     * Snapshot the resource identity at construction time so {@link #sharingKey()} returns a stable
     * value that encodes "which rules will this factory load".
     *
     * <p>Updateable (search-time) synonyms are identified by their source inputs alone — set names
     * or file path — and deliberately NOT by a content version. There is a single shared live
     * instance per recipe on the node, and {@code _reload_search_analyzers} refreshes that one
     * instance in place for every sharer. This is safe because these filters are search-time only
     * (the mapping layer rejects them as the index-time analyzer) and the underlying resource is
     * cluster-global, so reload can only change query-time tokenization, never indexed data.
     *
     * <p>Non-updateable file synonyms are the exception: they are applied at index time and baked
     * into the segments, so two indices built from different file contents must NOT share. Those
     * keep a content stamp (mtime+size) in the key.
     */
    private Object buildSharingKey() {
        Object resourceStamp = switch (synonymsSource) {
            // INLINE rules live in settings; capture them explicitly here so the key owns every
            // behavior-affecting input rather than leaning on the raw Settings blob. The raw list
            // is sufficient: identical inline rules build an identical SynonymMap. Settings#getAsList
            // returns an immutable List<String> with stable equals/hashCode.
            case INLINE -> settings.getAsList(SynonymsSource.INLINE.getSettingName());
            // synonyms_set is always search-time (rejected otherwise), hence updateable: identify the
            // recipe by its set names. Reload re-reads the .synonyms index for the shared instance.
            case INDEX -> settings.getAsList(SynonymsSource.INDEX.getSettingName()).stream().sorted().toList();
            // synonyms_path: updateable (search-time) files share by path and refresh on reload.
            // Non-updateable files are baked in at index time, so stamp their content (mtime+size)
            // to keep indices built from different file contents on distinct cache entries.
            case LOCAL_FILE -> {
                String path = settings.get(SynonymsSource.LOCAL_FILE.getSettingName());
                if (path == null) {
                    yield null;
                }
                if (analysisMode == AnalysisMode.SEARCH_TIME) {
                    yield path;
                }
                Path resolved = environment.configDir().resolve(path);
                try {
                    yield new FileStamp(path, Files.getLastModifiedTime(resolved).toMillis(), Files.size(resolved));
                } catch (IOException e) {
                    yield new FileStamp(path, -1, -1);
                }
            }
        };
        // No need to fold the factory class in here: AnalysisRegistry keys every chain slot by
        // (factory class, sharingKey()), so SynonymGraphTokenFilterFactory — which inherits this
        // method — can never collide with synonym even though they share this Key shape.
        return new Key(synonymsSource, format, expand, lenient, analysisMode, resourceStamp);
    }

    private record FileStamp(String path, long mtime, long size) {}

    private record Key(SynonymsSource source, String format, boolean expand, boolean lenient, AnalysisMode mode, Object resourceStamp) {}

    @Override
    public Object sharingKey() {
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
        return buildChainedFactory(
            name(),
            sharingKey(),
            synonyms,
            analysisMode,
            rulesReader.resources(),
            ts -> new SynonymFilter(ts, synonyms, false)
        );
    }

    /**
     * Static so the returned factory does not hold a reference to the outer instance,
     * allowing the outer factory's raw synonym rule strings to be GC'd after {@link SynonymMap} construction.
     */
    protected static TokenFilterFactory buildChainedFactory(
        String name,
        Object outerSharingKey,
        SynonymMap synonyms,
        AnalysisMode analysisMode,
        Set<String> resourceNames,
        Function<TokenStream, TokenStream> createFilter
    ) {
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
            public Object sharingKey() {
                // Propagate the outer factory's sharing key. AnalyzerComponents stores chain-aware
                // wrapped factories (this anonymous class), so the cache must key on the same value
                // the outer factory reports — otherwise an analyzer wrapping its synonym filter
                // would never match the recipe computed from the un-wrapped factory chain.
                return outerSharingKey;
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

    record ReaderWithOrigin(Reader reader, String origin, Set<String> resources) {
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
