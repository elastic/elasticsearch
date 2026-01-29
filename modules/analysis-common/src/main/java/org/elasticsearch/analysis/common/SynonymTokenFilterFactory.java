/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.synonym.SynonymFilter;
import org.apache.lucene.analysis.synonym.SynonymMap;
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Function;

public class SynonymTokenFilterFactory extends AbstractTokenFilterFactory {

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
                return new ReaderWithOrigin(new StringReader(sb.toString()), "'" + factory.name() + "' analyzer settings", INLINE);
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
                String synonymsSet = factory.settings.get(SynonymsSource.INDEX.getSettingName(), null);
                // provide empty synonyms on index creation and index metadata checks to ensure that we
                // don't block a master thread
                ReaderWithOrigin reader;
                if (context != IndexCreationContext.RELOAD_ANALYZERS) {
                    reader = new ReaderWithOrigin(
                        new StringReader(""),
                        "fake empty [" + synonymsSet + "] synonyms_set in .synonyms index",
                        INDEX,
                        synonymsSet
                    );
                } else {
                    reader = new ReaderWithOrigin(
                        Analysis.getReaderFromIndex(synonymsSet, factory.synonymsManagementAPIService, factory.lenient),
                        "[" + synonymsSet + "] synonyms_set in .synonyms index",
                        INDEX,
                        synonymsSet
                    );
                }

                return reader;
            }
        },
        LOCAL_FILE("synonyms_path") {
            @Override
            public ReaderWithOrigin getRulesReader(SynonymTokenFilterFactory factory, IndexCreationContext context) {
                String synonymsPath = factory.settings.get(SynonymsSource.LOCAL_FILE.getSettingName(), null);
                return new ReaderWithOrigin(
                    // Pass the inline setting name because "_path" is appended by getReaderFromFile
                    Analysis.getReaderFromFile(factory.environment, synonymsPath, SynonymsSource.INLINE.getSettingName()),
                    synonymsPath,
                    LOCAL_FILE
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

    SynonymTokenFilterFactory(
        IndexSettings indexSettings,
        Environment env,
        String name,
        Settings settings,
        SynonymsManagementAPIService synonymsManagementAPIService
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
        final String name = name();
        return new TokenFilterFactory() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                return synonyms.fst == null ? tokenStream : new SynonymFilter(tokenStream, synonyms, false);
            }

            @Override
            public TokenFilterFactory getSynonymFilter() {
                // In order to allow chained synonym filters, we return IDENTITY here to
                // ensure that synonyms don't get applied to the synonym map itself,
                // which doesn't support stacked input tokens
                return IDENTITY_FILTER;
            }

            @Override
            public AnalysisMode getAnalysisMode() {
                return analysisMode;
            }

            @Override
            public String getResourceName() {
                return rulesReader.resource();
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
                parser = new ESWordnetSynonymParser(true, expand, lenient, analyzer);
                ((ESWordnetSynonymParser) parser).parse(rules.reader);
            } else {
                if (rules.synonymsSource == SynonymsSource.INDEX) {
                    long estimatedSize = estimateSynonymsMapSize(rules);
                    // TODO: Add estimated size to circuit breaker
                }

                parser = new ESSolrSynonymParser(true, expand, lenient, analyzer);
                ((ESSolrSynonymParser) parser).parse(rules.reader);
            }
            return parser.build();
        } catch (Exception e) {
            throw new IllegalArgumentException("failed to build synonyms from [" + rules.origin + "]", e);
        }
    }

    /**
     * <p>
     * Estimates the synonyms map size by summing up the size of all the terms in the provided rules. The size of each term is determined
     * by UTF8 encoding it and taking its length.
     * </p>
     * <p>
     * Note that this method does not deduplicate terms. Doing so would require storing a reference to each unique term, which may require
     * a significant amount of memory. Since the point of this method is to estimate the synonyms map size to avoid OOMs while building it,
     * low memory usage is prioritized in the implementation.
     * </p>
     *
     * @param rules
     * @return
     * @throws IOException
     */
    static long estimateSynonymsMapSize(ReaderWithOrigin rules) throws IOException {
        final Function<String[], Long> estimateSize = a -> {
            long estimatedSize = 0;
            for (String s : a) {
                estimatedSize += s.trim().getBytes(StandardCharsets.UTF_8).length;
            }

            return estimatedSize;
        };

        long totalEstimatedSize = 0;
        try {
            // Don't close the buffered reader because that also closes the underlying rules reader
            BufferedReader bufferedReader = new BufferedReader(rules.reader);
            String line = bufferedReader.readLine();
            while (line != null) {
                // Ignore empty lines and comments
                if (line.isEmpty() == false && line.startsWith("#") == false) {
                    String[] inputAndOutput = line.split("=>", 2);
                    totalEstimatedSize += estimateSize.apply(inputAndOutput[0].split(","));
                    if (inputAndOutput.length == 2) {
                        // Explicit synonym
                        totalEstimatedSize += estimateSize.apply(inputAndOutput[1].split(","));
                    }
                }

                line = bufferedReader.readLine();
            }
        } finally {
            rules.reader.reset();
        }

        return totalEstimatedSize;
    }

    record ReaderWithOrigin(Reader reader, String origin, SynonymsSource synonymsSource, String resource) {
        ReaderWithOrigin(Reader reader, String origin, SynonymsSource synonymsSource) {
            this(reader, origin, synonymsSource, null);
        }
    }
}
