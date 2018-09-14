/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.categorization;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.analyze.TransportAnalyzeAction;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.index.analysis.CustomAnalyzer;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.xpack.core.ml.job.config.CategorizationAnalyzerConfig;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The categorization analyzer.
 *
 * Converts messages to lists of tokens that will be fed to the ML categorization algorithm.
 *
 * The code in {@link #makeAnalyzer} and the methods it calls is largely copied from {@link TransportAnalyzeAction}.
 * Unfortunately there is no easy way to reuse a subset of the <code>_analyze</code> action implementation, as the
 * logic required here is not quite identical to that of {@link TransportAnalyzeAction}, and the required code is
 * hard to partially reuse.
 * TODO: consider refactoring ES core to allow more reuse.
 */
public class CategorizationAnalyzer implements Closeable {

    private final Analyzer analyzer;
    private final boolean closeAnalyzer;

    public CategorizationAnalyzer(AnalysisRegistry analysisRegistry, Environment environment,
                                  CategorizationAnalyzerConfig categorizationAnalyzerConfig) throws IOException {

        Tuple<Analyzer, Boolean> tuple = makeAnalyzer(categorizationAnalyzerConfig, analysisRegistry, environment);
        analyzer = tuple.v1();
        closeAnalyzer = tuple.v2();
    }

    /**
     * Release resources held by the analyzer (unless it's global).
     */
    @Override
    public void close() {
        if (closeAnalyzer) {
            analyzer.close();
        }
    }

    /**
     * Given a field value, convert it to a list of tokens using the configured analyzer.
     */
    public List<String> tokenizeField(String fieldName, String fieldValue) {
        List<String> tokens = new ArrayList<>();
        try (TokenStream stream = analyzer.tokenStream(fieldName, fieldValue)) {
            stream.reset();
            CharTermAttribute term = stream.addAttribute(CharTermAttribute.class);
            while (stream.incrementToken()) {
                String token = term.toString();
                // Ignore empty tokens for categorization
                if (token.isEmpty() == false) {
                    tokens.add(term.toString());
                }
            }
            stream.end();
        } catch (IOException e) {
            throw new ElasticsearchException("Failed to analyze value [" + fieldValue + "] of field [" + fieldName + "]", e);
        }
        return tokens;
    }

    /**
     * Verify that the config builder will build a valid config.  This is not done as part of the basic build
     * because it verifies that the names of analyzers/tokenizers/filters referenced by the config are
     * known, and the validity of these names could change over time.  Additionally, it has to be done
     * server-side rather than client-side, as the client will not have loaded the appropriate analysis
     * modules/plugins.
     */
    public static void verifyConfigBuilder(CategorizationAnalyzerConfig.Builder configBuilder, AnalysisRegistry analysisRegistry,
                                           Environment environment) throws IOException {
        Tuple<Analyzer, Boolean> tuple = makeAnalyzer(configBuilder.build(), analysisRegistry, environment);
        if (tuple.v2()) {
            tuple.v1().close();
        }
    }

    /**
     * Convert a config to an {@link Analyzer}.  This may be a global analyzer or a newly created custom analyzer.
     * In the case of a global analyzer the caller must NOT close it when they have finished with it.  In the case of
     * a newly created custom analyzer the caller is responsible for closing it.
     * @return The first tuple member is the {@link Analyzer}; the second indicates whether the caller is responsible
     *         for closing it.
     */
    private static Tuple<Analyzer, Boolean> makeAnalyzer(CategorizationAnalyzerConfig config, AnalysisRegistry analysisRegistry,
                                                         Environment environment) throws IOException {
        String analyzer = config.getAnalyzer();
        if (analyzer != null) {
            Analyzer globalAnalyzer = analysisRegistry.getAnalyzer(analyzer);
            if (globalAnalyzer == null) {
                throw new IllegalArgumentException("Failed to find global analyzer [" + analyzer + "]");
            }
            return new Tuple<>(globalAnalyzer, Boolean.FALSE);
        } else {
            List<CharFilterFactory> charFilterFactoryList = parseCharFilterFactories(config, analysisRegistry, environment);

            Tuple<String, TokenizerFactory> tokenizerFactory = parseTokenizerFactory(config, analysisRegistry, environment);

            List<TokenFilterFactory> tokenFilterFactoryList = parseTokenFilterFactories(config, analysisRegistry, environment,
                tokenizerFactory, charFilterFactoryList);

            return new Tuple<>(new CustomAnalyzer(tokenizerFactory.v1(), tokenizerFactory.v2(),
                charFilterFactoryList.toArray(new CharFilterFactory[charFilterFactoryList.size()]),
                tokenFilterFactoryList.toArray(new TokenFilterFactory[tokenFilterFactoryList.size()])), Boolean.TRUE);
        }
    }


    /**
     * Get char filter factories for each configured char filter.  Each configuration
     * element can be the name of an out-of-the-box char filter, or a custom definition.
     */
    private static List<CharFilterFactory> parseCharFilterFactories(CategorizationAnalyzerConfig config, AnalysisRegistry analysisRegistry,
                                                                    Environment environment) throws IOException {
        List<CategorizationAnalyzerConfig.NameOrDefinition> charFilters = config.getCharFilters();
        final List<CharFilterFactory> charFilterFactoryList = new ArrayList<>();
        for (CategorizationAnalyzerConfig.NameOrDefinition charFilter : charFilters) {
            final CharFilterFactory charFilterFactory;
            if (charFilter.name != null) {
                AnalysisModule.AnalysisProvider<CharFilterFactory> charFilterFactoryFactory =
                    analysisRegistry.getCharFilterProvider(charFilter.name);
                if (charFilterFactoryFactory == null) {
                    throw new IllegalArgumentException("Failed to find global char filter under [" + charFilter.name + "]");
                }
                charFilterFactory = charFilterFactoryFactory.get(environment, charFilter.name);
            } else {
                String charFilterTypeName = charFilter.definition.get("type");
                if (charFilterTypeName == null) {
                    throw new IllegalArgumentException("Missing [type] setting for char filter: " + charFilter.definition);
                }
                AnalysisModule.AnalysisProvider<CharFilterFactory> charFilterFactoryFactory =
                    analysisRegistry.getCharFilterProvider(charFilterTypeName);
                if (charFilterFactoryFactory == null) {
                    throw new IllegalArgumentException("Failed to find global char filter under [" + charFilterTypeName + "]");
                }
                Settings settings = augmentSettings(charFilter.definition);
                // Need to set anonymous "name" of char_filter
                charFilterFactory = charFilterFactoryFactory.get(buildDummyIndexSettings(settings), environment, "_anonymous_charfilter",
                    settings);
            }
            if (charFilterFactory == null) {
                throw new IllegalArgumentException("Failed to find char filter [" + charFilter + "]");
            }
            charFilterFactoryList.add(charFilterFactory);
        }
        return charFilterFactoryList;
    }

    /**
     * Get the tokenizer factory for the configured tokenizer.  The configuration
     * can be the name of an out-of-the-box tokenizer, or a custom definition.
     */
    private static Tuple<String, TokenizerFactory> parseTokenizerFactory(CategorizationAnalyzerConfig config,
                                                                         AnalysisRegistry analysisRegistry, Environment environment)
        throws IOException {
        CategorizationAnalyzerConfig.NameOrDefinition tokenizer = config.getTokenizer();
        final String name;
        final TokenizerFactory tokenizerFactory;
        if (tokenizer.name != null) {
            name = tokenizer.name;
            AnalysisModule.AnalysisProvider<TokenizerFactory> tokenizerFactoryFactory = analysisRegistry.getTokenizerProvider(name);
            if (tokenizerFactoryFactory == null) {
                throw new IllegalArgumentException("Failed to find global tokenizer under [" + name + "]");
            }
            tokenizerFactory = tokenizerFactoryFactory.get(environment, name);
        } else {
            String tokenizerTypeName = tokenizer.definition.get("type");
            if (tokenizerTypeName == null) {
                throw new IllegalArgumentException("Missing [type] setting for tokenizer: " + tokenizer.definition);
            }
            AnalysisModule.AnalysisProvider<TokenizerFactory> tokenizerFactoryFactory =
                analysisRegistry.getTokenizerProvider(tokenizerTypeName);
            if (tokenizerFactoryFactory == null) {
                throw new IllegalArgumentException("Failed to find global tokenizer under [" + tokenizerTypeName + "]");
            }
            Settings settings = augmentSettings(tokenizer.definition);
            // Need to set anonymous "name" of tokenizer
            name = "_anonymous_tokenizer";
            tokenizerFactory = tokenizerFactoryFactory.get(buildDummyIndexSettings(settings), environment, name, settings);
        }
        return new Tuple<>(name, tokenizerFactory);
    }

    /**
     * Get token filter factories for each configured token filter.  Each configuration
     * element can be the name of an out-of-the-box token filter, or a custom definition.
     */
    private static List<TokenFilterFactory> parseTokenFilterFactories(CategorizationAnalyzerConfig config,
                                                                      AnalysisRegistry analysisRegistry, Environment environment,
                                                                      Tuple<String, TokenizerFactory> tokenizerFactory,
                                                                      List<CharFilterFactory> charFilterFactoryList) throws IOException {
        List<CategorizationAnalyzerConfig.NameOrDefinition> tokenFilters = config.getTokenFilters();
        TransportAnalyzeAction.DeferredTokenFilterRegistry deferredRegistry
            = new TransportAnalyzeAction.DeferredTokenFilterRegistry(analysisRegistry, null);
        final List<TokenFilterFactory> tokenFilterFactoryList = new ArrayList<>();
        for (CategorizationAnalyzerConfig.NameOrDefinition tokenFilter : tokenFilters) {
            TokenFilterFactory tokenFilterFactory;
            if (tokenFilter.name != null) {
                AnalysisModule.AnalysisProvider<TokenFilterFactory> tokenFilterFactoryFactory;
                tokenFilterFactoryFactory = analysisRegistry.getTokenFilterProvider(tokenFilter.name);
                if (tokenFilterFactoryFactory == null) {
                    throw new IllegalArgumentException("Failed to find global token filter under [" + tokenFilter.name + "]");
                }
                tokenFilterFactory = tokenFilterFactoryFactory.get(environment, tokenFilter.name);
            } else {
                String filterTypeName = tokenFilter.definition.get("type");
                if (filterTypeName == null) {
                    throw new IllegalArgumentException("Missing [type] setting for token filter: " + tokenFilter.definition);
                }
                AnalysisModule.AnalysisProvider<TokenFilterFactory> tokenFilterFactoryFactory =
                    analysisRegistry.getTokenFilterProvider(filterTypeName);
                if (tokenFilterFactoryFactory == null) {
                    throw new IllegalArgumentException("Failed to find global token filter under [" + filterTypeName + "]");
                }
                Settings settings = augmentSettings(tokenFilter.definition);
                // Need to set anonymous "name" of token_filter
                tokenFilterFactory = tokenFilterFactoryFactory.get(buildDummyIndexSettings(settings), environment, "_anonymous_tokenfilter",
                    settings);
                tokenFilterFactory = tokenFilterFactory.getChainAwareTokenFilterFactory(tokenizerFactory.v2(),
                    charFilterFactoryList, tokenFilterFactoryList, deferredRegistry);
            }
            if (tokenFilterFactory == null) {
                throw new IllegalArgumentException("Failed to find or create token filter [" + tokenFilter + "]");
            }
            tokenFilterFactoryList.add(tokenFilterFactory);
        }
        return tokenFilterFactoryList;
    }

    /**
     * The Elasticsearch analysis functionality is designed to work with indices.  For
     * categorization we have to pretend we've got some index settings.
     */
    private static IndexSettings buildDummyIndexSettings(Settings settings) {
        IndexMetaData metaData = IndexMetaData.builder(IndexMetaData.INDEX_UUID_NA_VALUE).settings(settings).build();
        return new IndexSettings(metaData, Settings.EMPTY);
    }

    /**
     * The behaviour of Elasticsearch analyzers can vary between versions.
     * For categorization we'll always use the latest version of the text analysis.
     * The other settings are just to stop classes that expect to be associated with
     * an index from complaining.
     */
    private static Settings augmentSettings(Settings settings) {
        return Settings.builder().put(settings)
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .build();
    }
}
