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

import org.apache.lucene.analysis.hunspell.Dictionary;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.analysis.AnalyzerProvider;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.analysis.TokenizerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * The AnalysisModule is the main extension point for node and index level analysis components. The lucene classes
 * {@link org.apache.lucene.analysis.Analyzer}, {@link org.apache.lucene.analysis.TokenFilter}, {@link org.apache.lucene.analysis.Tokenizer}
 * and {@link org.apache.lucene.analysis.CharFilter} can be extended in plugins and registered on node startup when the analysis module
 * gets loaded. Since elasticsearch needs to create multiple instances for different configurations dedicated factories need to be provided for
 * each of the components:
 * <ul>
 *     <li> {@link org.apache.lucene.analysis.Analyzer} can be exposed via {@link AnalyzerProvider} and registered on {@link #registerAnalyzer(String, AnalysisProvider)}</li>
 *     <li> {@link org.apache.lucene.analysis.TokenFilter} can be exposed via {@link TokenFilterFactory} and registered on {@link #registerTokenFilter(String, AnalysisProvider)}</li>
 *     <li> {@link org.apache.lucene.analysis.Tokenizer} can be exposed via {@link TokenizerFactory} and registered on {@link #registerTokenizer(String, AnalysisProvider)}</li>
 *     <li> {@link org.apache.lucene.analysis.CharFilter} can be exposed via {@link CharFilterFactory} and registered on {@link #registerCharFilter(String, AnalysisProvider)}</li>
 * </ul>
 *
 * The {@link org.elasticsearch.indices.analysis.AnalysisModule.AnalysisProvider} is only a functional interface that allows to register factory constructors directly like the plugin example below:
 * <pre>
 *     public class MyAnalysisPlugin extends Plugin {
 *       public void onModule(AnalysisModule module) {
 *         module.registerAnalyzer("my-analyzer-name", MyAnalyzer::new);
 *       }
 *     }
 * </pre>
 */
public final class AnalysisModule extends AbstractModule {

    static {
        Settings build = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .build();
        IndexMetaData metaData = IndexMetaData.builder("_na_").settings(build).build();
        NA_INDEX_SETTINGS = new IndexSettings(metaData, Settings.EMPTY);
    }
    private static final IndexSettings NA_INDEX_SETTINGS;
    private final Environment environment;
    private final Map<String, AnalysisProvider<CharFilterFactory>> charFilters = new HashMap<>();
    private final Map<String, AnalysisProvider<TokenFilterFactory>> tokenFilters = new HashMap<>();
    private final Map<String, AnalysisProvider<TokenizerFactory>> tokenizers = new HashMap<>();
    private final Map<String, AnalysisProvider<AnalyzerProvider>> analyzers = new HashMap<>();
    private final Map<String, org.apache.lucene.analysis.hunspell.Dictionary> knownDictionaries = new HashMap<>();

    /**
     * Creates a new AnalysisModule
     */
    public AnalysisModule(Environment environment) {
        this.environment = environment;
    }

    /**
     * Registers a new {@link AnalysisProvider} to create
     * {@link CharFilterFactory} instance per node as well as per index.
     */
    public void registerCharFilter(String name, AnalysisProvider<CharFilterFactory> charFilter) {
        if (charFilter == null) {
            throw new IllegalArgumentException("char_filter provider must not be null");
        }
        if (charFilters.putIfAbsent(name, charFilter) != null) {
            throw new IllegalArgumentException("char_filter provider for name " + name + " already registered");
        }
    }

    /**
     * Registers a new {@link AnalysisProvider} to create
     * {@link TokenFilterFactory} instance per node as well as per index.
     */
    public void registerTokenFilter(String name, AnalysisProvider<TokenFilterFactory> tokenFilter) {
        if (tokenFilter == null) {
            throw new IllegalArgumentException("token_filter provider must not be null");
        }
        if (tokenFilters.putIfAbsent(name, tokenFilter) != null) {
            throw new IllegalArgumentException("token_filter provider for name " + name + " already registered");
        }
    }

    /**
     * Registers a new {@link AnalysisProvider} to create
     * {@link TokenizerFactory} instance per node as well as per index.
     */
    public void registerTokenizer(String name, AnalysisProvider<TokenizerFactory> tokenizer) {
        if (tokenizer == null) {
            throw new IllegalArgumentException("tokenizer provider must not be null");
        }
        if (tokenizers.putIfAbsent(name, tokenizer) != null) {
            throw new IllegalArgumentException("tokenizer provider for name " + name + " already registered");
        }
    }

    /**
     * Registers a new {@link AnalysisProvider} to create
     * {@link AnalyzerProvider} instance per node as well as per index.
     */
    public void registerAnalyzer(String name, AnalysisProvider<AnalyzerProvider> analyzer) {
        if (analyzer == null) {
            throw new IllegalArgumentException("analyzer provider must not be null");
        }
        if (analyzers.putIfAbsent(name, analyzer) != null) {
            throw new IllegalArgumentException("analyzer provider for name " + name + " already registered");
        }
    }

    /**
     * Registers a new hunspell {@link Dictionary} that can be referenced by the given name in
     * hunspell analysis configuration.
     */
    public void registerHunspellDictionary(String name, Dictionary dictionary) {
        if (knownDictionaries.putIfAbsent(name, dictionary) != null) {
            throw new IllegalArgumentException("dictionary for [" + name + "] is already registered");
        }
    }

    @Override
    protected void configure() {
        try {
            AnalysisRegistry registry = buildRegistry();
            bind(HunspellService.class).toInstance(registry.getHunspellService());
            bind(AnalysisRegistry.class).toInstance(registry);
        } catch (IOException e) {
            throw new ElasticsearchException("failed to load hunspell service", e);
        }
    }

    /**
     * Builds an {@link AnalysisRegistry} from the current configuration.
     */
    public AnalysisRegistry buildRegistry() throws IOException {
        return new AnalysisRegistry(new HunspellService(environment.settings(), environment, knownDictionaries), environment, charFilters, tokenFilters, tokenizers, analyzers);
    }

    /**
     * AnalysisProvider is the basic factory interface for registering analysis components like:
     * <ul>
     *     <li>{@link TokenizerFactory} - see {@link AnalysisModule#registerTokenizer(String, AnalysisProvider)}</li>
     *     <li>{@link CharFilterFactory} - see {@link AnalysisModule#registerCharFilter(String, AnalysisProvider)}</li>
     *     <li>{@link AnalyzerProvider} - see {@link AnalysisModule#registerAnalyzer(String, AnalysisProvider)}</li>
     *     <li>{@link TokenFilterFactory}- see {@link AnalysisModule#registerTokenFilter(String, AnalysisProvider)} )}</li>
     * </ul>
     */
    public interface AnalysisProvider<T> {

        /**
         * Creates a new analysis provider.
         * @param indexSettings the index settings for the index this provider is created for
         * @param environment the nodes environment to load resources from persistent storage
         * @param name the name of the analysis component
         * @param settings the component specific settings without context prefixes
         * @return a new provider instance
         * @throws IOException if an {@link IOException} occurs
         */
        T get(IndexSettings indexSettings, Environment environment, String name, Settings settings) throws IOException;

        /**
         * Creates a new global scope analysis provider without index specific settings not settings for the provider itself.
         * This can be used to get a default instance of an analysis factory without binding to an index.
         *
         * @param environment the nodes environment to load resources from persistent storage
         * @param name the name of the analysis component
         * @return a new provider instance
         * @throws IOException if an {@link IOException} occurs
         * @throws IllegalArgumentException if the provider requires analysis settings ie. if {@link #requiresAnalysisSettings()} returns <code>true</code>
         */
        default T get(Environment environment, String name) throws IOException {
            if (requiresAnalysisSettings()) {
                throw new IllegalArgumentException("Analysis settings required - can't instantiate analysis factory");
            }
            return get(NA_INDEX_SETTINGS, environment, name, NA_INDEX_SETTINGS.getSettings());
        }

        /**
         * If <code>true</code> the analysis component created by this provider requires certain settings to be instantiated.
         * it can't be created with defaults. The default is <code>false</code>.
         */
        default boolean requiresAnalysisSettings() {
            return false;
        }
    }
}
