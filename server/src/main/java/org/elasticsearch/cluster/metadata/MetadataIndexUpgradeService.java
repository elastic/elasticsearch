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
package org.elasticsearch.cluster.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.Similarity;
import org.elasticsearch.Version;
import org.elasticsearch.common.TriFunction;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.indices.mapper.MapperRegistry;
import org.elasticsearch.script.ScriptService;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * This service is responsible for upgrading legacy index metadata to the current version
 * <p>
 * Every time an existing index is introduced into cluster this service should be used
 * to upgrade the existing index metadata to the latest version of the cluster. It typically
 * occurs during cluster upgrade, when dangling indices are imported into the cluster or indices
 * are restored from a repository.
 */
public class MetadataIndexUpgradeService {

    private static final Logger logger = LogManager.getLogger(MetadataIndexUpgradeService.class);

    private final Settings settings;
    private final NamedXContentRegistry xContentRegistry;
    private final MapperRegistry mapperRegistry;
    private final IndexScopedSettings indexScopedSettings;
    private final SystemIndices systemIndices;

    public MetadataIndexUpgradeService(Settings settings, NamedXContentRegistry xContentRegistry, MapperRegistry mapperRegistry,
                                       IndexScopedSettings indexScopedSettings, SystemIndices systemIndices) {
        this.settings = settings;
        this.xContentRegistry = xContentRegistry;
        this.mapperRegistry = mapperRegistry;
        this.indexScopedSettings = indexScopedSettings;
        this.systemIndices = systemIndices;
    }

    /**
     * Checks that the index can be upgraded to the current version of the master node.
     *
     * <p>
     * If the index does not need upgrade it returns the index metadata unchanged, otherwise it returns a modified index metadata. If index
     * cannot be updated the method throws an exception.
     */
    public IndexMetadata upgradeIndexMetadata(IndexMetadata indexMetadata, Version minimumIndexCompatibilityVersion) {
        if (isUpgraded(indexMetadata)) {
            /*
             * We still need to check for broken index settings since it might be that a user removed a plugin that registers a setting
             * needed by this index. Additionally, the system flag could have been lost during a rolling upgrade where the previous version
             * did not know about the flag.
             */
            return archiveBrokenIndexSettings(maybeMarkAsSystemIndex(indexMetadata));
        }
        // Throws an exception if there are too-old segments:
        checkSupportedVersion(indexMetadata, minimumIndexCompatibilityVersion);
        final IndexMetadata metadataWithSystemMarked = maybeMarkAsSystemIndex(indexMetadata);
        // we have to run this first otherwise in we try to create IndexSettings
        // with broken settings and fail in checkMappingsCompatibility
        final IndexMetadata newMetadata = archiveBrokenIndexSettings(metadataWithSystemMarked);
        // only run the check with the upgraded settings!!
        checkMappingsCompatibility(newMetadata);
        return markAsUpgraded(newMetadata);
    }

    /**
     * Checks if the index was already opened by this version of Elasticsearch and doesn't require any additional checks.
     */
    boolean isUpgraded(IndexMetadata indexMetadata) {
        return indexMetadata.getUpgradedVersion().onOrAfter(Version.CURRENT);
    }

    /**
     * Elasticsearch does not support indices created before the previous major version. They must be reindexed using an earlier version
     * before they can be opened here.
     */
    private void checkSupportedVersion(IndexMetadata indexMetadata, Version minimumIndexCompatibilityVersion) {
        if (isSupportedVersion(indexMetadata, minimumIndexCompatibilityVersion) == false) {
            throw new IllegalStateException("The index " + indexMetadata.getIndex() + " was created with version ["
                + indexMetadata.getCreationVersion() + "] but the minimum compatible version is ["
                + minimumIndexCompatibilityVersion + "]. It should be re-indexed in Elasticsearch "
                + minimumIndexCompatibilityVersion.major + ".x before upgrading to " + Version.CURRENT + ".");
        }
    }

    /*
     * Returns true if this index can be supported by the current version of elasticsearch
     */
    private static boolean isSupportedVersion(IndexMetadata indexMetadata, Version minimumIndexCompatibilityVersion) {
        return indexMetadata.getCreationVersion().onOrAfter(minimumIndexCompatibilityVersion);
    }

    /**
     * Checks the mappings for compatibility with the current version
     */
    private void checkMappingsCompatibility(IndexMetadata indexMetadata) {
        try {

            // We cannot instantiate real analysis server or similarity service at this point because the node
            // might not have been started yet. However, we don't really need real analyzers or similarities at
            // this stage - so we can fake it using constant maps accepting every key.
            // This is ok because all used similarities and analyzers for this index were known before the upgrade.
            // Missing analyzers and similarities plugin will still trigger the appropriate error during the
            // actual upgrade.

            IndexSettings indexSettings = new IndexSettings(indexMetadata, this.settings);

            final Map<String, TriFunction<Settings, Version, ScriptService, Similarity>> similarityMap
                    = new AbstractMap<String, TriFunction<Settings, Version, ScriptService, Similarity>>() {
                @Override
                public boolean containsKey(Object key) {
                    return true;
                }

                @Override
                public TriFunction<Settings, Version, ScriptService, Similarity> get(Object key) {
                    assert key instanceof String : "key must be a string but was: " + key.getClass();
                    return (settings, version, scriptService) -> new BM25Similarity();
                }

                // this entrySet impl isn't fully correct but necessary as SimilarityService will iterate
                // over all similarities
                @Override
                public Set<Entry<String, TriFunction<Settings, Version, ScriptService, Similarity>>> entrySet() {
                    return Collections.emptySet();
                }
            };
            SimilarityService similarityService = new SimilarityService(indexSettings, null, similarityMap);
            final NamedAnalyzer fakeDefault = new NamedAnalyzer("default", AnalyzerScope.INDEX, new Analyzer() {
                @Override
                protected TokenStreamComponents createComponents(String fieldName) {
                    throw new UnsupportedOperationException("shouldn't be here");
                }
            });

            final Map<String, NamedAnalyzer> analyzerMap = new AbstractMap<String, NamedAnalyzer>() {
                @Override
                public NamedAnalyzer get(Object key) {
                    assert key instanceof String : "key must be a string but was: " + key.getClass();
                    return new NamedAnalyzer((String)key, AnalyzerScope.INDEX, fakeDefault.analyzer());
                }

                // this entrySet impl isn't fully correct but necessary as IndexAnalyzers will iterate
                // over all analyzers to close them
                @Override
                public Set<Entry<String, NamedAnalyzer>> entrySet() {
                    return Collections.emptySet();
                }
            };
            try (IndexAnalyzers fakeIndexAnalzyers =
                     new IndexAnalyzers(analyzerMap, analyzerMap, analyzerMap)) {
                MapperService mapperService = new MapperService(indexSettings, fakeIndexAnalzyers, xContentRegistry, similarityService,
                        mapperRegistry, () -> null, () -> false);
                mapperService.merge(indexMetadata, MapperService.MergeReason.MAPPING_RECOVERY);
            }
        } catch (Exception ex) {
            // Wrap the inner exception so we have the index name in the exception message
            throw new IllegalStateException("unable to upgrade the mappings for the index [" + indexMetadata.getIndex() + "]", ex);
        }
    }

    /**
     * Marks index as upgraded so we don't have to test it again
     */
    private IndexMetadata markAsUpgraded(IndexMetadata indexMetadata) {
        Settings settings = Settings.builder().put(indexMetadata.getSettings())
            .put(IndexMetadata.SETTING_VERSION_UPGRADED, Version.CURRENT).build();
        return IndexMetadata.builder(indexMetadata).settings(settings).build();
    }

    IndexMetadata archiveBrokenIndexSettings(IndexMetadata indexMetadata) {
        final Settings settings = indexMetadata.getSettings();
        final Settings upgrade = indexScopedSettings.archiveUnknownOrInvalidSettings(
            settings,
            e -> logger.warn("{} ignoring unknown index setting: [{}] with value [{}]; archiving",
                indexMetadata.getIndex(), e.getKey(), e.getValue()),
            (e, ex) -> logger.warn(() -> new ParameterizedMessage("{} ignoring invalid index setting: [{}] with value [{}]; archiving",
                indexMetadata.getIndex(), e.getKey(), e.getValue()), ex));
        if (upgrade != settings) {
            return IndexMetadata.builder(indexMetadata).settings(upgrade).build();
        } else {
            return indexMetadata;
        }
    }

    IndexMetadata maybeMarkAsSystemIndex(IndexMetadata indexMetadata) {
        final boolean isSystem = systemIndices.isSystemIndex(indexMetadata.getIndex());
        if (isSystem != indexMetadata.isSystem()) {
            return IndexMetadata.builder(indexMetadata).system(isSystem).build();
        }
        return indexMetadata;
    }
}
