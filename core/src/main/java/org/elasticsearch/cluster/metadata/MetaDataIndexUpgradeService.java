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

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.lucene.analysis.Analyzer;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.routing.DjbHashFunction;
import org.elasticsearch.cluster.routing.HashFunction;
import org.elasticsearch.cluster.routing.SimpleHashFunction;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.Classes;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import com.google.common.collect.ImmutableSet;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.similarity.SimilarityLookupService;
import org.elasticsearch.index.store.IndexStoreModule;
import org.elasticsearch.script.ScriptService;

import java.util.Locale;
import java.util.Set;

/**
 * This service is responsible for upgrading legacy index metadata to the current version
 * <p/>
 * Every time an existing index is introduced into cluster this service should be used
 * to upgrade the existing index metadata to the latest version of the cluster. It typically
 * occurs during cluster upgrade, when dangling indices are imported into the cluster or indices
 * are restored from a repository.
 */
public class MetaDataIndexUpgradeService extends AbstractComponent {

    private static final String DEPRECATED_SETTING_ROUTING_HASH_FUNCTION = "cluster.routing.operation.hash.type";
    private static final String DEPRECATED_SETTING_ROUTING_USE_TYPE = "cluster.routing.operation.use_type";

    private final Class<? extends HashFunction> pre20HashFunction;
    private final Boolean pre20UseType;
    private final ScriptService scriptService;

    @Inject
    public MetaDataIndexUpgradeService(Settings settings, ScriptService scriptService) {
        super(settings);
        this.scriptService = scriptService;
        final String pre20HashFunctionName = settings.get(DEPRECATED_SETTING_ROUTING_HASH_FUNCTION, null);
        final boolean hasCustomPre20HashFunction = pre20HashFunctionName != null;
        // the hash function package has changed we replace the two hash functions if their fully qualified name is used.
        if (hasCustomPre20HashFunction) {
            switch (pre20HashFunctionName) {
                case "Simple":
                case "simple":
                case "org.elasticsearch.cluster.routing.operation.hash.simple.SimpleHashFunction":
                    pre20HashFunction = SimpleHashFunction.class;
                    break;
                case "Djb":
                case "djb":
                case "org.elasticsearch.cluster.routing.operation.hash.djb.DjbHashFunction":
                    pre20HashFunction = DjbHashFunction.class;
                    break;
                default:
                    try {
                        pre20HashFunction = Class.forName(pre20HashFunctionName).asSubclass(HashFunction.class);
                    } catch (ClassNotFoundException|NoClassDefFoundError e) {
                        throw new ElasticsearchException("failed to load custom hash function [" + pre20HashFunctionName + "]", e);
                    }
            }
        } else {
            pre20HashFunction = DjbHashFunction.class;
        }
        pre20UseType = settings.getAsBoolean(DEPRECATED_SETTING_ROUTING_USE_TYPE, null);
        if (hasCustomPre20HashFunction || pre20UseType != null) {
            logger.warn("Settings [{}] and [{}] are deprecated. Index settings from your old indices have been updated to record the fact that they "
                    + "used some custom routing logic, you can now remove these settings from your `elasticsearch.yml` file", DEPRECATED_SETTING_ROUTING_HASH_FUNCTION, DEPRECATED_SETTING_ROUTING_USE_TYPE);
        }
    }

    /**
     * Checks that the index can be upgraded to the current version of the master node.
     *
     * <p/>
     * If the index does not need upgrade it returns the index metadata unchanged, otherwise it returns a modified index metadata. If index
     * cannot be updated the method throws an exception.
     */
    public IndexMetaData upgradeIndexMetaData(IndexMetaData indexMetaData) {
        // Throws an exception if there are too-old segments:
        if (isUpgraded(indexMetaData)) {
            return indexMetaData;
        }
        checkSupportedVersion(indexMetaData);
        IndexMetaData newMetaData = upgradeLegacyRoutingSettings(indexMetaData);
        newMetaData = addDefaultUnitsIfNeeded(newMetaData);
        checkMappingsCompatibility(newMetaData);
        newMetaData = upgradeSettings(newMetaData);
        newMetaData = markAsUpgraded(newMetaData);
        return newMetaData;
    }

    IndexMetaData upgradeSettings(IndexMetaData indexMetaData) {
        final String storeType = indexMetaData.getSettings().get(IndexStoreModule.STORE_TYPE);
        if (storeType != null) {
            final String upgradeStoreType;
            switch (storeType.toLowerCase(Locale.ROOT)) {
                case "nio_fs":
                case "niofs":
                    upgradeStoreType = "niofs";
                    break;
                case "mmap_fs":
                case "mmapfs":
                    upgradeStoreType = "mmapfs";
                    break;
                case "simple_fs":
                case "simplefs":
                    upgradeStoreType = "simplefs";
                    break;
                case "default":
                    upgradeStoreType = "default";
                    break;
                case "fs":
                    upgradeStoreType = "fs";
                    break;
                default:
                    upgradeStoreType = storeType;
            }
            if (storeType.equals(upgradeStoreType) == false) {
                Settings indexSettings = Settings.builder().put(indexMetaData.settings())
                        .put(IndexStoreModule.STORE_TYPE, upgradeStoreType)
                        .build();
                return IndexMetaData.builder(indexMetaData)
                        .version(indexMetaData.version())
                        .settings(indexSettings)
                        .build();
            }
        }
        return indexMetaData;
    }

    /**
     * Checks if the index was already opened by this version of Elasticsearch and doesn't require any additional checks.
     */
    private boolean isUpgraded(IndexMetaData indexMetaData) {
        return indexMetaData.upgradeVersion().onOrAfter(Version.V_2_0_0_beta1);
    }

    /**
     * Elasticsearch 2.0 no longer supports indices with pre Lucene v4.0 (Elasticsearch v 0.90.0) segments. All indices
     * that were created before Elasticsearch v0.90.0 should be upgraded using upgrade plugin before they can
     * be open by this version of elasticsearch.
     */
    private void checkSupportedVersion(IndexMetaData indexMetaData) {
        if (indexMetaData.getState() == IndexMetaData.State.OPEN && isSupportedVersion(indexMetaData) == false) {
            throw new IllegalStateException("The index [" + indexMetaData.getIndex() + "] was created before v0.90.0 and wasn't upgraded."
                    + " This index should be open using a version before " + Version.CURRENT.minimumCompatibilityVersion()
                    + " and upgraded using the upgrade API.");
        }
    }

    /*
     * Returns true if this index can be supported by the current version of elasticsearch
     */
    private static boolean isSupportedVersion(IndexMetaData indexMetaData) {
        if (indexMetaData.creationVersion().onOrAfter(Version.V_0_90_0_Beta1)) {
            // The index was created with elasticsearch that was using Lucene 4.0
            return true;
        }
        if (indexMetaData.getMinimumCompatibleVersion() != null &&
                indexMetaData.getMinimumCompatibleVersion().onOrAfter(org.apache.lucene.util.Version.LUCENE_4_0_0)) {
            //The index was upgraded we can work with it
            return true;
        }
        return false;
    }

    /**
     * Elasticsearch 2.0 deprecated custom routing hash functions. So what we do here is that for old indices, we
     * move this old and deprecated node setting to an index setting so that we can keep things backward compatible.
     */
    private IndexMetaData upgradeLegacyRoutingSettings(IndexMetaData indexMetaData) {
        if (indexMetaData.settings().get(IndexMetaData.SETTING_LEGACY_ROUTING_HASH_FUNCTION) == null
                && indexMetaData.getCreationVersion().before(Version.V_2_0_0_beta1)) {
            // these settings need an upgrade
            Settings indexSettings = Settings.builder().put(indexMetaData.settings())
                    .put(IndexMetaData.SETTING_LEGACY_ROUTING_HASH_FUNCTION, pre20HashFunction)
                    .put(IndexMetaData.SETTING_LEGACY_ROUTING_USE_TYPE, pre20UseType == null ? false : pre20UseType)
                    .build();
            return IndexMetaData.builder(indexMetaData)
                    .version(indexMetaData.version())
                    .settings(indexSettings)
                    .build();
        } else if (indexMetaData.getCreationVersion().onOrAfter(Version.V_2_0_0_beta1)) {
            if (indexMetaData.getSettings().get(IndexMetaData.SETTING_LEGACY_ROUTING_HASH_FUNCTION) != null
                    || indexMetaData.getSettings().get(IndexMetaData.SETTING_LEGACY_ROUTING_USE_TYPE) != null) {
                throw new IllegalStateException("Index [" + indexMetaData.getIndex() + "] created on or after 2.0 should NOT contain [" + IndexMetaData.SETTING_LEGACY_ROUTING_HASH_FUNCTION
                        + "] + or [" + IndexMetaData.SETTING_LEGACY_ROUTING_USE_TYPE + "] in its index settings");
            }
        }
        return indexMetaData;
    }

    /** All known byte-sized settings for an index. */
    public static final Set<String> INDEX_BYTES_SIZE_SETTINGS = ImmutableSet.of(
                                    "index.buffer_size",
                                    "index.merge.policy.floor_segment",
                                    "index.merge.policy.max_merged_segment",
                                    "index.merge.policy.max_merge_size",
                                    "index.merge.policy.min_merge_size",
                                    "index.shard.recovery.file_chunk_size",
                                    "index.shard.recovery.translog_size",
                                    "index.store.throttle.max_bytes_per_sec",
                                    "index.translog.flush_threshold_size",
                                    "index.translog.fs.buffer_size",
                                    "index.version_map_size");

    /** All known time settings for an index. */
    public static final Set<String> INDEX_TIME_SETTINGS = ImmutableSet.of(
                                    "index.gateway.wait_for_mapping_update_post_recovery",
                                    "index.shard.wait_for_mapping_update_post_recovery",
                                    "index.gc_deletes",
                                    "index.indexing.slowlog.threshold.index.debug",
                                    "index.indexing.slowlog.threshold.index.info",
                                    "index.indexing.slowlog.threshold.index.trace",
                                    "index.indexing.slowlog.threshold.index.warn",
                                    "index.refresh_interval",
                                    "index.search.slowlog.threshold.fetch.debug",
                                    "index.search.slowlog.threshold.fetch.info",
                                    "index.search.slowlog.threshold.fetch.trace",
                                    "index.search.slowlog.threshold.fetch.warn",
                                    "index.search.slowlog.threshold.query.debug",
                                    "index.search.slowlog.threshold.query.info",
                                    "index.search.slowlog.threshold.query.trace",
                                    "index.search.slowlog.threshold.query.warn",
                                    "index.shadow.wait_for_initial_commit",
                                    "index.store.stats_refresh_interval",
                                    "index.translog.flush_threshold_period",
                                    "index.translog.interval",
                                    "index.translog.sync_interval",
                                    UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING);

    /**
     * Elasticsearch 2.0 requires units on byte/memory and time settings; this method adds the default unit to any such settings that are
     * missing units.
     */
    private IndexMetaData addDefaultUnitsIfNeeded(IndexMetaData indexMetaData) {
        if (indexMetaData.getCreationVersion().before(Version.V_2_0_0_beta1)) {
            // TODO: can we somehow only do this *once* for a pre-2.0 index?  Maybe we could stuff a "fake marker setting" here?  Seems hackish...
            // Created lazily if we find any settings that are missing units:
            Settings settings = indexMetaData.settings();
            Settings.Builder newSettings = null;
            for(String byteSizeSetting : INDEX_BYTES_SIZE_SETTINGS) {
                String value = settings.get(byteSizeSetting);
                if (value != null) {
                    try {
                        Long.parseLong(value);
                    } catch (NumberFormatException nfe) {
                        continue;
                    }
                    // It's a naked number that previously would be interpreted as default unit (bytes); now we add it:
                    logger.warn("byte-sized index setting [{}] with value [{}] is missing units; assuming default units (b) but in future versions this will be a hard error", byteSizeSetting, value);
                    if (newSettings == null) {
                        newSettings = Settings.builder();
                        newSettings.put(settings);
                    }
                    newSettings.put(byteSizeSetting, value + "b");
                }
            }
            for(String timeSetting : INDEX_TIME_SETTINGS) {
                String value = settings.get(timeSetting);
                if (value != null) {
                    try {
                        Long.parseLong(value);
                    } catch (NumberFormatException nfe) {
                        continue;
                    }
                    // It's a naked number that previously would be interpreted as default unit (ms); now we add it:
                    logger.warn("time index setting [{}] with value [{}] is missing units; assuming default units (ms) but in future versions this will be a hard error", timeSetting, value);
                    if (newSettings == null) {
                        newSettings = Settings.builder();
                        newSettings.put(settings);
                    }
                    newSettings.put(timeSetting, value + "ms");
                }
            }
            if (newSettings != null) {
                // At least one setting was changed:
                return IndexMetaData.builder(indexMetaData)
                    .version(indexMetaData.version())
                    .settings(newSettings.build())
                    .build();
            }
        }

        // No changes:
        return indexMetaData;
    }


    /**
     * Checks the mappings for compatibility with the current version
     */
    private void checkMappingsCompatibility(IndexMetaData indexMetaData) {
        Index index = new Index(indexMetaData.getIndex());
        Settings settings = indexMetaData.settings();
        try {
            SimilarityLookupService similarityLookupService = new SimilarityLookupService(index, settings);
            // We cannot instantiate real analysis server at this point because the node might not have
            // been started yet. However, we don't really need real analyzers at this stage - so we can fake it
            try (AnalysisService analysisService = new FakeAnalysisService(index, settings)) {
                try (MapperService mapperService = new MapperService(index, settings, analysisService, similarityLookupService, scriptService)) {
                    for (ObjectCursor<MappingMetaData> cursor : indexMetaData.getMappings().values()) {
                        MappingMetaData mappingMetaData = cursor.value;
                        mapperService.merge(mappingMetaData.type(), mappingMetaData.source(), false, false);
                    }
                }
            }
        } catch (Exception ex) {
            // Wrap the inner exception so we have the index name in the exception message
            throw new IllegalStateException("unable to upgrade the mappings for the index [" + indexMetaData.getIndex() + "], reason: [" + ex.getMessage() + "]", ex);
        }
    }

    /**
     * Marks index as upgraded so we don't have to test it again
     */
    private IndexMetaData markAsUpgraded(IndexMetaData indexMetaData) {
        Settings settings = Settings.builder().put(indexMetaData.settings()).put(IndexMetaData.SETTING_VERSION_UPGRADED, Version.CURRENT).build();
        return IndexMetaData.builder(indexMetaData).settings(settings).build();
    }

    /**
     * A fake analysis server that returns the same keyword analyzer for all requests
     */
    private static class FakeAnalysisService extends AnalysisService {

        private Analyzer fakeAnalyzer = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                throw new UnsupportedOperationException("shouldn't be here");
            }
        };

        public FakeAnalysisService(Index index, Settings indexSettings) {
            super(index, indexSettings);
        }

        @Override
        public NamedAnalyzer analyzer(String name) {
            return new NamedAnalyzer(name, fakeAnalyzer);
        }

        @Override
        public void close() {
            fakeAnalyzer.close();
            super.close();
        }
    }

}
