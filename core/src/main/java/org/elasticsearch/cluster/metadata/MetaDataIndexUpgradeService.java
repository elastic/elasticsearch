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
import org.elasticsearch.Version;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.script.ScriptService;

import java.util.Set;

import static java.util.Collections.unmodifiableSet;
import static org.elasticsearch.common.util.set.Sets.newHashSet;

/**
 * This service is responsible for upgrading legacy index metadata to the current version
 * <p>
 * Every time an existing index is introduced into cluster this service should be used
 * to upgrade the existing index metadata to the latest version of the cluster. It typically
 * occurs during cluster upgrade, when dangling indices are imported into the cluster or indices
 * are restored from a repository.
 */
public class MetaDataIndexUpgradeService extends AbstractComponent {

    private final ScriptService scriptService;

    @Inject
    public MetaDataIndexUpgradeService(Settings settings, ScriptService scriptService) {
        super(settings);
        this.scriptService = scriptService;
    }

    /**
     * Checks that the index can be upgraded to the current version of the master node.
     *
     * <p>
     * If the index does not need upgrade it returns the index metadata unchanged, otherwise it returns a modified index metadata. If index
     * cannot be updated the method throws an exception.
     */
    public IndexMetaData upgradeIndexMetaData(IndexMetaData indexMetaData) {
        // Throws an exception if there are too-old segments:
        if (isUpgraded(indexMetaData)) {
            return indexMetaData;
        }
        checkSupportedVersion(indexMetaData);
        IndexMetaData newMetaData = indexMetaData;
        newMetaData = addDefaultUnitsIfNeeded(newMetaData);
        checkMappingsCompatibility(newMetaData);
        newMetaData = markAsUpgraded(newMetaData);
        return newMetaData;
    }


    /**
     * Checks if the index was already opened by this version of Elasticsearch and doesn't require any additional checks.
     */
    private boolean isUpgraded(IndexMetaData indexMetaData) {
        return indexMetaData.getUpgradedVersion().onOrAfter(Version.V_3_0_0);
    }

    /**
     * Elasticsearch 3.0 no longer supports indices with pre Lucene v5.0 (Elasticsearch v2.0.0.beta1) segments. All indices
     * that were created before Elasticsearch v2.0.0.beta1 should be upgraded using upgrade API before they can
     * be open by this version of elasticsearch.
     */
    private void checkSupportedVersion(IndexMetaData indexMetaData) {
        if (indexMetaData.getState() == IndexMetaData.State.OPEN && isSupportedVersion(indexMetaData) == false) {
            throw new IllegalStateException("The index [" + indexMetaData.getIndex() + "] was created before v2.0.0.beta1 and wasn't upgraded."
                    + " This index should be open using a version before " + Version.CURRENT.minimumCompatibilityVersion()
                    + " and upgraded using the upgrade API.");
        }
    }

    /*
     * Returns true if this index can be supported by the current version of elasticsearch
     */
    private static boolean isSupportedVersion(IndexMetaData indexMetaData) {
        if (indexMetaData.getCreationVersion().onOrAfter(Version.V_2_0_0_beta1)) {
            // The index was created with elasticsearch that was using Lucene 5.2.1
            return true;
        }
        if (indexMetaData.getMinimumCompatibleVersion() != null &&
                indexMetaData.getMinimumCompatibleVersion().onOrAfter(org.apache.lucene.util.Version.LUCENE_5_0_0)) {
            //The index was upgraded we can work with it
            return true;
        }
        return false;
    }

    /** All known byte-sized settings for an index. */
    public static final Set<String> INDEX_BYTES_SIZE_SETTINGS = unmodifiableSet(newHashSet(
                                    "index.merge.policy.floor_segment",
                                    "index.merge.policy.max_merged_segment",
                                    "index.merge.policy.max_merge_size",
                                    "index.merge.policy.min_merge_size",
                                    "index.shard.recovery.file_chunk_size",
                                    "index.shard.recovery.translog_size",
                                    "index.store.throttle.max_bytes_per_sec",
                                    "index.translog.flush_threshold_size",
                                    "index.translog.fs.buffer_size",
                                    "index.version_map_size"));

    /** All known time settings for an index. */
    public static final Set<String> INDEX_TIME_SETTINGS = unmodifiableSet(newHashSet(
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
                                    UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING));

    /**
     * Elasticsearch 2.0 requires units on byte/memory and time settings; this method adds the default unit to any such settings that are
     * missing units.
     */
    private IndexMetaData addDefaultUnitsIfNeeded(IndexMetaData indexMetaData) {
        if (indexMetaData.getCreationVersion().before(Version.V_2_0_0_beta1)) {
            // TODO: can we somehow only do this *once* for a pre-2.0 index?  Maybe we could stuff a "fake marker setting" here?  Seems hackish...
            // Created lazily if we find any settings that are missing units:
            Settings settings = indexMetaData.getSettings();
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
                    .version(indexMetaData.getVersion())
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
        Settings settings = indexMetaData.getSettings();
        try {
            SimilarityService similarityService = new SimilarityService(index, settings);
            // We cannot instantiate real analysis server at this point because the node might not have
            // been started yet. However, we don't really need real analyzers at this stage - so we can fake it
            try (AnalysisService analysisService = new FakeAnalysisService(index, settings)) {
                try (MapperService mapperService = new MapperService(index, settings, analysisService, similarityService, scriptService)) {
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
        Settings settings = Settings.builder().put(indexMetaData.getSettings()).put(IndexMetaData.SETTING_VERSION_UPGRADED, Version.CURRENT).build();
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
