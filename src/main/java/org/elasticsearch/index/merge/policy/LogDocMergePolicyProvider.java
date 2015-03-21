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

package org.elasticsearch.index.merge.policy;

import org.apache.lucene.index.LogDocMergePolicy;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Preconditions;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.index.store.Store;

/**
 *
 */
public class LogDocMergePolicyProvider extends AbstractMergePolicyProvider<LogDocMergePolicy> {

    private final IndexSettingsService indexSettingsService;
    private final ApplySettings applySettings = new ApplySettings();
    private final LogDocMergePolicy mergePolicy = new LogDocMergePolicy();


    public static final String MAX_MERGE_DOCS_KEY = "index.merge.policy.max_merge_docs";
    public static final String MIN_MERGE_DOCS_KEY = "index.merge.policy.min_merge_docs";
    public static final String MERGE_FACTORY_KEY = "index.merge.policy.merge_factor";

    @Inject
    public LogDocMergePolicyProvider(Store store, IndexSettingsService indexSettingsService) {
        super(store);
        Preconditions.checkNotNull(store, "Store must be provided to merge policy");
        this.indexSettingsService = indexSettingsService;

        int minMergeDocs = indexSettings.getAsInt(MIN_MERGE_DOCS_KEY, LogDocMergePolicy.DEFAULT_MIN_MERGE_DOCS);
        int maxMergeDocs = indexSettings.getAsInt(MAX_MERGE_DOCS_KEY, LogDocMergePolicy.DEFAULT_MAX_MERGE_DOCS);
        int mergeFactor = indexSettings.getAsInt(MERGE_FACTORY_KEY, LogDocMergePolicy.DEFAULT_MERGE_FACTOR);
        boolean calibrateSizeByDeletes = indexSettings.getAsBoolean("index.merge.policy.calibrate_size_by_deletes", true);

        mergePolicy.setMinMergeDocs(minMergeDocs);
        mergePolicy.setMaxMergeDocs(maxMergeDocs);
        mergePolicy.setMergeFactor(mergeFactor);
        mergePolicy.setCalibrateSizeByDeletes(calibrateSizeByDeletes);
        mergePolicy.setNoCFSRatio(noCFSRatio);
        logger.debug("using [log_doc] merge policy with merge_factor[{}], min_merge_docs[{}], max_merge_docs[{}], calibrate_size_by_deletes[{}]",
                mergeFactor, minMergeDocs, maxMergeDocs, calibrateSizeByDeletes);

        indexSettingsService.addListener(applySettings);
    }

    @Override
    public void close() throws ElasticsearchException {
        indexSettingsService.removeListener(applySettings);
    }

    @Override
    public LogDocMergePolicy getMergePolicy() {
        return mergePolicy;
    }

    public static final String INDEX_MERGE_POLICY_MIN_MERGE_DOCS = "index.merge.policy.min_merge_docs";
    public static final String INDEX_MERGE_POLICY_MAX_MERGE_DOCS = "index.merge.policy.max_merge_docs";
    public static final String INDEX_MERGE_POLICY_MERGE_FACTOR = "index.merge.policy.merge_factor";
    public static final String INDEX_MERGE_POLICY_CALIBRATE_SIZE_BY_DELETES = "index.merge.policy.calibrate_size_by_deletes";

    class ApplySettings implements IndexSettingsService.Listener {
        @Override
        public void onRefreshSettings(Settings settings) {
            int oldMinMergeDocs = mergePolicy.getMinMergeDocs();
            int minMergeDocs = settings.getAsInt(INDEX_MERGE_POLICY_MIN_MERGE_DOCS, oldMinMergeDocs);
            if (minMergeDocs != oldMinMergeDocs) {
                logger.info("updating min_merge_docs from [{}] to [{}]", oldMinMergeDocs, minMergeDocs);
                mergePolicy.setMinMergeDocs(minMergeDocs);
            }

            int oldMaxMergeDocs = mergePolicy.getMaxMergeDocs();
            int maxMergeDocs = settings.getAsInt(INDEX_MERGE_POLICY_MAX_MERGE_DOCS, oldMaxMergeDocs);
            if (maxMergeDocs != oldMaxMergeDocs) {
                logger.info("updating max_merge_docs from [{}] to [{}]", oldMaxMergeDocs, maxMergeDocs);
                mergePolicy.setMaxMergeDocs(maxMergeDocs);
            }

            int oldMergeFactor = mergePolicy.getMergeFactor();
            int mergeFactor = settings.getAsInt(INDEX_MERGE_POLICY_MERGE_FACTOR, oldMergeFactor);
            if (mergeFactor != oldMergeFactor) {
                logger.info("updating merge_factor from [{}] to [{}]", oldMergeFactor, mergeFactor);
                mergePolicy.setMergeFactor(mergeFactor);
            }

            boolean oldCalibrateSizeByDeletes = mergePolicy.getCalibrateSizeByDeletes();
            boolean calibrateSizeByDeletes = settings.getAsBoolean(INDEX_MERGE_POLICY_CALIBRATE_SIZE_BY_DELETES, oldCalibrateSizeByDeletes);
            if (calibrateSizeByDeletes != oldCalibrateSizeByDeletes) {
                logger.info("updating calibrate_size_by_deletes from [{}] to [{}]", oldCalibrateSizeByDeletes, calibrateSizeByDeletes);
                mergePolicy.setCalibrateSizeByDeletes(calibrateSizeByDeletes);
            }

            double noCFSRatio = parseNoCFSRatio(settings.get(INDEX_COMPOUND_FORMAT, Double.toString(LogDocMergePolicyProvider.this.noCFSRatio)));
            if (noCFSRatio != LogDocMergePolicyProvider.this.noCFSRatio) {
                logger.info("updating index.compound_format from [{}] to [{}]", formatNoCFSRatio(LogDocMergePolicyProvider.this.noCFSRatio), formatNoCFSRatio(noCFSRatio));
                LogDocMergePolicyProvider.this.noCFSRatio = noCFSRatio;
                mergePolicy.setNoCFSRatio(noCFSRatio);
            }
        }
    }
}
