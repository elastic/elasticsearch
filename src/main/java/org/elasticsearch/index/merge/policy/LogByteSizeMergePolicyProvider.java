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

import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Preconditions;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.index.store.Store;

/**
 *
 */
public class LogByteSizeMergePolicyProvider extends AbstractMergePolicyProvider<LogByteSizeMergePolicy> {

    private final IndexSettingsService indexSettingsService;
    private final ApplySettings applySettings = new ApplySettings();
    private final LogByteSizeMergePolicy mergePolicy = new LogByteSizeMergePolicy();

    public static final ByteSizeValue DEFAULT_MIN_MERGE_SIZE = new ByteSizeValue((long) (LogByteSizeMergePolicy.DEFAULT_MIN_MERGE_MB * 1024 * 1024), ByteSizeUnit.BYTES);
    public static final ByteSizeValue DEFAULT_MAX_MERGE_SIZE = new ByteSizeValue((long) LogByteSizeMergePolicy.DEFAULT_MAX_MERGE_MB, ByteSizeUnit.MB);

    @Inject
    public LogByteSizeMergePolicyProvider(Store store, IndexSettingsService indexSettingsService) {
        super(store);
        Preconditions.checkNotNull(store, "Store must be provided to merge policy");
        this.indexSettingsService = indexSettingsService;

        ByteSizeValue minMergeSize = indexSettings.getAsBytesSize("index.merge.policy.min_merge_size", DEFAULT_MIN_MERGE_SIZE);
        ByteSizeValue maxMergeSize = indexSettings.getAsBytesSize("index.merge.policy.max_merge_size", DEFAULT_MAX_MERGE_SIZE);
        int mergeFactor = indexSettings.getAsInt("index.merge.policy.merge_factor", LogByteSizeMergePolicy.DEFAULT_MERGE_FACTOR);
        int maxMergeDocs = indexSettings.getAsInt("index.merge.policy.max_merge_docs", LogByteSizeMergePolicy.DEFAULT_MAX_MERGE_DOCS);
        boolean calibrateSizeByDeletes = indexSettings.getAsBoolean("index.merge.policy.calibrate_size_by_deletes", true);

        mergePolicy.setMinMergeMB(minMergeSize.mbFrac());
        mergePolicy.setMaxMergeMB(maxMergeSize.mbFrac());
        mergePolicy.setMergeFactor(mergeFactor);
        mergePolicy.setMaxMergeDocs(maxMergeDocs);
        mergePolicy.setCalibrateSizeByDeletes(calibrateSizeByDeletes);
        mergePolicy.setNoCFSRatio(noCFSRatio);
        logger.debug("using [log_bytes_size] merge policy with merge_factor[{}], min_merge_size[{}], max_merge_size[{}], max_merge_docs[{}], calibrate_size_by_deletes[{}]",
                mergeFactor, minMergeSize, maxMergeSize, maxMergeDocs, calibrateSizeByDeletes);

        indexSettingsService.addListener(applySettings);
    }

    @Override
    public LogByteSizeMergePolicy getMergePolicy() {
        return mergePolicy;
    }

    @Override
    public void close() throws ElasticsearchException {
        indexSettingsService.removeListener(applySettings);
    }

    public static final String INDEX_MERGE_POLICY_MIN_MERGE_SIZE = "index.merge.policy.min_merge_size";
    public static final String INDEX_MERGE_POLICY_MAX_MERGE_SIZE = "index.merge.policy.max_merge_size";
    public static final String INDEX_MERGE_POLICY_MAX_MERGE_DOCS = "index.merge.policy.max_merge_docs";
    public static final String INDEX_MERGE_POLICY_MERGE_FACTOR = "index.merge.policy.merge_factor";
    public static final String INDEX_MERGE_POLICY_CALIBRATE_SIZE_BY_DELETES = "index.merge.policy.calibrate_size_by_deletes";

    class ApplySettings implements IndexSettingsService.Listener {
        @Override
        public void onRefreshSettings(Settings settings) {
            double oldMinMergeSizeMB = mergePolicy.getMinMergeMB();
            ByteSizeValue minMergeSize = settings.getAsBytesSize(INDEX_MERGE_POLICY_MIN_MERGE_SIZE, null);
            if (minMergeSize != null && minMergeSize.mbFrac() != oldMinMergeSizeMB) {
                logger.info("updating min_merge_size from [{}mb] to [{}]", oldMinMergeSizeMB, minMergeSize);
                mergePolicy.setMinMergeMB(minMergeSize.mbFrac());
            }

            double oldMaxMergeSizeMB = mergePolicy.getMaxMergeMB();
            ByteSizeValue maxMergeSize = settings.getAsBytesSize(INDEX_MERGE_POLICY_MAX_MERGE_SIZE, null);
            if (maxMergeSize != null && maxMergeSize.mbFrac() != oldMaxMergeSizeMB) {
                logger.info("updating max_merge_size from [{}mb] to [{}]", oldMaxMergeSizeMB, maxMergeSize);
                mergePolicy.setMaxMergeMB(maxMergeSize.mbFrac());
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
            
            final double noCFSRatio = parseNoCFSRatio(settings.get(INDEX_COMPOUND_FORMAT, Double.toString(LogByteSizeMergePolicyProvider.this.noCFSRatio)));
            if (noCFSRatio != LogByteSizeMergePolicyProvider.this.noCFSRatio) {
                logger.info("updating index.compound_format from [{}] to [{}]", formatNoCFSRatio(LogByteSizeMergePolicyProvider.this.noCFSRatio), formatNoCFSRatio(noCFSRatio));
                LogByteSizeMergePolicyProvider.this.noCFSRatio = noCFSRatio;
                mergePolicy.setNoCFSRatio(noCFSRatio);
            }
        }
    }
}
