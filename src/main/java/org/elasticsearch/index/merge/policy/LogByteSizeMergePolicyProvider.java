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

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 *
 */
public class LogByteSizeMergePolicyProvider extends AbstractMergePolicyProvider<LogByteSizeMergePolicy> {

    private final IndexSettingsService indexSettingsService;
    public static final String MAX_MERGE_BYTE_SIZE_KEY = "index.merge.policy.max_merge_sizes";
    public static final String MIN_MERGE_BYTE_SIZE_KEY = "index.merge.policy.min_merge_size";
    public static final String MERGE_FACTORY_KEY = "index.merge.policy.merge_factor";
    private volatile ByteSizeValue minMergeSize;
    private volatile ByteSizeValue maxMergeSize;
    private volatile int mergeFactor;
    private volatile int maxMergeDocs;
    private final boolean calibrateSizeByDeletes;

    private final Set<CustomLogByteSizeMergePolicy> policies = new CopyOnWriteArraySet<>();

    private final ApplySettings applySettings = new ApplySettings();

    @Inject
    public LogByteSizeMergePolicyProvider(Store store, IndexSettingsService indexSettingsService) {
        super(store);
        Preconditions.checkNotNull(store, "Store must be provided to merge policy");
        this.indexSettingsService = indexSettingsService;
        this.minMergeSize = componentSettings.getAsBytesSize("min_merge_size", new ByteSizeValue((long) (LogByteSizeMergePolicy.DEFAULT_MIN_MERGE_MB * 1024 * 1024), ByteSizeUnit.BYTES));
        this.maxMergeSize = componentSettings.getAsBytesSize("max_merge_size", new ByteSizeValue((long) LogByteSizeMergePolicy.DEFAULT_MAX_MERGE_MB, ByteSizeUnit.MB));
        this.mergeFactor = componentSettings.getAsInt("merge_factor", LogByteSizeMergePolicy.DEFAULT_MERGE_FACTOR);
        this.maxMergeDocs = componentSettings.getAsInt("max_merge_docs", LogByteSizeMergePolicy.DEFAULT_MAX_MERGE_DOCS);
        this.calibrateSizeByDeletes = componentSettings.getAsBoolean("calibrate_size_by_deletes", true);
        logger.debug("using [log_bytes_size] merge policy with merge_factor[{}], min_merge_size[{}], max_merge_size[{}], max_merge_docs[{}], calibrate_size_by_deletes[{}]",
                mergeFactor, minMergeSize, maxMergeSize, maxMergeDocs, calibrateSizeByDeletes);

        indexSettingsService.addListener(applySettings);
    }

    @Override
    public LogByteSizeMergePolicy newMergePolicy() {
        final CustomLogByteSizeMergePolicy  mergePolicy = new CustomLogByteSizeMergePolicy(this);
        mergePolicy.setMinMergeMB(minMergeSize.mbFrac());
        mergePolicy.setMaxMergeMB(maxMergeSize.mbFrac());
        mergePolicy.setMergeFactor(mergeFactor);
        mergePolicy.setMaxMergeDocs(maxMergeDocs);
        mergePolicy.setCalibrateSizeByDeletes(calibrateSizeByDeletes);
        mergePolicy.setNoCFSRatio(noCFSRatio);

        policies.add(mergePolicy);
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

    class ApplySettings implements IndexSettingsService.Listener {
        @Override
        public void onRefreshSettings(Settings settings) {
            ByteSizeValue minMergeSize = settings.getAsBytesSize(INDEX_MERGE_POLICY_MIN_MERGE_SIZE, LogByteSizeMergePolicyProvider.this.minMergeSize);
            if (!minMergeSize.equals(LogByteSizeMergePolicyProvider.this.minMergeSize)) {
                logger.info("updating min_merge_size from [{}] to [{}]", LogByteSizeMergePolicyProvider.this.minMergeSize, minMergeSize);
                LogByteSizeMergePolicyProvider.this.minMergeSize = minMergeSize;
                for (CustomLogByteSizeMergePolicy policy : policies) {
                    policy.setMinMergeMB(minMergeSize.mbFrac());
                }
            }

            ByteSizeValue maxMergeSize = settings.getAsBytesSize(INDEX_MERGE_POLICY_MAX_MERGE_SIZE, LogByteSizeMergePolicyProvider.this.maxMergeSize);
            if (!maxMergeSize.equals(LogByteSizeMergePolicyProvider.this.maxMergeSize)) {
                logger.info("updating max_merge_size from [{}] to [{}]", LogByteSizeMergePolicyProvider.this.maxMergeSize, maxMergeSize);
                LogByteSizeMergePolicyProvider.this.maxMergeSize = maxMergeSize;
                for (CustomLogByteSizeMergePolicy policy : policies) {
                    policy.setMaxMergeMB(maxMergeSize.mbFrac());
                }
            }

            int maxMergeDocs = settings.getAsInt(INDEX_MERGE_POLICY_MAX_MERGE_DOCS, LogByteSizeMergePolicyProvider.this.maxMergeDocs);
            if (maxMergeDocs != LogByteSizeMergePolicyProvider.this.maxMergeDocs) {
                logger.info("updating max_merge_docs from [{}] to [{}]", LogByteSizeMergePolicyProvider.this.maxMergeDocs, maxMergeDocs);
                LogByteSizeMergePolicyProvider.this.maxMergeDocs = maxMergeDocs;
                for (CustomLogByteSizeMergePolicy policy : policies) {
                    policy.setMaxMergeDocs(maxMergeDocs);
                }
            }

            int mergeFactor = settings.getAsInt(INDEX_MERGE_POLICY_MERGE_FACTOR, LogByteSizeMergePolicyProvider.this.mergeFactor);
            if (mergeFactor != LogByteSizeMergePolicyProvider.this.mergeFactor) {
                logger.info("updating merge_factor from [{}] to [{}]", LogByteSizeMergePolicyProvider.this.mergeFactor, mergeFactor);
                LogByteSizeMergePolicyProvider.this.mergeFactor = mergeFactor;
                for (CustomLogByteSizeMergePolicy policy : policies) {
                    policy.setMergeFactor(mergeFactor);
                }
            }
            
            final double noCFSRatio = parseNoCFSRatio(settings.get(INDEX_COMPOUND_FORMAT, Double.toString(LogByteSizeMergePolicyProvider.this.noCFSRatio)));
            if (noCFSRatio != LogByteSizeMergePolicyProvider.this.noCFSRatio) {
                logger.info("updating index.compound_format from [{}] to [{}]", formatNoCFSRatio(LogByteSizeMergePolicyProvider.this.noCFSRatio), formatNoCFSRatio(noCFSRatio));
                LogByteSizeMergePolicyProvider.this.noCFSRatio = noCFSRatio;
                for (CustomLogByteSizeMergePolicy policy : policies) {
                    policy.setNoCFSRatio(noCFSRatio);
                }
            }

        }
    }

    public static class CustomLogByteSizeMergePolicy extends LogByteSizeMergePolicy {

        private final LogByteSizeMergePolicyProvider provider;

        public CustomLogByteSizeMergePolicy(LogByteSizeMergePolicyProvider provider) {
            super();
            this.provider = provider;
        }

        @Override
        public void close() {
            super.close();
            provider.policies.remove(this);
        }
    }

}
