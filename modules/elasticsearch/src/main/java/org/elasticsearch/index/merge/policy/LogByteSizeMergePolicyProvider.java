/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.Preconditions;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.store.Store;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * @author kimchy (shay.banon)
 */
public class LogByteSizeMergePolicyProvider extends AbstractIndexShardComponent implements MergePolicyProvider<LogByteSizeMergePolicy> {

    private final IndexSettingsService indexSettingsService;

    private volatile boolean compoundFormat;
    private volatile ByteSizeValue minMergeSize;
    private volatile ByteSizeValue maxMergeSize;
    private volatile int mergeFactor;
    private volatile int maxMergeDocs;
    private final boolean calibrateSizeByDeletes;
    private boolean asyncMerge;

    private final Set<CustomLogByteSizeMergePolicy> policies = new CopyOnWriteArraySet<CustomLogByteSizeMergePolicy>();

    private final ApplySettings applySettings = new ApplySettings();

    @Inject public LogByteSizeMergePolicyProvider(Store store, IndexSettingsService indexSettingsService) {
        super(store.shardId(), store.indexSettings());
        Preconditions.checkNotNull(store, "Store must be provided to merge policy");
        this.indexSettingsService = indexSettingsService;

        this.compoundFormat = indexSettings.getAsBoolean("index.compound_format", store.suggestUseCompoundFile());
        this.minMergeSize = componentSettings.getAsBytesSize("min_merge_size", new ByteSizeValue((long) (LogByteSizeMergePolicy.DEFAULT_MIN_MERGE_MB * 1024 * 1024), ByteSizeUnit.BYTES));
        this.maxMergeSize = componentSettings.getAsBytesSize("max_merge_size", new ByteSizeValue((long) LogByteSizeMergePolicy.DEFAULT_MAX_MERGE_MB, ByteSizeUnit.MB));
        this.mergeFactor = componentSettings.getAsInt("merge_factor", LogByteSizeMergePolicy.DEFAULT_MERGE_FACTOR);
        this.maxMergeDocs = componentSettings.getAsInt("max_merge_docs", LogByteSizeMergePolicy.DEFAULT_MAX_MERGE_DOCS);
        this.calibrateSizeByDeletes = componentSettings.getAsBoolean("calibrate_size_by_deletes", true);
        this.asyncMerge = indexSettings.getAsBoolean("index.merge.async", true);
        logger.debug("using [log_bytes_size] merge policy with merge_factor[{}], min_merge_size[{}], max_merge_size[{}], max_merge_docs[{}], calibrate_size_by_deletes[{}], async_merge[{}]",
                mergeFactor, minMergeSize, maxMergeSize, maxMergeDocs, calibrateSizeByDeletes, asyncMerge);

        indexSettingsService.addListener(applySettings);
    }

    @Override public LogByteSizeMergePolicy newMergePolicy() {
        CustomLogByteSizeMergePolicy mergePolicy;
        if (asyncMerge) {
            mergePolicy = new CustomLogByteSizeMergePolicy(this);
        } else {
            mergePolicy = new CustomLogByteSizeMergePolicy(this);
        }
        mergePolicy.setMinMergeMB(minMergeSize.mbFrac());
        mergePolicy.setMaxMergeMB(maxMergeSize.mbFrac());
        mergePolicy.setMergeFactor(mergeFactor);
        mergePolicy.setMaxMergeDocs(maxMergeDocs);
        mergePolicy.setCalibrateSizeByDeletes(calibrateSizeByDeletes);
        mergePolicy.setUseCompoundFile(compoundFormat);

        policies.add(mergePolicy);
        return mergePolicy;
    }

    @Override public void close(boolean delete) throws ElasticSearchException {
        indexSettingsService.removeListener(applySettings);
    }

    class ApplySettings implements IndexSettingsService.Listener {
        @Override public void onRefreshSettings(Settings settings) {
            ByteSizeValue minMergeSize = settings.getAsBytesSize("index.merge.policy.min_merge_size", LogByteSizeMergePolicyProvider.this.minMergeSize);
            if (!minMergeSize.equals(LogByteSizeMergePolicyProvider.this.minMergeSize)) {
                logger.info("updating min_merge_size from [{}] to [{}]", LogByteSizeMergePolicyProvider.this.minMergeSize, minMergeSize);
                LogByteSizeMergePolicyProvider.this.minMergeSize = minMergeSize;
                for (CustomLogByteSizeMergePolicy policy : policies) {
                    policy.setMinMergeMB(minMergeSize.mbFrac());
                }
            }

            ByteSizeValue maxMergeSize = settings.getAsBytesSize("index.merge.policy.max_merge_size", LogByteSizeMergePolicyProvider.this.maxMergeSize);
            if (!maxMergeSize.equals(LogByteSizeMergePolicyProvider.this.maxMergeSize)) {
                logger.info("updating max_merge_size from [{}] to [{}]", LogByteSizeMergePolicyProvider.this.maxMergeSize, maxMergeSize);
                LogByteSizeMergePolicyProvider.this.maxMergeSize = maxMergeSize;
                for (CustomLogByteSizeMergePolicy policy : policies) {
                    policy.setMaxMergeMB(maxMergeSize.mbFrac());
                }
            }

            int maxMergeDocs = settings.getAsInt("index.merge.policy.max_merge_docs", LogByteSizeMergePolicyProvider.this.maxMergeDocs);
            if (maxMergeDocs != LogByteSizeMergePolicyProvider.this.maxMergeDocs) {
                logger.info("updating max_merge_docs from [{}] to [{}]", LogByteSizeMergePolicyProvider.this.maxMergeDocs, maxMergeDocs);
                LogByteSizeMergePolicyProvider.this.maxMergeDocs = maxMergeDocs;
                for (CustomLogByteSizeMergePolicy policy : policies) {
                    policy.setMaxMergeDocs(maxMergeDocs);
                }
            }

            int mergeFactor = settings.getAsInt("index.merge.policy.merge_factor", LogByteSizeMergePolicyProvider.this.mergeFactor);
            if (mergeFactor != LogByteSizeMergePolicyProvider.this.mergeFactor) {
                logger.info("updating merge_factor from [{}] to [{}]", LogByteSizeMergePolicyProvider.this.mergeFactor, mergeFactor);
                LogByteSizeMergePolicyProvider.this.mergeFactor = mergeFactor;
                for (CustomLogByteSizeMergePolicy policy : policies) {
                    policy.setMergeFactor(mergeFactor);
                }
            }

            boolean compoundFormat = settings.getAsBoolean("index.compound_format", LogByteSizeMergePolicyProvider.this.compoundFormat);
            if (compoundFormat != LogByteSizeMergePolicyProvider.this.compoundFormat) {
                logger.info("updating index.compound_format from [{}] to [{}]", LogByteSizeMergePolicyProvider.this.compoundFormat, compoundFormat);
                LogByteSizeMergePolicyProvider.this.compoundFormat = compoundFormat;
                for (CustomLogByteSizeMergePolicy policy : policies) {
                    policy.setUseCompoundFile(compoundFormat);
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

        @Override public void close() {
            super.close();
            provider.policies.remove(this);
        }
    }

    public static class EnableMergeLogByteSizeMergePolicy extends CustomLogByteSizeMergePolicy implements EnableMergePolicy {

        private final ThreadLocal<Boolean> enableMerge = new ThreadLocal<Boolean>() {
            @Override protected Boolean initialValue() {
                return Boolean.FALSE;
            }
        };

        public EnableMergeLogByteSizeMergePolicy(LogByteSizeMergePolicyProvider provider) {
            super(provider);
        }

        @Override public void enableMerge() {
            enableMerge.set(Boolean.TRUE);
        }

        @Override public void disableMerge() {
            enableMerge.set(Boolean.FALSE);
        }

        @Override public boolean isMergeEnabled() {
            return enableMerge.get() == Boolean.TRUE;
        }

        @Override public void close() {
            enableMerge.remove();
            super.close();
        }

        @Override public MergeSpecification findMerges(SegmentInfos infos) throws IOException {
            if (enableMerge.get() == Boolean.FALSE) {
                return null;
            }
            return super.findMerges(infos);
        }

        @Override public MergeSpecification findMergesToExpungeDeletes(SegmentInfos segmentInfos) throws CorruptIndexException, IOException {
            if (enableMerge.get() == Boolean.FALSE) {
                return null;
            }
            return super.findMergesToExpungeDeletes(segmentInfos);
        }

        @Override public MergeSpecification findMergesForOptimize(SegmentInfos infos, int maxNumSegments, Set<SegmentInfo> segmentsToOptimize) throws IOException {
            if (enableMerge.get() == Boolean.FALSE) {
                return null;
            }
            return super.findMergesForOptimize(infos, maxNumSegments, segmentsToOptimize);
        }
    }
}
