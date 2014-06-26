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

import org.apache.lucene.index.TieredMergePolicy;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.index.store.Store;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

public class TieredMergePolicyProvider extends AbstractMergePolicyProvider<TieredMergePolicy> {

    private final IndexSettingsService indexSettingsService;

    private final Set<CustomTieredMergePolicyProvider> policies = new CopyOnWriteArraySet<>();

    private volatile double forceMergeDeletesPctAllowed;
    private volatile ByteSizeValue floorSegment;
    private volatile int maxMergeAtOnce;
    private volatile int maxMergeAtOnceExplicit;
    private volatile ByteSizeValue maxMergedSegment;
    private volatile double segmentsPerTier;
    private volatile double reclaimDeletesWeight;

    private final ApplySettings applySettings = new ApplySettings();

    

    @Inject
    public TieredMergePolicyProvider(Store store, IndexSettingsService indexSettingsService) {
        super(store);
        this.indexSettingsService = indexSettingsService;
        this.forceMergeDeletesPctAllowed = componentSettings.getAsDouble("expunge_deletes_allowed", 10d); // percentage
        this.floorSegment = componentSettings.getAsBytesSize("floor_segment", new ByteSizeValue(2, ByteSizeUnit.MB));
        this.maxMergeAtOnce = componentSettings.getAsInt("max_merge_at_once", 10);
        this.maxMergeAtOnceExplicit = componentSettings.getAsInt("max_merge_at_once_explicit", 30);
        // TODO is this really a good default number for max_merge_segment, what happens for large indices, won't they end up with many segments?
        this.maxMergedSegment = componentSettings.getAsBytesSize("max_merged_segment", componentSettings.getAsBytesSize("max_merge_segment", new ByteSizeValue(5, ByteSizeUnit.GB)));
        this.segmentsPerTier = componentSettings.getAsDouble("segments_per_tier", 10.0d);
        this.reclaimDeletesWeight = componentSettings.getAsDouble("reclaim_deletes_weight", 2.0d);

        fixSettingsIfNeeded();

        logger.debug("using [tiered] merge policy with expunge_deletes_allowed[{}], floor_segment[{}], max_merge_at_once[{}], max_merge_at_once_explicit[{}], max_merged_segment[{}], segments_per_tier[{}], reclaim_deletes_weight[{}]",
                forceMergeDeletesPctAllowed, floorSegment, maxMergeAtOnce, maxMergeAtOnceExplicit, maxMergedSegment, segmentsPerTier, reclaimDeletesWeight);

        indexSettingsService.addListener(applySettings);
    }
    
    private void fixSettingsIfNeeded() {
        // fixing maxMergeAtOnce, see TieredMergePolicy#setMaxMergeAtOnce
        if (!(segmentsPerTier >= maxMergeAtOnce)) {
            int newMaxMergeAtOnce = (int) segmentsPerTier;
            // max merge at once should be at least 2
            if (newMaxMergeAtOnce <= 1) {
                newMaxMergeAtOnce = 2;
            }
            logger.debug("[tiered] merge policy changing max_merge_at_once from [{}] to [{}] because segments_per_tier [{}] has to be higher or equal to it", maxMergeAtOnce, newMaxMergeAtOnce, segmentsPerTier);
            this.maxMergeAtOnce = newMaxMergeAtOnce;
        }
    }


    @Override
    public TieredMergePolicy newMergePolicy() {
        final CustomTieredMergePolicyProvider mergePolicy = new CustomTieredMergePolicyProvider(this);
        mergePolicy.setNoCFSRatio(noCFSRatio);
        mergePolicy.setForceMergeDeletesPctAllowed(forceMergeDeletesPctAllowed);
        mergePolicy.setFloorSegmentMB(floorSegment.mbFrac());
        mergePolicy.setMaxMergeAtOnce(maxMergeAtOnce);
        mergePolicy.setMaxMergeAtOnceExplicit(maxMergeAtOnceExplicit);
        mergePolicy.setMaxMergedSegmentMB(maxMergedSegment.mbFrac());
        mergePolicy.setSegmentsPerTier(segmentsPerTier);
        mergePolicy.setReclaimDeletesWeight(reclaimDeletesWeight);
        return mergePolicy;
    }

    @Override
    public void close() throws ElasticsearchException {
        indexSettingsService.removeListener(applySettings);
    }

    public static final String INDEX_MERGE_POLICY_EXPUNGE_DELETES_ALLOWED = "index.merge.policy.expunge_deletes_allowed";
    public static final String INDEX_MERGE_POLICY_FLOOR_SEGMENT = "index.merge.policy.floor_segment";
    public static final String INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE = "index.merge.policy.max_merge_at_once";
    public static final String INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_EXPLICIT = "index.merge.policy.max_merge_at_once_explicit";
    public static final String INDEX_MERGE_POLICY_MAX_MERGED_SEGMENT = "index.merge.policy.max_merged_segment";
    public static final String INDEX_MERGE_POLICY_SEGMENTS_PER_TIER = "index.merge.policy.segments_per_tier";
    public static final String INDEX_MERGE_POLICY_RECLAIM_DELETES_WEIGHT = "index.merge.policy.reclaim_deletes_weight";

    class ApplySettings implements IndexSettingsService.Listener {
        @Override
        public void onRefreshSettings(Settings settings) {
            double expungeDeletesPctAllowed = settings.getAsDouble(INDEX_MERGE_POLICY_EXPUNGE_DELETES_ALLOWED, TieredMergePolicyProvider.this.forceMergeDeletesPctAllowed);
            if (expungeDeletesPctAllowed != TieredMergePolicyProvider.this.forceMergeDeletesPctAllowed) {
                logger.info("updating [expunge_deletes_allowed] from [{}] to [{}]", TieredMergePolicyProvider.this.forceMergeDeletesPctAllowed, expungeDeletesPctAllowed);
                TieredMergePolicyProvider.this.forceMergeDeletesPctAllowed = expungeDeletesPctAllowed;
                for (CustomTieredMergePolicyProvider policy : policies) {
                    policy.setForceMergeDeletesPctAllowed(expungeDeletesPctAllowed);
                }
            }

            ByteSizeValue floorSegment = settings.getAsBytesSize(INDEX_MERGE_POLICY_FLOOR_SEGMENT, TieredMergePolicyProvider.this.floorSegment);
            if (!floorSegment.equals(TieredMergePolicyProvider.this.floorSegment)) {
                logger.info("updating [floor_segment] from [{}] to [{}]", TieredMergePolicyProvider.this.floorSegment, floorSegment);
                TieredMergePolicyProvider.this.floorSegment = floorSegment;
                for (CustomTieredMergePolicyProvider policy : policies) {
                    policy.setFloorSegmentMB(floorSegment.mbFrac());
                }
            }

            int maxMergeAtOnce = settings.getAsInt(INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE, TieredMergePolicyProvider.this.maxMergeAtOnce);
            if (maxMergeAtOnce != TieredMergePolicyProvider.this.maxMergeAtOnce) {
                logger.info("updating [max_merge_at_once] from [{}] to [{}]", TieredMergePolicyProvider.this.maxMergeAtOnce, maxMergeAtOnce);
                TieredMergePolicyProvider.this.maxMergeAtOnce = maxMergeAtOnce;
                for (CustomTieredMergePolicyProvider policy : policies) {
                    policy.setMaxMergeAtOnce(maxMergeAtOnce);
                }
            }

            int maxMergeAtOnceExplicit = settings.getAsInt(INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_EXPLICIT, TieredMergePolicyProvider.this.maxMergeAtOnceExplicit);
            if (maxMergeAtOnceExplicit != TieredMergePolicyProvider.this.maxMergeAtOnceExplicit) {
                logger.info("updating [max_merge_at_once_explicit] from [{}] to [{}]", TieredMergePolicyProvider.this.maxMergeAtOnceExplicit, maxMergeAtOnceExplicit);
                TieredMergePolicyProvider.this.maxMergeAtOnceExplicit = maxMergeAtOnceExplicit;
                for (CustomTieredMergePolicyProvider policy : policies) {
                    policy.setMaxMergeAtOnceExplicit(maxMergeAtOnceExplicit);
                }
            }

            ByteSizeValue maxMergedSegment = settings.getAsBytesSize(INDEX_MERGE_POLICY_MAX_MERGED_SEGMENT, TieredMergePolicyProvider.this.maxMergedSegment);
            if (!maxMergedSegment.equals(TieredMergePolicyProvider.this.maxMergedSegment)) {
                logger.info("updating [max_merged_segment] from [{}] to [{}]", TieredMergePolicyProvider.this.maxMergedSegment, maxMergedSegment);
                TieredMergePolicyProvider.this.maxMergedSegment = maxMergedSegment;
                for (CustomTieredMergePolicyProvider policy : policies) {
                    policy.setFloorSegmentMB(maxMergedSegment.mbFrac());
                }
            }

            double segmentsPerTier = settings.getAsDouble(INDEX_MERGE_POLICY_SEGMENTS_PER_TIER, TieredMergePolicyProvider.this.segmentsPerTier);
            if (segmentsPerTier != TieredMergePolicyProvider.this.segmentsPerTier) {
                logger.info("updating [segments_per_tier] from [{}] to [{}]", TieredMergePolicyProvider.this.segmentsPerTier, segmentsPerTier);
                TieredMergePolicyProvider.this.segmentsPerTier = segmentsPerTier;
                for (CustomTieredMergePolicyProvider policy : policies) {
                    policy.setSegmentsPerTier(segmentsPerTier);
                }
            }

            double reclaimDeletesWeight = settings.getAsDouble(INDEX_MERGE_POLICY_RECLAIM_DELETES_WEIGHT, TieredMergePolicyProvider.this.reclaimDeletesWeight);
            if (reclaimDeletesWeight != TieredMergePolicyProvider.this.reclaimDeletesWeight) {
                logger.info("updating [reclaim_deletes_weight] from [{}] to [{}]", TieredMergePolicyProvider.this.reclaimDeletesWeight, reclaimDeletesWeight);
                TieredMergePolicyProvider.this.reclaimDeletesWeight = reclaimDeletesWeight;
                for (CustomTieredMergePolicyProvider policy : policies) {
                    policy.setReclaimDeletesWeight(reclaimDeletesWeight);
                }
            }

            final double noCFSRatio = parseNoCFSRatio(settings.get(INDEX_COMPOUND_FORMAT, Double.toString(TieredMergePolicyProvider.this.noCFSRatio)));
            if (noCFSRatio != TieredMergePolicyProvider.this.noCFSRatio) {
                logger.info("updating index.compound_format from [{}] to [{}]", formatNoCFSRatio(TieredMergePolicyProvider.this.noCFSRatio), formatNoCFSRatio(noCFSRatio));
                TieredMergePolicyProvider.this.noCFSRatio = noCFSRatio;
                for (CustomTieredMergePolicyProvider policy : policies) {
                    policy.setNoCFSRatio(noCFSRatio);
                }
            }

            fixSettingsIfNeeded();
        }
    }

    public static class CustomTieredMergePolicyProvider extends TieredMergePolicy {

        private final TieredMergePolicyProvider provider;

        public CustomTieredMergePolicyProvider(TieredMergePolicyProvider provider) {
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