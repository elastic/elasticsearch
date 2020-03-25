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

package org.elasticsearch.index;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.TieredMergePolicy;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;

/**
 * A shard in elasticsearch is a Lucene index, and a Lucene index is broken
 * down into segments. Segments are internal storage elements in the index
 * where the index data is stored, and are immutable up to delete markers.
 * Segments are, periodically, merged into larger segments to keep the
 * index size at bay and expunge deletes.
 *
 * <p>
 * Merges select segments of approximately equal size, subject to an allowed
 * number of segments per tier. The merge policy is able to merge
 * non-adjacent segments, and separates how many segments are merged at once from how many
 * segments are allowed per tier. It also does not over-merge (i.e., cascade merges).
 *
 * <p>
 * All merge policy settings are <b>dynamic</b> and can be updated on a live index.
 * The merge policy has the following settings:
 *
 * <ul>
 * <li><code>index.merge.policy.expunge_deletes_allowed</code>:
 *
 *     When expungeDeletes is called, we only merge away a segment if its delete
 *     percentage is over this threshold. Default is <code>10</code>.
 *
 * <li><code>index.merge.policy.floor_segment</code>:
 *
 *     Segments smaller than this are "rounded up" to this size, i.e. treated as
 *     equal (floor) size for merge selection. This is to prevent frequent
 *     flushing of tiny segments, thus preventing a long tail in the index. Default
 *     is <code>2mb</code>.
 *
 * <li><code>index.merge.policy.max_merge_at_once</code>:
 *
 *     Maximum number of segments to be merged at a time during "normal" merging.
 *     Default is <code>10</code>.
 *
 * <li><code>index.merge.policy.max_merge_at_once_explicit</code>:
 *
 *     Maximum number of segments to be merged at a time, during force merge or
 *     expungeDeletes. Default is <code>30</code>.
 *
 * <li><code>index.merge.policy.max_merged_segment</code>:
 *
 *     Maximum sized segment to produce during normal merging (not explicit
 *     force merge). This setting is approximate: the estimate of the merged
 *     segment size is made by summing sizes of to-be-merged segments
 *     (compensating for percent deleted docs). Default is <code>5gb</code>.
 *
 * <li><code>index.merge.policy.segments_per_tier</code>:
 *
 *     Sets the allowed number of segments per tier. Smaller values mean more
 *     merging but fewer segments. Default is <code>10</code>. Note, this value needs to be
 *     &gt;= than the <code>max_merge_at_once</code> otherwise you'll force too many merges to
 *     occur.
 *
 * <li><code>index.merge.policy.deletes_pct_allowed</code>:
 *
 *     Controls the maximum percentage of deleted documents that is tolerated in
 *     the index. Lower values make the index more space efficient at the
 *     expense of increased CPU and I/O activity. Values must be between <code>20</code> and
 *     <code>50</code>. Default value is <code>33</code>.
 * </ul>
 *
 * <p>
 * For normal merging, the policy first computes a "budget" of how many
 * segments are allowed to be in the index. If the index is over-budget,
 * then the policy sorts segments by decreasing size (proportionally considering percent
 * deletes), and then finds the least-cost merge. Merge cost is measured by
 * a combination of the "skew" of the merge (size of largest seg divided by
 * smallest seg), total merge size and pct deletes reclaimed, so that
 * merges with lower skew, smaller size and those reclaiming more deletes,
 * are favored.
 *
 * <p>
 * If a merge will produce a segment that's larger than
 * <code>max_merged_segment</code> then the policy will merge fewer segments (down to
 * 1 at once, if that one has deletions) to keep the segment size under
 * budget.
 *
 * <p>
 * Note, this can mean that for large shards that holds many gigabytes of
 * data, the default of <code>max_merged_segment</code> (<code>5gb</code>) can cause for many
 * segments to be in an index, and causing searches to be slower. Use the
 * indices segments API to see the segments that an index has, and
 * possibly either increase the <code>max_merged_segment</code> or issue an optimize
 * call for the index (try and aim to issue it on a low traffic time).
 */

public final class MergePolicyConfig {
    private final EsTieredMergePolicy mergePolicy = new EsTieredMergePolicy();
    private final Logger logger;
    private final boolean mergesEnabled;

    public static final double          DEFAULT_EXPUNGE_DELETES_ALLOWED     = 10d;
    public static final ByteSizeValue   DEFAULT_FLOOR_SEGMENT               = new ByteSizeValue(2, ByteSizeUnit.MB);
    public static final int             DEFAULT_MAX_MERGE_AT_ONCE           = 10;
    public static final int             DEFAULT_MAX_MERGE_AT_ONCE_EXPLICIT  = 30;
    public static final ByteSizeValue   DEFAULT_MAX_MERGED_SEGMENT          = new ByteSizeValue(5, ByteSizeUnit.GB);
    public static final double          DEFAULT_SEGMENTS_PER_TIER           = 10.0d;
    public static final double          DEFAULT_RECLAIM_DELETES_WEIGHT      = 2.0d;
    public static final double          DEFAULT_DELETES_PCT_ALLOWED         = 33.0d;
    public static final Setting<Double> INDEX_COMPOUND_FORMAT_SETTING       =
        new Setting<>("index.compound_format", Double.toString(TieredMergePolicy.DEFAULT_NO_CFS_RATIO),
            MergePolicyConfig::parseNoCFSRatio, Property.Dynamic, Property.IndexScope);

    public static final Setting<Double> INDEX_MERGE_POLICY_EXPUNGE_DELETES_ALLOWED_SETTING =
        Setting.doubleSetting("index.merge.policy.expunge_deletes_allowed", DEFAULT_EXPUNGE_DELETES_ALLOWED, 0.0d,
            Property.Dynamic, Property.IndexScope);
    public static final Setting<ByteSizeValue> INDEX_MERGE_POLICY_FLOOR_SEGMENT_SETTING =
        Setting.byteSizeSetting("index.merge.policy.floor_segment", DEFAULT_FLOOR_SEGMENT,
            Property.Dynamic, Property.IndexScope);
    public static final Setting<Integer> INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_SETTING =
        Setting.intSetting("index.merge.policy.max_merge_at_once", DEFAULT_MAX_MERGE_AT_ONCE, 2,
            Property.Dynamic, Property.IndexScope);
    public static final Setting<Integer> INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_EXPLICIT_SETTING =
        Setting.intSetting("index.merge.policy.max_merge_at_once_explicit", DEFAULT_MAX_MERGE_AT_ONCE_EXPLICIT, 2,
            Property.Dynamic, Property.IndexScope);
    public static final Setting<ByteSizeValue> INDEX_MERGE_POLICY_MAX_MERGED_SEGMENT_SETTING =
        Setting.byteSizeSetting("index.merge.policy.max_merged_segment", DEFAULT_MAX_MERGED_SEGMENT,
            Property.Dynamic, Property.IndexScope);
    public static final Setting<Double> INDEX_MERGE_POLICY_SEGMENTS_PER_TIER_SETTING =
        Setting.doubleSetting("index.merge.policy.segments_per_tier", DEFAULT_SEGMENTS_PER_TIER, 2.0d,
            Property.Dynamic, Property.IndexScope);
    public static final Setting<Double> INDEX_MERGE_POLICY_RECLAIM_DELETES_WEIGHT_SETTING =
        Setting.doubleSetting("index.merge.policy.reclaim_deletes_weight", DEFAULT_RECLAIM_DELETES_WEIGHT, 0.0d,
            Property.Dynamic, Property.IndexScope, Property.Deprecated);
    public static final Setting<Double> INDEX_MERGE_POLICY_DELETES_PCT_ALLOWED_SETTING =
        Setting.doubleSetting("index.merge.policy.deletes_pct_allowed", DEFAULT_DELETES_PCT_ALLOWED, 20.0d, 50.0d,
            Property.Dynamic, Property.IndexScope);
    // don't convert to Setting<> and register... we only set this in tests and register via a plugin
    public static final String INDEX_MERGE_ENABLED = "index.merge.enabled";

    MergePolicyConfig(Logger logger, IndexSettings indexSettings) {
        this.logger = logger;
        double forceMergeDeletesPctAllowed = indexSettings.getValue(INDEX_MERGE_POLICY_EXPUNGE_DELETES_ALLOWED_SETTING); // percentage
        ByteSizeValue floorSegment = indexSettings.getValue(INDEX_MERGE_POLICY_FLOOR_SEGMENT_SETTING);
        int maxMergeAtOnce = indexSettings.getValue(INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_SETTING);
        int maxMergeAtOnceExplicit = indexSettings.getValue(INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_EXPLICIT_SETTING);
        // TODO is this really a good default number for max_merge_segment, what happens for large indices,
        // won't they end up with many segments?
        ByteSizeValue maxMergedSegment = indexSettings.getValue(INDEX_MERGE_POLICY_MAX_MERGED_SEGMENT_SETTING);
        double segmentsPerTier = indexSettings.getValue(INDEX_MERGE_POLICY_SEGMENTS_PER_TIER_SETTING);
        double reclaimDeletesWeight = indexSettings.getValue(INDEX_MERGE_POLICY_RECLAIM_DELETES_WEIGHT_SETTING);
        double deletesPctAllowed = indexSettings.getValue(INDEX_MERGE_POLICY_DELETES_PCT_ALLOWED_SETTING);
        this.mergesEnabled = indexSettings.getSettings().getAsBoolean(INDEX_MERGE_ENABLED, true);
        if (mergesEnabled == false) {
            logger.warn("[{}] is set to false, this should only be used in tests and can cause serious problems in production" +
                " environments", INDEX_MERGE_ENABLED);
        }
        maxMergeAtOnce = adjustMaxMergeAtOnceIfNeeded(maxMergeAtOnce, segmentsPerTier);
        mergePolicy.setNoCFSRatio(indexSettings.getValue(INDEX_COMPOUND_FORMAT_SETTING));
        mergePolicy.setForceMergeDeletesPctAllowed(forceMergeDeletesPctAllowed);
        mergePolicy.setFloorSegmentMB(floorSegment.getMbFrac());
        mergePolicy.setMaxMergeAtOnce(maxMergeAtOnce);
        mergePolicy.setMaxMergeAtOnceExplicit(maxMergeAtOnceExplicit);
        mergePolicy.setMaxMergedSegmentMB(maxMergedSegment.getMbFrac());
        mergePolicy.setSegmentsPerTier(segmentsPerTier);
        mergePolicy.setDeletesPctAllowed(deletesPctAllowed);
        if (logger.isTraceEnabled()) {
            logger.trace("using [tiered] merge mergePolicy with expunge_deletes_allowed[{}], floor_segment[{}]," +
                    " max_merge_at_once[{}], max_merge_at_once_explicit[{}], max_merged_segment[{}], segments_per_tier[{}]," +
                    " deletes_pct_allowed[{}]",
                forceMergeDeletesPctAllowed, floorSegment, maxMergeAtOnce, maxMergeAtOnceExplicit, maxMergedSegment, segmentsPerTier,
                deletesPctAllowed);
        }
    }

    void setSegmentsPerTier(Double segmentsPerTier) {
        mergePolicy.setSegmentsPerTier(segmentsPerTier);
    }

    void setMaxMergedSegment(ByteSizeValue maxMergedSegment) {
        mergePolicy.setMaxMergedSegmentMB(maxMergedSegment.getMbFrac());
    }

    void setMaxMergesAtOnceExplicit(Integer maxMergeAtOnceExplicit) {
        mergePolicy.setMaxMergeAtOnceExplicit(maxMergeAtOnceExplicit);
    }

    void setMaxMergesAtOnce(Integer maxMergeAtOnce) {
        mergePolicy.setMaxMergeAtOnce(maxMergeAtOnce);
    }

    void setFloorSegmentSetting(ByteSizeValue floorSegementSetting) {
        mergePolicy.setFloorSegmentMB(floorSegementSetting.getMbFrac());
    }

    void setExpungeDeletesAllowed(Double value) {
        mergePolicy.setForceMergeDeletesPctAllowed(value);
    }

    void setNoCFSRatio(Double noCFSRatio) {
        mergePolicy.setNoCFSRatio(noCFSRatio);
    }

    void setDeletesPctAllowed(Double deletesPctAllowed) {
        mergePolicy.setDeletesPctAllowed(deletesPctAllowed);
    }

    private int adjustMaxMergeAtOnceIfNeeded(int maxMergeAtOnce, double segmentsPerTier) {
        // fixing maxMergeAtOnce, see TieredMergePolicy#setMaxMergeAtOnce
        if (!(segmentsPerTier >= maxMergeAtOnce)) {
            int newMaxMergeAtOnce = (int) segmentsPerTier;
            // max merge at once should be at least 2
            if (newMaxMergeAtOnce <= 1) {
                newMaxMergeAtOnce = 2;
            }
            logger.debug("changing max_merge_at_once from [{}] to [{}] because segments_per_tier [{}] has to be higher or " +
                    "equal to it",
                maxMergeAtOnce, newMaxMergeAtOnce, segmentsPerTier);
            maxMergeAtOnce = newMaxMergeAtOnce;
        }
        return maxMergeAtOnce;
    }

    MergePolicy getMergePolicy() {
        return mergesEnabled ? mergePolicy : NoMergePolicy.INSTANCE;
    }

    private static double parseNoCFSRatio(String noCFSRatio) {
        noCFSRatio = noCFSRatio.trim();
        if (noCFSRatio.equalsIgnoreCase("true")) {
            return 1.0d;
        } else if (noCFSRatio.equalsIgnoreCase("false")) {
            return 0.0;
        } else {
            try {
                double value = Double.parseDouble(noCFSRatio);
                if (value < 0.0 || value > 1.0) {
                    throw new IllegalArgumentException("NoCFSRatio must be in the interval [0..1] but was: [" + value + "]");
                }
                return value;
            } catch (NumberFormatException ex) {
                throw new IllegalArgumentException("Expected a boolean or a value in the interval [0..1] but was: " +
                    "[" + noCFSRatio + "]", ex);
            }
        }
    }
}
