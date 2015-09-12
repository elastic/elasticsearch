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
package org.elasticsearch.index.shard;

import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.TieredMergePolicy;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import java.io.IOException;

import static org.elasticsearch.common.settings.Settings.Builder.EMPTY_SETTINGS;
import static org.hamcrest.Matchers.equalTo;

public class MergePolicySettingsTests extends ESTestCase {

    protected final ShardId shardId = new ShardId(new Index("index"), 1);

    @Test
    public void testCompoundFileSettings() throws IOException {

        assertThat(new MergePolicyConfig(logger, EMPTY_SETTINGS).getMergePolicy().getNoCFSRatio(), equalTo(0.1));
        assertThat(new MergePolicyConfig(logger, build(true)).getMergePolicy().getNoCFSRatio(), equalTo(1.0));
        assertThat(new MergePolicyConfig(logger, build(0.5)).getMergePolicy().getNoCFSRatio(), equalTo(0.5));
        assertThat(new MergePolicyConfig(logger, build(1.0)).getMergePolicy().getNoCFSRatio(), equalTo(1.0));
        assertThat(new MergePolicyConfig(logger, build("true")).getMergePolicy().getNoCFSRatio(), equalTo(1.0));
        assertThat(new MergePolicyConfig(logger, build("True")).getMergePolicy().getNoCFSRatio(), equalTo(1.0));
        assertThat(new MergePolicyConfig(logger, build("False")).getMergePolicy().getNoCFSRatio(), equalTo(0.0));
        assertThat(new MergePolicyConfig(logger, build("false")).getMergePolicy().getNoCFSRatio(), equalTo(0.0));
        assertThat(new MergePolicyConfig(logger, build(false)).getMergePolicy().getNoCFSRatio(), equalTo(0.0));
        assertThat(new MergePolicyConfig(logger, build(0)).getMergePolicy().getNoCFSRatio(), equalTo(0.0));
        assertThat(new MergePolicyConfig(logger, build(0.0)).getMergePolicy().getNoCFSRatio(), equalTo(0.0));
    }

    public void testNoMerges() {
        MergePolicyConfig mp = new MergePolicyConfig(logger, Settings.builder().put(MergePolicyConfig.INDEX_MERGE_ENABLED, false).build());
        assertTrue(mp.getMergePolicy() instanceof NoMergePolicy);
    }

    @Test
    public void testUpdateSettings() throws IOException {
        {
            IndexSettingsService service = new IndexSettingsService(new Index("test"), EMPTY_SETTINGS);
            MergePolicyConfig mp = new MergePolicyConfig(logger, EMPTY_SETTINGS);
            assertThat(((TieredMergePolicy) mp.getMergePolicy()).getNoCFSRatio(), equalTo(0.1));

            mp.onRefreshSettings(build(1.0));
            assertThat(((TieredMergePolicy) mp.getMergePolicy()).getNoCFSRatio(), equalTo(1.0));

            mp.onRefreshSettings(build(0.1));
            assertThat(((TieredMergePolicy) mp.getMergePolicy()).getNoCFSRatio(), equalTo(0.1));

            mp.onRefreshSettings(build(0.0));
            assertThat(((TieredMergePolicy) mp.getMergePolicy()).getNoCFSRatio(), equalTo(0.0));
        }


    }


    public void testTieredMergePolicySettingsUpdate() throws IOException {
        MergePolicyConfig mp = new MergePolicyConfig(logger, EMPTY_SETTINGS);
        assertThat(mp.getMergePolicy().getNoCFSRatio(), equalTo(0.1));

        assertEquals(((TieredMergePolicy) mp.getMergePolicy()).getForceMergeDeletesPctAllowed(), MergePolicyConfig.DEFAULT_EXPUNGE_DELETES_ALLOWED, 0.0d);
        mp.onRefreshSettings(Settings.builder().put(MergePolicyConfig.INDEX_MERGE_POLICY_EXPUNGE_DELETES_ALLOWED, MergePolicyConfig.DEFAULT_EXPUNGE_DELETES_ALLOWED + 1.0d).build());
        assertEquals(((TieredMergePolicy) mp.getMergePolicy()).getForceMergeDeletesPctAllowed(), MergePolicyConfig.DEFAULT_EXPUNGE_DELETES_ALLOWED + 1.0d, 0.0d);

        assertEquals(((TieredMergePolicy) mp.getMergePolicy()).getFloorSegmentMB(), MergePolicyConfig.DEFAULT_FLOOR_SEGMENT.mbFrac(), 0);
        mp.onRefreshSettings(Settings.builder().put(MergePolicyConfig.INDEX_MERGE_POLICY_FLOOR_SEGMENT, new ByteSizeValue(MergePolicyConfig.DEFAULT_FLOOR_SEGMENT.mb() + 1, ByteSizeUnit.MB)).build());
        assertEquals(((TieredMergePolicy) mp.getMergePolicy()).getFloorSegmentMB(), new ByteSizeValue(MergePolicyConfig.DEFAULT_FLOOR_SEGMENT.mb() + 1, ByteSizeUnit.MB).mbFrac(), 0.001);

        assertEquals(((TieredMergePolicy) mp.getMergePolicy()).getMaxMergeAtOnce(), MergePolicyConfig.DEFAULT_MAX_MERGE_AT_ONCE);
        mp.onRefreshSettings(Settings.builder().put(MergePolicyConfig.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE, MergePolicyConfig.DEFAULT_MAX_MERGE_AT_ONCE - 1).build());
        assertEquals(((TieredMergePolicy) mp.getMergePolicy()).getMaxMergeAtOnce(), MergePolicyConfig.DEFAULT_MAX_MERGE_AT_ONCE-1);

        assertEquals(((TieredMergePolicy) mp.getMergePolicy()).getMaxMergeAtOnceExplicit(), MergePolicyConfig.DEFAULT_MAX_MERGE_AT_ONCE_EXPLICIT);
        mp.onRefreshSettings(Settings.builder().put(MergePolicyConfig.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_EXPLICIT, MergePolicyConfig.DEFAULT_MAX_MERGE_AT_ONCE_EXPLICIT - 1).build());
        assertEquals(((TieredMergePolicy) mp.getMergePolicy()).getMaxMergeAtOnceExplicit(), MergePolicyConfig.DEFAULT_MAX_MERGE_AT_ONCE_EXPLICIT-1);

        assertEquals(((TieredMergePolicy) mp.getMergePolicy()).getMaxMergedSegmentMB(), MergePolicyConfig.DEFAULT_MAX_MERGED_SEGMENT.mbFrac(), 0.0001);
        mp.onRefreshSettings(Settings.builder().put(MergePolicyConfig.INDEX_MERGE_POLICY_MAX_MERGED_SEGMENT, new ByteSizeValue(MergePolicyConfig.DEFAULT_MAX_MERGED_SEGMENT.bytes() + 1)).build());
        assertEquals(((TieredMergePolicy) mp.getMergePolicy()).getMaxMergedSegmentMB(), new ByteSizeValue(MergePolicyConfig.DEFAULT_MAX_MERGED_SEGMENT.bytes() + 1).mbFrac(), 0.0001);

        assertEquals(((TieredMergePolicy) mp.getMergePolicy()).getReclaimDeletesWeight(), MergePolicyConfig.DEFAULT_RECLAIM_DELETES_WEIGHT, 0);
        mp.onRefreshSettings(Settings.builder().put(MergePolicyConfig.INDEX_MERGE_POLICY_RECLAIM_DELETES_WEIGHT, MergePolicyConfig.DEFAULT_RECLAIM_DELETES_WEIGHT + 1).build());
        assertEquals(((TieredMergePolicy) mp.getMergePolicy()).getReclaimDeletesWeight(), MergePolicyConfig.DEFAULT_RECLAIM_DELETES_WEIGHT + 1, 0);

        assertEquals(((TieredMergePolicy) mp.getMergePolicy()).getSegmentsPerTier(), MergePolicyConfig.DEFAULT_SEGMENTS_PER_TIER, 0);
        mp.onRefreshSettings(Settings.builder().put(MergePolicyConfig.INDEX_MERGE_POLICY_SEGMENTS_PER_TIER, MergePolicyConfig.DEFAULT_SEGMENTS_PER_TIER + 1).build());
        assertEquals(((TieredMergePolicy) mp.getMergePolicy()).getSegmentsPerTier(), MergePolicyConfig.DEFAULT_SEGMENTS_PER_TIER + 1, 0);

        mp.onRefreshSettings(EMPTY_SETTINGS); // update without the settings and see if we stick to the values

        assertEquals(((TieredMergePolicy) mp.getMergePolicy()).getForceMergeDeletesPctAllowed(), MergePolicyConfig.DEFAULT_EXPUNGE_DELETES_ALLOWED + 1.0d, 0.0d);
        assertEquals(((TieredMergePolicy) mp.getMergePolicy()).getFloorSegmentMB(), new ByteSizeValue(MergePolicyConfig.DEFAULT_FLOOR_SEGMENT.mb() + 1, ByteSizeUnit.MB).mbFrac(), 0.001);
        assertEquals(((TieredMergePolicy) mp.getMergePolicy()).getMaxMergeAtOnce(), MergePolicyConfig.DEFAULT_MAX_MERGE_AT_ONCE-1);
        assertEquals(((TieredMergePolicy) mp.getMergePolicy()).getMaxMergeAtOnceExplicit(), MergePolicyConfig.DEFAULT_MAX_MERGE_AT_ONCE_EXPLICIT-1);
        assertEquals(((TieredMergePolicy) mp.getMergePolicy()).getMaxMergedSegmentMB(), new ByteSizeValue(MergePolicyConfig.DEFAULT_MAX_MERGED_SEGMENT.bytes() + 1).mbFrac(), 0.0001);
        assertEquals(((TieredMergePolicy) mp.getMergePolicy()).getReclaimDeletesWeight(), MergePolicyConfig.DEFAULT_RECLAIM_DELETES_WEIGHT + 1, 0);
        assertEquals(((TieredMergePolicy) mp.getMergePolicy()).getSegmentsPerTier(), MergePolicyConfig.DEFAULT_SEGMENTS_PER_TIER + 1, 0);
    }

    public Settings build(String value) {
        return Settings.builder().put(MergePolicyConfig.INDEX_COMPOUND_FORMAT, value).build();
    }

    public Settings build(double value) {
        return Settings.builder().put(MergePolicyConfig.INDEX_COMPOUND_FORMAT, value).build();
    }

    public Settings build(int value) {
        return Settings.builder().put(MergePolicyConfig.INDEX_COMPOUND_FORMAT, value).build();
    }

    public Settings build(boolean value) {
        return Settings.builder().put(MergePolicyConfig.INDEX_COMPOUND_FORMAT, value).build();
    }


}
