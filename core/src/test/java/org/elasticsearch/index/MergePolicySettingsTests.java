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

import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.TieredMergePolicy;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.common.settings.Settings.Builder.EMPTY_SETTINGS;
import static org.elasticsearch.index.IndexSettingsTests.newIndexMeta;
import static org.hamcrest.Matchers.equalTo;

public class MergePolicySettingsTests extends ESTestCase {
    protected final ShardId shardId = new ShardId("index", "_na_", 1);

    public void testCompoundFileSettings() throws IOException {
        assertThat(new MergePolicyConfig(logger, indexSettings(Settings.EMPTY)).getMergePolicy().getNoCFSRatio(), equalTo(0.1));
        assertThat(new MergePolicyConfig(logger, indexSettings(build(true))).getMergePolicy().getNoCFSRatio(), equalTo(1.0));
        assertThat(new MergePolicyConfig(logger, indexSettings(build(0.5))).getMergePolicy().getNoCFSRatio(), equalTo(0.5));
        assertThat(new MergePolicyConfig(logger, indexSettings(build(1.0))).getMergePolicy().getNoCFSRatio(), equalTo(1.0));
        assertThat(new MergePolicyConfig(logger, indexSettings(build("true"))).getMergePolicy().getNoCFSRatio(), equalTo(1.0));
        assertThat(new MergePolicyConfig(logger, indexSettings(build("True"))).getMergePolicy().getNoCFSRatio(), equalTo(1.0));
        assertThat(new MergePolicyConfig(logger, indexSettings(build("False"))).getMergePolicy().getNoCFSRatio(), equalTo(0.0));
        assertThat(new MergePolicyConfig(logger, indexSettings(build("false"))).getMergePolicy().getNoCFSRatio(), equalTo(0.0));
        assertThat(new MergePolicyConfig(logger, indexSettings(build(false))).getMergePolicy().getNoCFSRatio(), equalTo(0.0));
        assertThat(new MergePolicyConfig(logger, indexSettings(build(0))).getMergePolicy().getNoCFSRatio(), equalTo(0.0));
        assertThat(new MergePolicyConfig(logger, indexSettings(build(0.0))).getMergePolicy().getNoCFSRatio(), equalTo(0.0));
    }

    private static IndexSettings indexSettings(Settings settings) {
        return new IndexSettings(newIndexMeta("test", settings), Settings.EMPTY);
    }

    public void testNoMerges() {
        MergePolicyConfig mp = new MergePolicyConfig(logger, indexSettings(Settings.builder().put(MergePolicyConfig.INDEX_MERGE_ENABLED, false).build()));
        assertTrue(mp.getMergePolicy() instanceof NoMergePolicy);
    }

    public void testUpdateSettings() throws IOException {
        IndexSettings indexSettings = indexSettings(EMPTY_SETTINGS);
        assertThat(indexSettings.getMergePolicy().getNoCFSRatio(), equalTo(0.1));
        indexSettings = indexSettings(build(0.9));
        assertThat((indexSettings.getMergePolicy()).getNoCFSRatio(), equalTo(0.9));
        indexSettings.updateIndexMetaData(newIndexMeta("index", build(0.1)));
        assertThat((indexSettings.getMergePolicy()).getNoCFSRatio(), equalTo(0.1));
        indexSettings.updateIndexMetaData(newIndexMeta("index", build(0.0)));
        assertThat((indexSettings.getMergePolicy()).getNoCFSRatio(), equalTo(0.0));
        indexSettings.updateIndexMetaData(newIndexMeta("index", build("true")));
        assertThat((indexSettings.getMergePolicy()).getNoCFSRatio(), equalTo(1.0));
        indexSettings.updateIndexMetaData(newIndexMeta("index", build("false")));
        assertThat((indexSettings.getMergePolicy()).getNoCFSRatio(), equalTo(0.0));
    }


    public void testTieredMergePolicySettingsUpdate() throws IOException {
        IndexSettings indexSettings = indexSettings(Settings.EMPTY);
        assertEquals(((TieredMergePolicy) indexSettings.getMergePolicy()).getForceMergeDeletesPctAllowed(), MergePolicyConfig.DEFAULT_EXPUNGE_DELETES_ALLOWED, 0.0d);

        indexSettings.updateIndexMetaData(newIndexMeta("index", Settings.builder().put(MergePolicyConfig.INDEX_MERGE_POLICY_EXPUNGE_DELETES_ALLOWED_SETTING.getKey(), MergePolicyConfig.DEFAULT_EXPUNGE_DELETES_ALLOWED + 1.0d).build()));
        assertEquals(((TieredMergePolicy) indexSettings.getMergePolicy()).getForceMergeDeletesPctAllowed(), MergePolicyConfig.DEFAULT_EXPUNGE_DELETES_ALLOWED + 1.0d, 0.0d);

        assertEquals(((TieredMergePolicy) indexSettings.getMergePolicy()).getFloorSegmentMB(), MergePolicyConfig.DEFAULT_FLOOR_SEGMENT.mbFrac(), 0);
        indexSettings.updateIndexMetaData(newIndexMeta("index", Settings.builder().put(MergePolicyConfig.INDEX_MERGE_POLICY_FLOOR_SEGMENT_SETTING.getKey(), new ByteSizeValue(MergePolicyConfig.DEFAULT_FLOOR_SEGMENT.mb() + 1, ByteSizeUnit.MB)).build()));
        assertEquals(((TieredMergePolicy) indexSettings.getMergePolicy()).getFloorSegmentMB(), new ByteSizeValue(MergePolicyConfig.DEFAULT_FLOOR_SEGMENT.mb() + 1, ByteSizeUnit.MB).mbFrac(), 0.001);

        assertEquals(((TieredMergePolicy) indexSettings.getMergePolicy()).getMaxMergeAtOnce(), MergePolicyConfig.DEFAULT_MAX_MERGE_AT_ONCE);
        indexSettings.updateIndexMetaData(newIndexMeta("index", Settings.builder().put(MergePolicyConfig.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_SETTING.getKey(), MergePolicyConfig.DEFAULT_MAX_MERGE_AT_ONCE - 1).build()));
        assertEquals(((TieredMergePolicy) indexSettings.getMergePolicy()).getMaxMergeAtOnce(), MergePolicyConfig.DEFAULT_MAX_MERGE_AT_ONCE - 1);

        assertEquals(((TieredMergePolicy) indexSettings.getMergePolicy()).getMaxMergeAtOnceExplicit(), MergePolicyConfig.DEFAULT_MAX_MERGE_AT_ONCE_EXPLICIT);
        indexSettings.updateIndexMetaData(newIndexMeta("index", Settings.builder().put(MergePolicyConfig.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_EXPLICIT_SETTING.getKey(), MergePolicyConfig.DEFAULT_MAX_MERGE_AT_ONCE_EXPLICIT - 1).build()));
        assertEquals(((TieredMergePolicy) indexSettings.getMergePolicy()).getMaxMergeAtOnceExplicit(), MergePolicyConfig.DEFAULT_MAX_MERGE_AT_ONCE_EXPLICIT-1);

        assertEquals(((TieredMergePolicy) indexSettings.getMergePolicy()).getMaxMergedSegmentMB(), MergePolicyConfig.DEFAULT_MAX_MERGED_SEGMENT.mbFrac(), 0.0001);
        indexSettings.updateIndexMetaData(newIndexMeta("index", Settings.builder().put(MergePolicyConfig.INDEX_MERGE_POLICY_MAX_MERGED_SEGMENT_SETTING.getKey(), new ByteSizeValue(MergePolicyConfig.DEFAULT_MAX_MERGED_SEGMENT.bytes() + 1)).build()));
        assertEquals(((TieredMergePolicy) indexSettings.getMergePolicy()).getMaxMergedSegmentMB(), new ByteSizeValue(MergePolicyConfig.DEFAULT_MAX_MERGED_SEGMENT.bytes() + 1).mbFrac(), 0.0001);

        assertEquals(((TieredMergePolicy) indexSettings.getMergePolicy()).getReclaimDeletesWeight(), MergePolicyConfig.DEFAULT_RECLAIM_DELETES_WEIGHT, 0);
        indexSettings.updateIndexMetaData(newIndexMeta("index", Settings.builder().put(MergePolicyConfig.INDEX_MERGE_POLICY_RECLAIM_DELETES_WEIGHT_SETTING.getKey(), MergePolicyConfig.DEFAULT_RECLAIM_DELETES_WEIGHT + 1).build()));
        assertEquals(((TieredMergePolicy) indexSettings.getMergePolicy()).getReclaimDeletesWeight(), MergePolicyConfig.DEFAULT_RECLAIM_DELETES_WEIGHT + 1, 0);

        assertEquals(((TieredMergePolicy) indexSettings.getMergePolicy()).getSegmentsPerTier(), MergePolicyConfig.DEFAULT_SEGMENTS_PER_TIER, 0);
        indexSettings.updateIndexMetaData(newIndexMeta("index", Settings.builder().put(MergePolicyConfig.INDEX_MERGE_POLICY_SEGMENTS_PER_TIER_SETTING.getKey(), MergePolicyConfig.DEFAULT_SEGMENTS_PER_TIER + 1).build()));
        assertEquals(((TieredMergePolicy) indexSettings.getMergePolicy()).getSegmentsPerTier(), MergePolicyConfig.DEFAULT_SEGMENTS_PER_TIER + 1, 0);

        indexSettings.updateIndexMetaData(newIndexMeta("index", EMPTY_SETTINGS)); // see if defaults are restored
        assertEquals(((TieredMergePolicy) indexSettings.getMergePolicy()).getForceMergeDeletesPctAllowed(), MergePolicyConfig.DEFAULT_EXPUNGE_DELETES_ALLOWED, 0.0d);
        assertEquals(((TieredMergePolicy) indexSettings.getMergePolicy()).getFloorSegmentMB(), new ByteSizeValue(MergePolicyConfig.DEFAULT_FLOOR_SEGMENT.mb(), ByteSizeUnit.MB).mbFrac(), 0.00);
        assertEquals(((TieredMergePolicy) indexSettings.getMergePolicy()).getMaxMergeAtOnce(), MergePolicyConfig.DEFAULT_MAX_MERGE_AT_ONCE);
        assertEquals(((TieredMergePolicy) indexSettings.getMergePolicy()).getMaxMergeAtOnceExplicit(), MergePolicyConfig.DEFAULT_MAX_MERGE_AT_ONCE_EXPLICIT);
        assertEquals(((TieredMergePolicy) indexSettings.getMergePolicy()).getMaxMergedSegmentMB(), new ByteSizeValue(MergePolicyConfig.DEFAULT_MAX_MERGED_SEGMENT.bytes() + 1).mbFrac(), 0.0001);
        assertEquals(((TieredMergePolicy) indexSettings.getMergePolicy()).getReclaimDeletesWeight(), MergePolicyConfig.DEFAULT_RECLAIM_DELETES_WEIGHT, 0);
        assertEquals(((TieredMergePolicy) indexSettings.getMergePolicy()).getSegmentsPerTier(), MergePolicyConfig.DEFAULT_SEGMENTS_PER_TIER, 0);
    }

    public Settings build(String value) {
        return Settings.builder().put(MergePolicyConfig.INDEX_COMPOUND_FORMAT_SETTING.getKey(), value).build();
    }

    public Settings build(double value) {
        return Settings.builder().put(MergePolicyConfig.INDEX_COMPOUND_FORMAT_SETTING.getKey(), value).build();
    }

    public Settings build(int value) {
        return Settings.builder().put(MergePolicyConfig.INDEX_COMPOUND_FORMAT_SETTING.getKey(), value).build();
    }

    public Settings build(boolean value) {
        return Settings.builder().put(MergePolicyConfig.INDEX_COMPOUND_FORMAT_SETTING.getKey(), value).build();
    }


}
