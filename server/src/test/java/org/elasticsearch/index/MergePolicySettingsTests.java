/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index;

import org.apache.lucene.index.NoMergePolicy;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.common.settings.Settings.Builder.EMPTY_SETTINGS;
import static org.elasticsearch.index.IndexSettingsTests.newIndexMeta;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class MergePolicySettingsTests extends ESTestCase {
    protected final ShardId shardId = new ShardId("index", "_na_", 1);

    public void testCompoundFileSettings() throws IOException {
        assertThat(new MergePolicyConfig(logger, indexSettings(Settings.EMPTY)).getMergePolicy().getNoCFSRatio(), equalTo(0.1));
        assertThat(new MergePolicyConfig(logger, indexSettings(build(true))).getMergePolicy().getNoCFSRatio(), equalTo(1.0));
        assertThat(new MergePolicyConfig(logger, indexSettings(build(0.5))).getMergePolicy().getNoCFSRatio(), equalTo(0.5));
        assertThat(new MergePolicyConfig(logger, indexSettings(build(1.0))).getMergePolicy().getNoCFSRatio(), equalTo(1.0));
        assertThat(new MergePolicyConfig(logger,
            indexSettings(build("true"))).getMergePolicy().getNoCFSRatio(), equalTo(1.0));
        assertThat(new MergePolicyConfig(logger,
            indexSettings(build("True"))).getMergePolicy().getNoCFSRatio(), equalTo(1.0));
        assertThat(new MergePolicyConfig(logger,
            indexSettings(build("False"))).getMergePolicy().getNoCFSRatio(), equalTo(0.0));
        assertThat(new MergePolicyConfig(logger,
            indexSettings(build("false"))).getMergePolicy().getNoCFSRatio(), equalTo(0.0));
        assertThat(new MergePolicyConfig(logger,
            indexSettings(build(false))).getMergePolicy().getNoCFSRatio(), equalTo(0.0));
        assertThat(new MergePolicyConfig(logger, indexSettings(build(0))).getMergePolicy().getNoCFSRatio(), equalTo(0.0));
        assertThat(new MergePolicyConfig(logger, indexSettings(build(0.0))).getMergePolicy().getNoCFSRatio(), equalTo(0.0));
    }

    private static IndexSettings indexSettings(Settings settings) {
        return new IndexSettings(newIndexMeta("test", settings), Settings.EMPTY);
    }

    public void testNoMerges() {
        MergePolicyConfig mp = new MergePolicyConfig(logger,
            indexSettings(Settings.builder().put(MergePolicyConfig.INDEX_MERGE_ENABLED, false).build()));
        assertTrue(mp.getMergePolicy() instanceof NoMergePolicy);
    }

    public void testUpdateSettings() throws IOException {
        IndexSettings indexSettings = indexSettings(EMPTY_SETTINGS);
        assertThat(indexSettings.getMergePolicy().getNoCFSRatio(), equalTo(0.1));
        indexSettings = indexSettings(build(0.9));
        assertThat((indexSettings.getMergePolicy()).getNoCFSRatio(), equalTo(0.9));
        indexSettings.updateIndexMetadata(newIndexMeta("index", build(0.1)));
        assertThat((indexSettings.getMergePolicy()).getNoCFSRatio(), equalTo(0.1));
        indexSettings.updateIndexMetadata(newIndexMeta("index", build(0.0)));
        assertThat((indexSettings.getMergePolicy()).getNoCFSRatio(), equalTo(0.0));
        indexSettings.updateIndexMetadata(newIndexMeta("index", build("true")));
        assertThat((indexSettings.getMergePolicy()).getNoCFSRatio(), equalTo(1.0));
        indexSettings.updateIndexMetadata(newIndexMeta("index", build("false")));
        assertThat((indexSettings.getMergePolicy()).getNoCFSRatio(), equalTo(0.0));
    }


    public void testTieredMergePolicySettingsUpdate() throws IOException {
        IndexSettings indexSettings = indexSettings(Settings.EMPTY);
        assertEquals(((EsTieredMergePolicy) indexSettings.getMergePolicy()).getForceMergeDeletesPctAllowed(),
            MergePolicyConfig.DEFAULT_EXPUNGE_DELETES_ALLOWED, 0.0d);

        indexSettings.updateIndexMetadata(newIndexMeta("index",
            Settings.builder().put(MergePolicyConfig.INDEX_MERGE_POLICY_EXPUNGE_DELETES_ALLOWED_SETTING.getKey(),
                MergePolicyConfig.DEFAULT_EXPUNGE_DELETES_ALLOWED + 1.0d).build()));
        assertEquals(((EsTieredMergePolicy) indexSettings.getMergePolicy()).getForceMergeDeletesPctAllowed(),
            MergePolicyConfig.DEFAULT_EXPUNGE_DELETES_ALLOWED + 1.0d, 0.0d);

        assertEquals(((EsTieredMergePolicy) indexSettings.getMergePolicy()).getFloorSegmentMB(),
            MergePolicyConfig.DEFAULT_FLOOR_SEGMENT.getMbFrac(), 0);
        indexSettings.updateIndexMetadata(newIndexMeta("index",
            Settings.builder().put(MergePolicyConfig.INDEX_MERGE_POLICY_FLOOR_SEGMENT_SETTING.getKey(),
                new ByteSizeValue(MergePolicyConfig.DEFAULT_FLOOR_SEGMENT.getMb() + 1, ByteSizeUnit.MB)).build()));
        assertEquals(((EsTieredMergePolicy) indexSettings.getMergePolicy()).getFloorSegmentMB(),
            new ByteSizeValue(MergePolicyConfig.DEFAULT_FLOOR_SEGMENT.getMb() + 1, ByteSizeUnit.MB).getMbFrac(), 0.001);

        assertEquals(((EsTieredMergePolicy) indexSettings.getMergePolicy()).getMaxMergeAtOnce(),
            MergePolicyConfig.DEFAULT_MAX_MERGE_AT_ONCE);
        indexSettings.updateIndexMetadata(newIndexMeta("index",
            Settings.builder().put(MergePolicyConfig.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_SETTING.getKey(),
                MergePolicyConfig.DEFAULT_MAX_MERGE_AT_ONCE - 1).build()));
        assertEquals(((EsTieredMergePolicy) indexSettings.getMergePolicy()).getMaxMergeAtOnce(),
            MergePolicyConfig.DEFAULT_MAX_MERGE_AT_ONCE - 1);

        assertEquals(((EsTieredMergePolicy) indexSettings.getMergePolicy()).getMaxMergeAtOnceExplicit(),
            MergePolicyConfig.DEFAULT_MAX_MERGE_AT_ONCE_EXPLICIT);
        indexSettings.updateIndexMetadata(newIndexMeta("index",
            Settings.builder().put(MergePolicyConfig.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_EXPLICIT_SETTING.getKey(),
                MergePolicyConfig.DEFAULT_MAX_MERGE_AT_ONCE_EXPLICIT - 1).build()));
        assertEquals(((EsTieredMergePolicy) indexSettings.getMergePolicy()).getMaxMergeAtOnceExplicit(),
            MergePolicyConfig.DEFAULT_MAX_MERGE_AT_ONCE_EXPLICIT-1);

        assertEquals(((EsTieredMergePolicy) indexSettings.getMergePolicy()).getMaxMergedSegmentMB(),
            MergePolicyConfig.DEFAULT_MAX_MERGED_SEGMENT.getMbFrac(), 0.0001);
        indexSettings.updateIndexMetadata(newIndexMeta("index",
            Settings.builder().put(MergePolicyConfig.INDEX_MERGE_POLICY_MAX_MERGED_SEGMENT_SETTING.getKey(),
                new ByteSizeValue(MergePolicyConfig.DEFAULT_MAX_MERGED_SEGMENT.getBytes() + 1)).build()));
        assertEquals(((EsTieredMergePolicy) indexSettings.getMergePolicy()).getMaxMergedSegmentMB(),
            new ByteSizeValue(MergePolicyConfig.DEFAULT_MAX_MERGED_SEGMENT.getBytes() + 1).getMbFrac(), 0.0001);

        assertEquals(((EsTieredMergePolicy) indexSettings.getMergePolicy()).getSegmentsPerTier(),
            MergePolicyConfig.DEFAULT_SEGMENTS_PER_TIER, 0);
        indexSettings.updateIndexMetadata(newIndexMeta("index",
            Settings.builder().put(MergePolicyConfig.INDEX_MERGE_POLICY_SEGMENTS_PER_TIER_SETTING.getKey(),
                MergePolicyConfig.DEFAULT_SEGMENTS_PER_TIER + 1).build()));
        assertEquals(((EsTieredMergePolicy) indexSettings.getMergePolicy()).getSegmentsPerTier(),
            MergePolicyConfig.DEFAULT_SEGMENTS_PER_TIER + 1, 0);

        assertEquals(((EsTieredMergePolicy) indexSettings.getMergePolicy()).getDeletesPctAllowed(),
            MergePolicyConfig.DEFAULT_DELETES_PCT_ALLOWED, 0);
        indexSettings.updateIndexMetadata(newIndexMeta("index",
            Settings.builder().put(MergePolicyConfig.INDEX_MERGE_POLICY_DELETES_PCT_ALLOWED_SETTING.getKey(), 22).build()));
        assertEquals(((EsTieredMergePolicy) indexSettings.getMergePolicy()).getDeletesPctAllowed(), 22, 0);

        IllegalArgumentException exc = expectThrows(IllegalArgumentException.class, () ->
            indexSettings.updateIndexMetadata(newIndexMeta("index",
                Settings.builder().put(MergePolicyConfig.INDEX_MERGE_POLICY_DELETES_PCT_ALLOWED_SETTING.getKey(), 53).build())));
        final Throwable cause = exc.getCause();
        assertThat(cause.getMessage(), containsString("must be <= 50.0"));
        indexSettings.updateIndexMetadata(newIndexMeta("index", EMPTY_SETTINGS)); // see if defaults are restored
        assertEquals(((EsTieredMergePolicy) indexSettings.getMergePolicy()).getForceMergeDeletesPctAllowed(),
            MergePolicyConfig.DEFAULT_EXPUNGE_DELETES_ALLOWED, 0.0d);
        assertEquals(((EsTieredMergePolicy) indexSettings.getMergePolicy()).getFloorSegmentMB(),
            new ByteSizeValue(MergePolicyConfig.DEFAULT_FLOOR_SEGMENT.getMb(), ByteSizeUnit.MB).getMbFrac(), 0.00);
        assertEquals(((EsTieredMergePolicy) indexSettings.getMergePolicy()).getMaxMergeAtOnce(),
            MergePolicyConfig.DEFAULT_MAX_MERGE_AT_ONCE);
        assertEquals(((EsTieredMergePolicy) indexSettings.getMergePolicy()).getMaxMergeAtOnceExplicit(),
            MergePolicyConfig.DEFAULT_MAX_MERGE_AT_ONCE_EXPLICIT);
        assertEquals(((EsTieredMergePolicy) indexSettings.getMergePolicy()).getMaxMergedSegmentMB(),
            new ByteSizeValue(MergePolicyConfig.DEFAULT_MAX_MERGED_SEGMENT.getBytes() + 1).getMbFrac(), 0.0001);
        assertEquals(((EsTieredMergePolicy) indexSettings.getMergePolicy()).getSegmentsPerTier(),
            MergePolicyConfig.DEFAULT_SEGMENTS_PER_TIER, 0);
        assertEquals(((EsTieredMergePolicy) indexSettings.getMergePolicy()).getDeletesPctAllowed(),
            MergePolicyConfig.DEFAULT_DELETES_PCT_ALLOWED, 0);
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
