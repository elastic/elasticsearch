/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;

import static org.elasticsearch.index.IndexSettingsTests.newIndexMeta;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class MergePolicyConfigTests extends ESTestCase {
    protected final ShardId shardId = new ShardId("index", "_na_", 1);

    public void testCompoundFileSettings() throws IOException {
        assertCompoundThreshold(Settings.EMPTY, 1.0, ByteSizeValue.ofGb(1));
        assertCompoundThreshold(build(true), 1.0, ByteSizeValue.ofBytes(Long.MAX_VALUE));
        assertCompoundThreshold(build(0.5), 0.5, ByteSizeValue.ofBytes(Long.MAX_VALUE));
        assertCompoundThreshold(build(1.0), 1.0, ByteSizeValue.ofBytes(Long.MAX_VALUE));
        assertCompoundThreshold(build("true"), 1.0, ByteSizeValue.ofBytes(Long.MAX_VALUE));
        assertCompoundThreshold(build("True"), 1.0, ByteSizeValue.ofBytes(Long.MAX_VALUE));
        assertCompoundThreshold(build("False"), 0.0, ByteSizeValue.ofBytes(Long.MAX_VALUE));
        assertCompoundThreshold(build("false"), 0.0, ByteSizeValue.ofBytes(Long.MAX_VALUE));
        assertCompoundThreshold(build(false), 0.0, ByteSizeValue.ofBytes(Long.MAX_VALUE));
        assertCompoundThreshold(build(0), 0.0, ByteSizeValue.ofBytes(Long.MAX_VALUE));
        assertCompoundThreshold(build(0.0), 0.0, ByteSizeValue.ofBytes(Long.MAX_VALUE));
        assertCompoundThreshold(build(0.0), 0.0, ByteSizeValue.ofBytes(Long.MAX_VALUE));
        assertCompoundThreshold(build("100MB"), 1.0, ByteSizeValue.ofMb(100));
        assertCompoundThreshold(build("0MB"), 1.0, ByteSizeValue.ofBytes(0));
        assertCompoundThreshold(build("0B"), 1.0, ByteSizeValue.ofBytes(0));
    }

    private void assertCompoundThreshold(Settings settings, double noCFSRatio, ByteSizeValue maxCFSSize) {
        MergePolicy mp = new MergePolicyConfig(logger, indexSettings(settings)).getMergePolicy(randomBoolean());
        assertThat(mp.getNoCFSRatio(), equalTo(noCFSRatio));
        assertThat(mp.getMaxCFSSegmentSizeMB(), equalTo(maxCFSSize.getMbFrac()));
    }

    private static IndexSettings indexSettings(Settings settings) {
        return indexSettings(settings, Settings.EMPTY);
    }

    private static IndexSettings indexSettings(Settings indexSettings, Settings nodeSettings) {
        return new IndexSettings(newIndexMeta("test", indexSettings), nodeSettings);
    }

    public void testNoMerges() {
        MergePolicyConfig mp = new MergePolicyConfig(
            logger,
            indexSettings(Settings.builder().put(MergePolicyConfig.INDEX_MERGE_ENABLED, false).build())
        );
        assertTrue(mp.getMergePolicy(randomBoolean()) instanceof NoMergePolicy);
    }

    public void testUpdateSettings() throws IOException {
        IndexSettings indexSettings = indexSettings(Settings.EMPTY);
        assertThat(indexSettings.getMergePolicy(randomBoolean()).getNoCFSRatio(), equalTo(1.0));
        assertThat(indexSettings.getMergePolicy(randomBoolean()).getMaxCFSSegmentSizeMB(), equalTo(1024d));
        indexSettings = indexSettings(build(0.9));
        assertThat((indexSettings.getMergePolicy(randomBoolean())).getNoCFSRatio(), equalTo(0.9));
        assertThat(
            indexSettings.getMergePolicy(randomBoolean()).getMaxCFSSegmentSizeMB(),
            equalTo(ByteSizeValue.ofBytes(Long.MAX_VALUE).getMbFrac())
        );
        indexSettings.updateIndexMetadata(newIndexMeta("index", build(0.1)));
        assertThat((indexSettings.getMergePolicy(randomBoolean())).getNoCFSRatio(), equalTo(0.1));
        assertThat(
            indexSettings.getMergePolicy(randomBoolean()).getMaxCFSSegmentSizeMB(),
            equalTo(ByteSizeValue.ofBytes(Long.MAX_VALUE).getMbFrac())
        );
        indexSettings.updateIndexMetadata(newIndexMeta("index", build(0.0)));
        assertThat((indexSettings.getMergePolicy(randomBoolean())).getNoCFSRatio(), equalTo(0.0));
        assertThat(
            indexSettings.getMergePolicy(randomBoolean()).getMaxCFSSegmentSizeMB(),
            equalTo(ByteSizeValue.ofBytes(Long.MAX_VALUE).getMbFrac())
        );
        indexSettings.updateIndexMetadata(newIndexMeta("index", build("true")));
        assertThat((indexSettings.getMergePolicy(randomBoolean())).getNoCFSRatio(), equalTo(1.0));
        assertThat(
            indexSettings.getMergePolicy(randomBoolean()).getMaxCFSSegmentSizeMB(),
            equalTo(ByteSizeValue.ofBytes(Long.MAX_VALUE).getMbFrac())
        );
        indexSettings.updateIndexMetadata(newIndexMeta("index", build("false")));
        assertThat((indexSettings.getMergePolicy(randomBoolean())).getNoCFSRatio(), equalTo(0.0));
        assertThat(
            indexSettings.getMergePolicy(randomBoolean()).getMaxCFSSegmentSizeMB(),
            equalTo(ByteSizeValue.ofBytes(Long.MAX_VALUE).getMbFrac())
        );
        indexSettings.updateIndexMetadata(newIndexMeta("index", build("100mb")));
        assertThat((indexSettings.getMergePolicy(randomBoolean())).getNoCFSRatio(), equalTo(1.0));
        assertThat(indexSettings.getMergePolicy(randomBoolean()).getMaxCFSSegmentSizeMB(), equalTo(100d));
        indexSettings.updateIndexMetadata(
            newIndexMeta("index", Settings.builder().put(MergePolicyConfig.INDEX_MERGE_POLICY_TYPE_SETTING.getKey(), "tiered").build())
        );
        assertThat(indexSettings.getMergePolicy(randomBoolean()), Matchers.instanceOf(TieredMergePolicy.class));
        indexSettings.updateIndexMetadata(
            newIndexMeta("index", Settings.builder().put(MergePolicyConfig.INDEX_MERGE_POLICY_TYPE_SETTING.getKey(), "time_based").build())
        );
        assertThat(indexSettings.getMergePolicy(randomBoolean()), Matchers.instanceOf(LogByteSizeMergePolicy.class));
    }

    public void testTieredMergePolicySettingsUpdate() {
        IndexSettings indexSettings = indexSettings(Settings.EMPTY);
        assertEquals(
            ((TieredMergePolicy) indexSettings.getMergePolicy(false)).getForceMergeDeletesPctAllowed(),
            MergePolicyConfig.DEFAULT_EXPUNGE_DELETES_ALLOWED,
            0.0d
        );

        indexSettings.updateIndexMetadata(
            newIndexMeta(
                "index",
                Settings.builder()
                    .put(
                        MergePolicyConfig.INDEX_MERGE_POLICY_EXPUNGE_DELETES_ALLOWED_SETTING.getKey(),
                        MergePolicyConfig.DEFAULT_EXPUNGE_DELETES_ALLOWED + 1.0d
                    )
                    .build()
            )
        );
        assertEquals(
            ((TieredMergePolicy) indexSettings.getMergePolicy(false)).getForceMergeDeletesPctAllowed(),
            MergePolicyConfig.DEFAULT_EXPUNGE_DELETES_ALLOWED + 1.0d,
            0.0d
        );

        assertEquals(
            ((TieredMergePolicy) indexSettings.getMergePolicy(false)).getFloorSegmentMB(),
            MergePolicyConfig.DEFAULT_FLOOR_SEGMENT.getMbFrac(),
            0
        );
        assertEquals(
            ((LogByteSizeMergePolicy) indexSettings.getMergePolicy(true)).getMinMergeMB(),
            MergePolicyConfig.DEFAULT_FLOOR_SEGMENT.getMbFrac(),
            0
        );
        indexSettings.updateIndexMetadata(
            newIndexMeta(
                "index",
                Settings.builder()
                    .put(
                        MergePolicyConfig.INDEX_MERGE_POLICY_FLOOR_SEGMENT_SETTING.getKey(),
                        ByteSizeValue.of(MergePolicyConfig.DEFAULT_FLOOR_SEGMENT.getMb() + 1, ByteSizeUnit.MB)
                    )
                    .build()
            )
        );
        assertEquals(
            ((TieredMergePolicy) indexSettings.getMergePolicy(false)).getFloorSegmentMB(),
            ByteSizeValue.of(MergePolicyConfig.DEFAULT_FLOOR_SEGMENT.getMb() + 1, ByteSizeUnit.MB).getMbFrac(),
            0.001
        );
        assertEquals(
            ((LogByteSizeMergePolicy) indexSettings.getMergePolicy(true)).getMinMergeMB(),
            ByteSizeValue.of(MergePolicyConfig.DEFAULT_FLOOR_SEGMENT.getMb() + 1, ByteSizeUnit.MB).getMbFrac(),
            0.001
        );

        assertEquals(
            ((TieredMergePolicy) indexSettings.getMergePolicy(false)).getMaxMergeAtOnce(),
            MergePolicyConfig.DEFAULT_MAX_MERGE_AT_ONCE
        );
        indexSettings.updateIndexMetadata(
            newIndexMeta(
                "index",
                Settings.builder()
                    .put(
                        MergePolicyConfig.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_SETTING.getKey(),
                        MergePolicyConfig.DEFAULT_MAX_MERGE_AT_ONCE - 1
                    )
                    .build()
            )
        );
        assertEquals(
            ((TieredMergePolicy) indexSettings.getMergePolicy(false)).getMaxMergeAtOnce(),
            MergePolicyConfig.DEFAULT_MAX_MERGE_AT_ONCE - 1
        );

        assertEquals(
            ((TieredMergePolicy) indexSettings.getMergePolicy(false)).getMaxMergedSegmentMB(),
            MergePolicyConfig.DEFAULT_MAX_MERGED_SEGMENT.getMbFrac(),
            0.0001
        );
        assertEquals(
            ((LogByteSizeMergePolicy) indexSettings.getMergePolicy(true)).getMaxMergeMB(),
            MergePolicyConfig.DEFAULT_MAX_TIME_BASED_MERGED_SEGMENT.getMbFrac(),
            0.0001
        );
        indexSettings.updateIndexMetadata(
            newIndexMeta(
                "index",
                Settings.builder()
                    .put(MergePolicyConfig.INDEX_MERGE_POLICY_MAX_MERGED_SEGMENT_SETTING.getKey(), ByteSizeValue.ofGb(8))
                    .build()
            )
        );
        assertEquals(
            ((TieredMergePolicy) indexSettings.getMergePolicy(false)).getMaxMergedSegmentMB(),
            ByteSizeValue.ofGb(8).getMbFrac(),
            0.0001
        );
        assertEquals(
            ((LogByteSizeMergePolicy) indexSettings.getMergePolicy(true)).getMaxMergeMB(),
            ByteSizeValue.ofGb(8).getMbFrac(),
            0.0001
        );

        assertEquals(
            ((TieredMergePolicy) indexSettings.getMergePolicy(false)).getSegmentsPerTier(),
            MergePolicyConfig.DEFAULT_SEGMENTS_PER_TIER,
            0
        );
        indexSettings.updateIndexMetadata(
            newIndexMeta(
                "index",
                Settings.builder()
                    .put(
                        MergePolicyConfig.INDEX_MERGE_POLICY_SEGMENTS_PER_TIER_SETTING.getKey(),
                        MergePolicyConfig.DEFAULT_SEGMENTS_PER_TIER + 1
                    )
                    .build()
            )
        );
        assertEquals(
            ((TieredMergePolicy) indexSettings.getMergePolicy(false)).getSegmentsPerTier(),
            MergePolicyConfig.DEFAULT_SEGMENTS_PER_TIER + 1,
            0
        );

        assertEquals(
            ((LogByteSizeMergePolicy) indexSettings.getMergePolicy(true)).getMergeFactor(),
            MergePolicyConfig.DEFAULT_MERGE_FACTOR,
            0
        );
        indexSettings.updateIndexMetadata(
            newIndexMeta(
                "index",
                Settings.builder()
                    .put(MergePolicyConfig.INDEX_MERGE_POLICY_MERGE_FACTOR_SETTING.getKey(), MergePolicyConfig.DEFAULT_MERGE_FACTOR + 1)
                    .build()
            )
        );

        assertEquals(
            ((LogByteSizeMergePolicy) indexSettings.getMergePolicy(true)).getMergeFactor(),
            MergePolicyConfig.DEFAULT_MERGE_FACTOR + 1,
            0
        );

        assertEquals(
            ((TieredMergePolicy) indexSettings.getMergePolicy(false)).getDeletesPctAllowed(),
            MergePolicyConfig.DEFAULT_DELETES_PCT_ALLOWED,
            0
        );
        indexSettings.updateIndexMetadata(
            newIndexMeta(
                "index",
                Settings.builder().put(MergePolicyConfig.INDEX_MERGE_POLICY_DELETES_PCT_ALLOWED_SETTING.getKey(), 22).build()
            )
        );
        assertEquals(((TieredMergePolicy) indexSettings.getMergePolicy(false)).getDeletesPctAllowed(), 22, 0);

        IllegalArgumentException exc = expectThrows(
            IllegalArgumentException.class,
            () -> indexSettings.updateIndexMetadata(
                newIndexMeta(
                    "index",
                    Settings.builder().put(MergePolicyConfig.INDEX_MERGE_POLICY_DELETES_PCT_ALLOWED_SETTING.getKey(), 53).build()
                )
            )
        );
        final Throwable cause = exc.getCause();
        assertThat(cause.getMessage(), containsString("must be <= 50.0"));
        indexSettings.updateIndexMetadata(newIndexMeta("index", Settings.EMPTY)); // see if defaults are restored
        assertEquals(
            ((TieredMergePolicy) indexSettings.getMergePolicy(false)).getForceMergeDeletesPctAllowed(),
            MergePolicyConfig.DEFAULT_EXPUNGE_DELETES_ALLOWED,
            0.0d
        );
        assertEquals(
            ((TieredMergePolicy) indexSettings.getMergePolicy(false)).getFloorSegmentMB(),
            ByteSizeValue.of(MergePolicyConfig.DEFAULT_FLOOR_SEGMENT.getMb(), ByteSizeUnit.MB).getMbFrac(),
            0.00
        );
        assertEquals(
            ((LogByteSizeMergePolicy) indexSettings.getMergePolicy(true)).getMinMergeMB(),
            ByteSizeValue.of(MergePolicyConfig.DEFAULT_FLOOR_SEGMENT.getMb(), ByteSizeUnit.MB).getMbFrac(),
            0.00
        );
        assertEquals(
            ((TieredMergePolicy) indexSettings.getMergePolicy(false)).getMaxMergeAtOnce(),
            MergePolicyConfig.DEFAULT_MAX_MERGE_AT_ONCE
        );
        assertEquals(
            ((TieredMergePolicy) indexSettings.getMergePolicy(false)).getMaxMergedSegmentMB(),
            MergePolicyConfig.DEFAULT_MAX_MERGED_SEGMENT.getMbFrac(),
            0.0001
        );
        assertEquals(
            ((LogByteSizeMergePolicy) indexSettings.getMergePolicy(true)).getMaxMergeMB(),
            MergePolicyConfig.DEFAULT_MAX_TIME_BASED_MERGED_SEGMENT.getMbFrac(),
            0.0001
        );
        assertEquals(
            ((TieredMergePolicy) indexSettings.getMergePolicy(false)).getSegmentsPerTier(),
            MergePolicyConfig.DEFAULT_SEGMENTS_PER_TIER,
            0
        );
        assertEquals(
            ((LogByteSizeMergePolicy) indexSettings.getMergePolicy(true)).getMergeFactor(),
            MergePolicyConfig.DEFAULT_MERGE_FACTOR,
            0
        );
        assertEquals(
            ((TieredMergePolicy) indexSettings.getMergePolicy(false)).getDeletesPctAllowed(),
            MergePolicyConfig.DEFAULT_DELETES_PCT_ALLOWED,
            0
        );
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

    public void testCompoundFileConfiguredByByteSize() throws IOException {
        for (boolean isTimeSeriesIndex : new boolean[] { false, true }) {
            try (Directory dir = newDirectory()) {
                // index.compound_format: 1gb, the merge will use a compound file
                MergePolicy mp = new MergePolicyConfig(logger, indexSettings(Settings.EMPTY)).getMergePolicy(isTimeSeriesIndex);
                try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(null).setMergePolicy(mp))) {
                    w.addDocument(new Document());
                    w.flush();
                    w.addDocument(new Document());
                    w.forceMerge(1);
                }
                try (DirectoryReader reader = DirectoryReader.open(dir)) {
                    LeafReader leaf = getOnlyLeafReader(reader);
                    SegmentCommitInfo sci = ((SegmentReader) leaf).getSegmentInfo();
                    assertEquals(IndexWriter.SOURCE_MERGE, sci.info.getDiagnostics().get(IndexWriter.SOURCE));
                    assertTrue(sci.info.getUseCompoundFile());
                }
            }

            // index.compound_format: 1b, the merge will not use a compound file
            try (Directory dir = newDirectory()) {
                MergePolicy mp = new MergePolicyConfig(logger, indexSettings(build("1b"))).getMergePolicy(isTimeSeriesIndex);
                try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(null).setMergePolicy(mp))) {
                    w.addDocument(new Document());
                    w.flush();
                    w.addDocument(new Document());
                    w.forceMerge(1);
                }
                try (DirectoryReader reader = DirectoryReader.open(dir)) {
                    LeafReader leaf = getOnlyLeafReader(reader);
                    SegmentCommitInfo sci = ((SegmentReader) leaf).getSegmentInfo();
                    assertEquals(IndexWriter.SOURCE_MERGE, sci.info.getDiagnostics().get(IndexWriter.SOURCE));
                    assertFalse(sci.info.getUseCompoundFile());
                }
            }
        }
    }

    public void testDefaultMaxMergedSegment() {
        var indexSettings = indexSettings(Settings.EMPTY);
        {
            TieredMergePolicy tieredPolicy = (TieredMergePolicy) new MergePolicyConfig(logger, indexSettings).getMergePolicy(false);
            assertEquals(MergePolicyConfig.DEFAULT_MAX_MERGED_SEGMENT.getMbFrac(), tieredPolicy.getMaxMergedSegmentMB(), 0.0d);
        }
        {
            LogByteSizeMergePolicy timePolicy = (LogByteSizeMergePolicy) new MergePolicyConfig(logger, indexSettings).getMergePolicy(true);
            assertEquals(MergePolicyConfig.DEFAULT_MAX_TIME_BASED_MERGED_SEGMENT.getMbFrac(), timePolicy.getMaxMergeMB(), 0.0d);
        }
    }

    public void testDefaultMaxMergedSegmentWithNodeOverrides() {
        var maxMergedSegmentSize = ByteSizeValue.ofBytes(randomLongBetween(1L, Long.MAX_VALUE));
        {
            var indexSettings = indexSettings(
                Settings.EMPTY,
                Settings.builder().put(MergePolicyConfig.DEFAULT_MAX_MERGED_SEGMENT_SETTING.getKey(), maxMergedSegmentSize).build()
            );
            TieredMergePolicy tieredPolicy = (TieredMergePolicy) new MergePolicyConfig(logger, indexSettings).getMergePolicy(false);
            assertEquals(maxMergedSegmentSize.getMbFrac(), tieredPolicy.getMaxMergedSegmentMB(), 0.0d);
        }
        {
            var indexSettings = indexSettings(
                Settings.EMPTY,
                Settings.builder()
                    .put(MergePolicyConfig.DEFAULT_MAX_TIME_BASED_MERGED_SEGMENT_SETTING.getKey(), maxMergedSegmentSize)
                    .build()
            );
            LogByteSizeMergePolicy timePolicy = (LogByteSizeMergePolicy) new MergePolicyConfig(logger, indexSettings).getMergePolicy(true);
            assertEquals(maxMergedSegmentSize.getMbFrac(), timePolicy.getMaxMergeMB(), 0.0d);
        }
    }
}
