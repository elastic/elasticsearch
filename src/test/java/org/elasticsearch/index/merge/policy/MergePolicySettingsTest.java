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
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.DirectoryService;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.test.DummyShardLock;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.IOException;

import static org.elasticsearch.common.settings.ImmutableSettings.Builder.EMPTY_SETTINGS;
import static org.hamcrest.Matchers.equalTo;

public class MergePolicySettingsTest extends ElasticsearchTestCase {

    protected final ShardId shardId = new ShardId(new Index("index"), 1);

    @Test
    public void testCompoundFileSettings() throws IOException {
        IndexSettingsService service = new IndexSettingsService(new Index("test"), EMPTY_SETTINGS);

        assertThat(new TieredMergePolicyProvider(createStore(EMPTY_SETTINGS), service).getMergePolicy().getNoCFSRatio(), equalTo(0.1));
        assertThat(new TieredMergePolicyProvider(createStore(build(true)), service).getMergePolicy().getNoCFSRatio(), equalTo(1.0));
        assertThat(new TieredMergePolicyProvider(createStore(build(0.5)), service).getMergePolicy().getNoCFSRatio(), equalTo(0.5));
        assertThat(new TieredMergePolicyProvider(createStore(build(1.0)), service).getMergePolicy().getNoCFSRatio(), equalTo(1.0));
        assertThat(new TieredMergePolicyProvider(createStore(build("true")), service).getMergePolicy().getNoCFSRatio(), equalTo(1.0));
        assertThat(new TieredMergePolicyProvider(createStore(build("True")), service).getMergePolicy().getNoCFSRatio(), equalTo(1.0));
        assertThat(new TieredMergePolicyProvider(createStore(build("False")), service).getMergePolicy().getNoCFSRatio(), equalTo(0.0));
        assertThat(new TieredMergePolicyProvider(createStore(build("false")), service).getMergePolicy().getNoCFSRatio(), equalTo(0.0));
        assertThat(new TieredMergePolicyProvider(createStore(build(false)), service).getMergePolicy().getNoCFSRatio(), equalTo(0.0));
        assertThat(new TieredMergePolicyProvider(createStore(build(0)), service).getMergePolicy().getNoCFSRatio(), equalTo(0.0));
        assertThat(new TieredMergePolicyProvider(createStore(build(0.0)), service).getMergePolicy().getNoCFSRatio(), equalTo(0.0));

        assertThat(new LogByteSizeMergePolicyProvider(createStore(EMPTY_SETTINGS), service).getMergePolicy().getNoCFSRatio(), equalTo(0.1));
        assertThat(new LogByteSizeMergePolicyProvider(createStore(build(true)), service).getMergePolicy().getNoCFSRatio(), equalTo(1.0));
        assertThat(new LogByteSizeMergePolicyProvider(createStore(build(0.5)), service).getMergePolicy().getNoCFSRatio(), equalTo(0.5));
        assertThat(new LogByteSizeMergePolicyProvider(createStore(build(1.0)), service).getMergePolicy().getNoCFSRatio(), equalTo(1.0));
        assertThat(new LogByteSizeMergePolicyProvider(createStore(build("true")), service).getMergePolicy().getNoCFSRatio(), equalTo(1.0));
        assertThat(new LogByteSizeMergePolicyProvider(createStore(build("True")), service).getMergePolicy().getNoCFSRatio(), equalTo(1.0));
        assertThat(new LogByteSizeMergePolicyProvider(createStore(build("False")), service).getMergePolicy().getNoCFSRatio(), equalTo(0.0));
        assertThat(new LogByteSizeMergePolicyProvider(createStore(build("false")), service).getMergePolicy().getNoCFSRatio(), equalTo(0.0));
        assertThat(new LogByteSizeMergePolicyProvider(createStore(build(false)), service).getMergePolicy().getNoCFSRatio(), equalTo(0.0));
        assertThat(new LogByteSizeMergePolicyProvider(createStore(build(0)), service).getMergePolicy().getNoCFSRatio(), equalTo(0.0));
        assertThat(new LogByteSizeMergePolicyProvider(createStore(build(0.0)), service).getMergePolicy().getNoCFSRatio(), equalTo(0.0));

        assertThat(new LogDocMergePolicyProvider(createStore(EMPTY_SETTINGS), service).getMergePolicy().getNoCFSRatio(), equalTo(0.1));
        assertThat(new LogDocMergePolicyProvider(createStore(build(true)), service).getMergePolicy().getNoCFSRatio(), equalTo(1.0));
        assertThat(new LogDocMergePolicyProvider(createStore(build(0.5)), service).getMergePolicy().getNoCFSRatio(), equalTo(0.5));
        assertThat(new LogDocMergePolicyProvider(createStore(build(1.0)), service).getMergePolicy().getNoCFSRatio(), equalTo(1.0));
        assertThat(new LogDocMergePolicyProvider(createStore(build("true")), service).getMergePolicy().getNoCFSRatio(), equalTo(1.0));
        assertThat(new LogDocMergePolicyProvider(createStore(build("True")), service).getMergePolicy().getNoCFSRatio(), equalTo(1.0));
        assertThat(new LogDocMergePolicyProvider(createStore(build("False")), service).getMergePolicy().getNoCFSRatio(), equalTo(0.0));
        assertThat(new LogDocMergePolicyProvider(createStore(build("false")), service).getMergePolicy().getNoCFSRatio(), equalTo(0.0));
        assertThat(new LogDocMergePolicyProvider(createStore(build(false)), service).getMergePolicy().getNoCFSRatio(), equalTo(0.0));
        assertThat(new LogDocMergePolicyProvider(createStore(build(0)), service).getMergePolicy().getNoCFSRatio(), equalTo(0.0));
        assertThat(new LogDocMergePolicyProvider(createStore(build(0.0)), service).getMergePolicy().getNoCFSRatio(), equalTo(0.0));

    }

    @Test
    public void testInvalidValue() throws IOException {
        IndexSettingsService service = new IndexSettingsService(new Index("test"), EMPTY_SETTINGS);
        try {
            new LogDocMergePolicyProvider(createStore(build(-0.1)), service).getMergePolicy().getNoCFSRatio();
            fail("exception expected");
        } catch (ElasticsearchIllegalArgumentException ex) {

        }
        try {
            new LogDocMergePolicyProvider(createStore(build(1.1)), service).getMergePolicy().getNoCFSRatio();
            fail("exception expected");
        } catch (ElasticsearchIllegalArgumentException ex) {

        }
        try {
            new LogDocMergePolicyProvider(createStore(build("Falsch")), service).getMergePolicy().getNoCFSRatio();
            fail("exception expected");
        } catch (ElasticsearchIllegalArgumentException ex) {

        }

    }

    @Test
    public void testUpdateSettings() throws IOException {
        {
            IndexSettingsService service = new IndexSettingsService(new Index("test"), EMPTY_SETTINGS);
            TieredMergePolicyProvider mp = new TieredMergePolicyProvider(createStore(EMPTY_SETTINGS), service);
            assertThat(mp.getMergePolicy().getNoCFSRatio(), equalTo(0.1));

            service.refreshSettings(build(1.0));
            assertThat(mp.getMergePolicy().getNoCFSRatio(), equalTo(1.0));

            service.refreshSettings(build(0.1));
            assertThat(mp.getMergePolicy().getNoCFSRatio(), equalTo(0.1));

            service.refreshSettings(build(0.0));
            assertThat(mp.getMergePolicy().getNoCFSRatio(), equalTo(0.0));
        }

        {
            IndexSettingsService service = new IndexSettingsService(new Index("test"), EMPTY_SETTINGS);
            LogByteSizeMergePolicyProvider mp = new LogByteSizeMergePolicyProvider(createStore(EMPTY_SETTINGS), service);
            assertThat(mp.getMergePolicy().getNoCFSRatio(), equalTo(0.1));

            service.refreshSettings(build(1.0));
            assertThat(mp.getMergePolicy().getNoCFSRatio(), equalTo(1.0));

            service.refreshSettings(build(0.1));
            assertThat(mp.getMergePolicy().getNoCFSRatio(), equalTo(0.1));

            service.refreshSettings(build(0.0));
            assertThat(mp.getMergePolicy().getNoCFSRatio(), equalTo(0.0));
        }

        {
            IndexSettingsService service = new IndexSettingsService(new Index("test"), EMPTY_SETTINGS);
            LogDocMergePolicyProvider mp = new LogDocMergePolicyProvider(createStore(EMPTY_SETTINGS), service);
            assertThat(mp.getMergePolicy().getNoCFSRatio(), equalTo(0.1));

            service.refreshSettings(build(1.0));
            assertThat(mp.getMergePolicy().getNoCFSRatio(), equalTo(1.0));

            service.refreshSettings(build(0.1));
            assertThat(mp.getMergePolicy().getNoCFSRatio(), equalTo(0.1));

            service.refreshSettings(build(0.0));
            assertThat(mp.getMergePolicy().getNoCFSRatio(), equalTo(0.0));
        }
    }

    public void testLogDocSizeMergePolicySettingsUpdate() throws IOException {
        IndexSettingsService service = new IndexSettingsService(new Index("test"), EMPTY_SETTINGS);
        LogDocMergePolicyProvider mp = new LogDocMergePolicyProvider(createStore(EMPTY_SETTINGS), service);

        assertEquals(mp.getMergePolicy().getMaxMergeDocs(), LogDocMergePolicy.DEFAULT_MAX_MERGE_DOCS);
        service.refreshSettings(ImmutableSettings.builder().put(LogDocMergePolicyProvider.INDEX_MERGE_POLICY_MAX_MERGE_DOCS, LogDocMergePolicy.DEFAULT_MAX_MERGE_DOCS / 2).build());
        assertEquals(mp.getMergePolicy().getMaxMergeDocs(), LogDocMergePolicy.DEFAULT_MAX_MERGE_DOCS / 2);

        assertEquals(mp.getMergePolicy().getMinMergeDocs(), LogDocMergePolicy.DEFAULT_MIN_MERGE_DOCS);
        service.refreshSettings(ImmutableSettings.builder().put(LogDocMergePolicyProvider.INDEX_MERGE_POLICY_MIN_MERGE_DOCS, LogDocMergePolicy.DEFAULT_MIN_MERGE_DOCS / 2).build());
        assertEquals(mp.getMergePolicy().getMinMergeDocs(), LogDocMergePolicy.DEFAULT_MIN_MERGE_DOCS / 2);

        assertTrue(mp.getMergePolicy().getCalibrateSizeByDeletes());
        service.refreshSettings(ImmutableSettings.builder().put(LogDocMergePolicyProvider.INDEX_MERGE_POLICY_CALIBRATE_SIZE_BY_DELETES, false).build());
        assertFalse(mp.getMergePolicy().getCalibrateSizeByDeletes());

        assertEquals(mp.getMergePolicy().getMergeFactor(), LogDocMergePolicy.DEFAULT_MERGE_FACTOR);
        service.refreshSettings(ImmutableSettings.builder().put(LogDocMergePolicyProvider.INDEX_MERGE_POLICY_MERGE_FACTOR, LogDocMergePolicy.DEFAULT_MERGE_FACTOR * 2).build());
        assertEquals(mp.getMergePolicy().getMergeFactor(), LogDocMergePolicy.DEFAULT_MERGE_FACTOR * 2);

        service.refreshSettings(EMPTY_SETTINGS); // update without the settings and see if we stick to the values
        assertEquals(mp.getMergePolicy().getMaxMergeDocs(), LogDocMergePolicy.DEFAULT_MAX_MERGE_DOCS / 2);
        assertEquals(mp.getMergePolicy().getMinMergeDocs(), LogDocMergePolicy.DEFAULT_MIN_MERGE_DOCS / 2);
        assertFalse(mp.getMergePolicy().getCalibrateSizeByDeletes());
        assertEquals(mp.getMergePolicy().getMergeFactor(), LogByteSizeMergePolicy.DEFAULT_MERGE_FACTOR * 2);


        service = new IndexSettingsService(new Index("test"), EMPTY_SETTINGS);
        mp = new LogDocMergePolicyProvider(createStore(ImmutableSettings.builder()
                .put(LogDocMergePolicyProvider.INDEX_MERGE_POLICY_MAX_MERGE_DOCS, LogByteSizeMergePolicy.DEFAULT_MAX_MERGE_DOCS / 2)
                .put(LogDocMergePolicyProvider.INDEX_MERGE_POLICY_MERGE_FACTOR, LogByteSizeMergePolicy.DEFAULT_MERGE_FACTOR / 2)
                .put(LogDocMergePolicyProvider.INDEX_MERGE_POLICY_CALIBRATE_SIZE_BY_DELETES, false)
                .put(LogDocMergePolicyProvider.INDEX_MERGE_POLICY_MIN_MERGE_DOCS, LogDocMergePolicy.DEFAULT_MIN_MERGE_DOCS - 1)
                .build()), service);


        assertEquals(mp.getMergePolicy().getMinMergeDocs(), LogDocMergePolicy.DEFAULT_MIN_MERGE_DOCS - 1);
        assertFalse(mp.getMergePolicy().getCalibrateSizeByDeletes());
        assertEquals(mp.getMergePolicy().getMergeFactor(), LogByteSizeMergePolicy.DEFAULT_MERGE_FACTOR / 2);
        assertEquals(mp.getMergePolicy().getMaxMergeDocs(), LogByteSizeMergePolicy.DEFAULT_MAX_MERGE_DOCS / 2);
    }

    public void testLogByteSizeMergePolicySettingsUpdate() throws IOException {
        IndexSettingsService service = new IndexSettingsService(new Index("test"), EMPTY_SETTINGS);
        LogByteSizeMergePolicyProvider mp = new LogByteSizeMergePolicyProvider(createStore(EMPTY_SETTINGS), service);

        assertEquals(mp.getMergePolicy().getMaxMergeMB(), LogByteSizeMergePolicyProvider.DEFAULT_MAX_MERGE_SIZE.mbFrac(), 0.0d);
        service.refreshSettings(ImmutableSettings.builder().put(LogByteSizeMergePolicyProvider.INDEX_MERGE_POLICY_MAX_MERGE_SIZE, new ByteSizeValue(LogByteSizeMergePolicyProvider.DEFAULT_MAX_MERGE_SIZE.mb() / 2, ByteSizeUnit.MB)).build());
        assertEquals(mp.getMergePolicy().getMaxMergeMB(), new ByteSizeValue(LogByteSizeMergePolicyProvider.DEFAULT_MAX_MERGE_SIZE.mb() / 2, ByteSizeUnit.MB).mbFrac(), 0.0d);

        assertEquals(mp.getMergePolicy().getMinMergeMB(), LogByteSizeMergePolicyProvider.DEFAULT_MIN_MERGE_SIZE.mbFrac(), 0.0d);
        service.refreshSettings(ImmutableSettings.builder().put(LogByteSizeMergePolicyProvider.INDEX_MERGE_POLICY_MIN_MERGE_SIZE, new ByteSizeValue(LogByteSizeMergePolicyProvider.DEFAULT_MIN_MERGE_SIZE.mb() + 1, ByteSizeUnit.MB)).build());
        assertEquals(mp.getMergePolicy().getMinMergeMB(), new ByteSizeValue(LogByteSizeMergePolicyProvider.DEFAULT_MIN_MERGE_SIZE.mb() + 1, ByteSizeUnit.MB).mbFrac(), 0.0d);

        assertTrue(mp.getMergePolicy().getCalibrateSizeByDeletes());
        service.refreshSettings(ImmutableSettings.builder().put(LogByteSizeMergePolicyProvider.INDEX_MERGE_POLICY_CALIBRATE_SIZE_BY_DELETES, false).build());
        assertFalse(mp.getMergePolicy().getCalibrateSizeByDeletes());

        assertEquals(mp.getMergePolicy().getMergeFactor(), LogByteSizeMergePolicy.DEFAULT_MERGE_FACTOR);
        service.refreshSettings(ImmutableSettings.builder().put(LogByteSizeMergePolicyProvider.INDEX_MERGE_POLICY_MERGE_FACTOR, LogByteSizeMergePolicy.DEFAULT_MERGE_FACTOR / 2).build());
        assertEquals(mp.getMergePolicy().getMergeFactor(), LogByteSizeMergePolicy.DEFAULT_MERGE_FACTOR / 2);

        assertEquals(mp.getMergePolicy().getMaxMergeDocs(), LogByteSizeMergePolicy.DEFAULT_MAX_MERGE_DOCS);
        service.refreshSettings(ImmutableSettings.builder().put(LogByteSizeMergePolicyProvider.INDEX_MERGE_POLICY_MAX_MERGE_DOCS, LogByteSizeMergePolicy.DEFAULT_MAX_MERGE_DOCS / 2).build());
        assertEquals(mp.getMergePolicy().getMaxMergeDocs(), LogByteSizeMergePolicy.DEFAULT_MAX_MERGE_DOCS / 2);

        service.refreshSettings(EMPTY_SETTINGS); // update without the settings and see if we stick to the values
        assertEquals(mp.getMergePolicy().getMaxMergeMB(), new ByteSizeValue(LogByteSizeMergePolicyProvider.DEFAULT_MAX_MERGE_SIZE.mb() / 2, ByteSizeUnit.MB).mbFrac(), 0.0d);
        assertEquals(mp.getMergePolicy().getMinMergeMB(), new ByteSizeValue(LogByteSizeMergePolicyProvider.DEFAULT_MIN_MERGE_SIZE.mb() + 1, ByteSizeUnit.MB).mbFrac(), 0.0d);
        assertFalse(mp.getMergePolicy().getCalibrateSizeByDeletes());
        assertEquals(mp.getMergePolicy().getMergeFactor(), LogByteSizeMergePolicy.DEFAULT_MERGE_FACTOR / 2);
        assertEquals(mp.getMergePolicy().getMaxMergeDocs(), LogByteSizeMergePolicy.DEFAULT_MAX_MERGE_DOCS / 2);


        service = new IndexSettingsService(new Index("test"), EMPTY_SETTINGS);
        mp = new LogByteSizeMergePolicyProvider(createStore(ImmutableSettings.builder()
                .put(LogByteSizeMergePolicyProvider.INDEX_MERGE_POLICY_MAX_MERGE_DOCS, LogByteSizeMergePolicy.DEFAULT_MAX_MERGE_DOCS * 2)
                .put(LogByteSizeMergePolicyProvider.INDEX_MERGE_POLICY_MERGE_FACTOR, LogByteSizeMergePolicy.DEFAULT_MERGE_FACTOR * 2)
                .put(LogByteSizeMergePolicyProvider.INDEX_MERGE_POLICY_MAX_MERGE_SIZE, new ByteSizeValue(LogByteSizeMergePolicyProvider.DEFAULT_MAX_MERGE_SIZE.mb() / 2, ByteSizeUnit.MB))
                .put(LogByteSizeMergePolicyProvider.INDEX_MERGE_POLICY_CALIBRATE_SIZE_BY_DELETES, false)
                .put(LogByteSizeMergePolicyProvider.INDEX_MERGE_POLICY_MIN_MERGE_SIZE, new ByteSizeValue(LogByteSizeMergePolicyProvider.DEFAULT_MIN_MERGE_SIZE.mb() + 1, ByteSizeUnit.MB))
                .build()), service);


        assertEquals(mp.getMergePolicy().getMaxMergeMB(), new ByteSizeValue(LogByteSizeMergePolicyProvider.DEFAULT_MAX_MERGE_SIZE.mb() / 2, ByteSizeUnit.MB).mbFrac(), 0.0d);
        assertEquals(mp.getMergePolicy().getMinMergeMB(), new ByteSizeValue(LogByteSizeMergePolicyProvider.DEFAULT_MIN_MERGE_SIZE.mb() + 1, ByteSizeUnit.MB).mbFrac(), 0.0d);
        assertFalse(mp.getMergePolicy().getCalibrateSizeByDeletes());
        assertEquals(mp.getMergePolicy().getMergeFactor(), LogByteSizeMergePolicy.DEFAULT_MERGE_FACTOR * 2);
        assertEquals(mp.getMergePolicy().getMaxMergeDocs(), LogByteSizeMergePolicy.DEFAULT_MAX_MERGE_DOCS * 2);
    }

    public void testTieredMergePolicySettingsUpdate() throws IOException {
        IndexSettingsService service = new IndexSettingsService(new Index("test"), EMPTY_SETTINGS);
        TieredMergePolicyProvider mp = new TieredMergePolicyProvider(createStore(EMPTY_SETTINGS), service);
        assertThat(mp.getMergePolicy().getNoCFSRatio(), equalTo(0.1));

        assertEquals(mp.getMergePolicy().getForceMergeDeletesPctAllowed(), TieredMergePolicyProvider.DEFAULT_EXPUNGE_DELETES_ALLOWED, 0.0d);
        service.refreshSettings(ImmutableSettings.builder().put(TieredMergePolicyProvider.INDEX_MERGE_POLICY_EXPUNGE_DELETES_ALLOWED, TieredMergePolicyProvider.DEFAULT_EXPUNGE_DELETES_ALLOWED + 1.0d).build());
        assertEquals(mp.getMergePolicy().getForceMergeDeletesPctAllowed(), TieredMergePolicyProvider.DEFAULT_EXPUNGE_DELETES_ALLOWED + 1.0d, 0.0d);

        assertEquals(mp.getMergePolicy().getFloorSegmentMB(), TieredMergePolicyProvider.DEFAULT_FLOOR_SEGMENT.mbFrac(), 0);
        service.refreshSettings(ImmutableSettings.builder().put(TieredMergePolicyProvider.INDEX_MERGE_POLICY_FLOOR_SEGMENT, new ByteSizeValue(TieredMergePolicyProvider.DEFAULT_FLOOR_SEGMENT.mb() + 1, ByteSizeUnit.MB)).build());
        assertEquals(mp.getMergePolicy().getFloorSegmentMB(), new ByteSizeValue(TieredMergePolicyProvider.DEFAULT_FLOOR_SEGMENT.mb() + 1, ByteSizeUnit.MB).mbFrac(), 0.001);

        assertEquals(mp.getMergePolicy().getMaxMergeAtOnce(), TieredMergePolicyProvider.DEFAULT_MAX_MERGE_AT_ONCE);
        service.refreshSettings(ImmutableSettings.builder().put(TieredMergePolicyProvider.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE, TieredMergePolicyProvider.DEFAULT_MAX_MERGE_AT_ONCE -1 ).build());
        assertEquals(mp.getMergePolicy().getMaxMergeAtOnce(), TieredMergePolicyProvider.DEFAULT_MAX_MERGE_AT_ONCE-1);

        assertEquals(mp.getMergePolicy().getMaxMergeAtOnceExplicit(), TieredMergePolicyProvider.DEFAULT_MAX_MERGE_AT_ONCE_EXPLICIT);
        service.refreshSettings(ImmutableSettings.builder().put(TieredMergePolicyProvider.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_EXPLICIT, TieredMergePolicyProvider.DEFAULT_MAX_MERGE_AT_ONCE_EXPLICIT -1 ).build());
        assertEquals(mp.getMergePolicy().getMaxMergeAtOnceExplicit(), TieredMergePolicyProvider.DEFAULT_MAX_MERGE_AT_ONCE_EXPLICIT-1);

        assertEquals(mp.getMergePolicy().getMaxMergedSegmentMB(), TieredMergePolicyProvider.DEFAULT_MAX_MERGED_SEGMENT.mbFrac(), 0.0001);
        service.refreshSettings(ImmutableSettings.builder().put(TieredMergePolicyProvider.INDEX_MERGE_POLICY_MAX_MERGED_SEGMENT, new ByteSizeValue(TieredMergePolicyProvider.DEFAULT_MAX_MERGED_SEGMENT.bytes() + 1)).build());
        assertEquals(mp.getMergePolicy().getMaxMergedSegmentMB(), new ByteSizeValue(TieredMergePolicyProvider.DEFAULT_MAX_MERGED_SEGMENT.bytes() + 1).mbFrac(), 0.0001);

        assertEquals(mp.getMergePolicy().getReclaimDeletesWeight(), TieredMergePolicyProvider.DEFAULT_RECLAIM_DELETES_WEIGHT, 0);
        service.refreshSettings(ImmutableSettings.builder().put(TieredMergePolicyProvider.INDEX_MERGE_POLICY_RECLAIM_DELETES_WEIGHT, TieredMergePolicyProvider.DEFAULT_RECLAIM_DELETES_WEIGHT + 1 ).build());
        assertEquals(mp.getMergePolicy().getReclaimDeletesWeight(), TieredMergePolicyProvider.DEFAULT_RECLAIM_DELETES_WEIGHT + 1, 0);

        assertEquals(mp.getMergePolicy().getSegmentsPerTier(), TieredMergePolicyProvider.DEFAULT_SEGMENTS_PER_TIER, 0);
        service.refreshSettings(ImmutableSettings.builder().put(TieredMergePolicyProvider.INDEX_MERGE_POLICY_SEGMENTS_PER_TIER, TieredMergePolicyProvider.DEFAULT_SEGMENTS_PER_TIER + 1 ).build());
        assertEquals(mp.getMergePolicy().getSegmentsPerTier(), TieredMergePolicyProvider.DEFAULT_SEGMENTS_PER_TIER + 1, 0);

        service.refreshSettings(EMPTY_SETTINGS); // update without the settings and see if we stick to the values

        assertEquals(mp.getMergePolicy().getForceMergeDeletesPctAllowed(), TieredMergePolicyProvider.DEFAULT_EXPUNGE_DELETES_ALLOWED + 1.0d, 0.0d);
        assertEquals(mp.getMergePolicy().getFloorSegmentMB(), new ByteSizeValue(TieredMergePolicyProvider.DEFAULT_FLOOR_SEGMENT.mb() + 1, ByteSizeUnit.MB).mbFrac(), 0.001);
        assertEquals(mp.getMergePolicy().getMaxMergeAtOnce(), TieredMergePolicyProvider.DEFAULT_MAX_MERGE_AT_ONCE-1);
        assertEquals(mp.getMergePolicy().getMaxMergeAtOnceExplicit(), TieredMergePolicyProvider.DEFAULT_MAX_MERGE_AT_ONCE_EXPLICIT-1);
        assertEquals(mp.getMergePolicy().getMaxMergedSegmentMB(), new ByteSizeValue(TieredMergePolicyProvider.DEFAULT_MAX_MERGED_SEGMENT.bytes() + 1).mbFrac(), 0.0001);
        assertEquals(mp.getMergePolicy().getReclaimDeletesWeight(), TieredMergePolicyProvider.DEFAULT_RECLAIM_DELETES_WEIGHT + 1, 0);
        assertEquals(mp.getMergePolicy().getSegmentsPerTier(), TieredMergePolicyProvider.DEFAULT_SEGMENTS_PER_TIER + 1, 0);
    }

    public Settings build(String value) {
        return ImmutableSettings.builder().put(AbstractMergePolicyProvider.INDEX_COMPOUND_FORMAT, value).build();
    }

    public Settings build(double value) {
        return ImmutableSettings.builder().put(AbstractMergePolicyProvider.INDEX_COMPOUND_FORMAT, value).build();
    }

    public Settings build(int value) {
        return ImmutableSettings.builder().put(AbstractMergePolicyProvider.INDEX_COMPOUND_FORMAT, value).build();
    }

    public Settings build(boolean value) {
        return ImmutableSettings.builder().put(AbstractMergePolicyProvider.INDEX_COMPOUND_FORMAT, value).build();
    }

    protected Store createStore(Settings settings) throws IOException {
        final DirectoryService directoryService = new DirectoryService(shardId, EMPTY_SETTINGS) {
            @Override
            public Directory newDirectory() throws IOException {
                return  new RAMDirectory() ;
            }

            @Override
            public long throttleTimeInNanos() {
                return 0;
            }
        };
        return new Store(shardId, settings, directoryService, new DummyShardLock(shardId));
    }

}
