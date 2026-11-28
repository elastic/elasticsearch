/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.store;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.DummyShardLock;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

public class ShardCodecStateDirectoryTests extends ESTestCase {

    private static final IndexSettings INDEX_SETTINGS = IndexSettingsModule.newIndexSettings(
        "index",
        Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build()
    );

    public void testWrapIfEnabledIdempotent() throws IOException {
        assumeTrue("feature flag must be enabled for this test", ShardCodecStateDirectory.FEATURE_FLAG.isEnabled());
        try (Directory raw = newDirectory()) {
            Directory wrapped = ShardCodecStateDirectory.wrapIfEnabled(raw);
            assertThat(wrapped, not(sameInstance(raw)));
            assertThat(DelegatingByteSizeDirectory.unwrap(wrapped, ShardCodecStateDirectory.class), notNullValue());

            Directory outer = new FilterDirectory(wrapped) {};
            assertThat(ShardCodecStateDirectory.wrapIfEnabled(outer), sameInstance(outer));
        }
    }

    public void testWrapIfEnabledNoOpWhenFlagDisabled() throws IOException {
        assumeFalse("only meaningful when feature flag is off", ShardCodecStateDirectory.FEATURE_FLAG.isEnabled());
        try (Directory raw = newDirectory()) {
            assertThat(ShardCodecStateDirectory.wrapIfEnabled(raw), sameInstance(raw));
        }
    }

    public void testGetOrCreateKeepsTypesApart() throws IOException {
        try (Directory raw = newDirectory()) {
            Directory directory = new FilterDirectory(new ShardCodecStateDirectory(raw)) {};
            StateA a = ShardCodecStateDirectory.getOrCreate(directory, StateA.class, StateA::new);
            StateB b = ShardCodecStateDirectory.getOrCreate(directory, StateB.class, StateB::new);
            assertThat(ShardCodecStateDirectory.getOrCreate(directory, StateA.class, StateA::new), sameInstance(a));
            assertThat(ShardCodecStateDirectory.getOrCreate(directory, StateB.class, StateB::new), sameInstance(b));
        }
    }

    public void testGetOrCreateSharesNothingWithoutTheDirectory() throws IOException {
        try (Directory raw = newDirectory()) {
            Directory directory = randomBoolean() ? raw : new FilterDirectory(raw) {};
            StateA a = ShardCodecStateDirectory.getOrCreate(directory, StateA.class, StateA::new);
            assertThat(a, notNullValue());
            assertThat(ShardCodecStateDirectory.getOrCreate(directory, StateA.class, StateA::new), not(sameInstance(a)));
        }
    }

    public void testRamBytesUsedSumsStates() throws IOException {
        try (Directory raw = newDirectory()) {
            ShardCodecStateDirectory stateDirectory = new ShardCodecStateDirectory(raw);
            long empty = stateDirectory.ramBytesUsed();
            // every wrapped shard holds the directory and its map of states, even before it has any state
            assertThat(
                empty,
                equalTo(
                    RamUsageEstimator.shallowSizeOfInstance(ShardCodecStateDirectory.class) + RamUsageEstimator.shallowSizeOfInstance(
                        ConcurrentHashMap.class
                    )
                )
            );
            ShardCodecStateDirectory.getOrCreate(stateDirectory, StateA.class, StateA::new);
            assertThat(stateDirectory.ramBytesUsed(), equalTo(empty + RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY + 1000));
            ShardCodecStateDirectory.getOrCreate(stateDirectory, StateB.class, StateB::new);
            assertThat(stateDirectory.ramBytesUsed(), equalTo(empty + 2 * RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY + 1000 + 2000));
        }
    }

    /**
     * Segments written and read through a shard's {@link Store}, whether flushed or merged, reach the shard's state through
     * {@code SegmentInfo.dir}, which is the only handle a codec has on its shard.
     */
    public void testSegmentsOfStoreReachState() throws IOException {
        assumeTrue("feature flag must be enabled for this test", ShardCodecStateDirectory.FEATURE_FLAG.isEnabled());
        ShardId shardId = new ShardId("index", "_na_", 1);
        Store store = new Store(shardId, INDEX_SETTINGS, newDirectory(), new DummyShardLock(shardId));
        try {
            StateA state = ShardCodecStateDirectory.getOrCreate(store.directory(), StateA.class, StateA::new);

            int flushes = between(2, 5);
            try (IndexWriter writer = new IndexWriter(store.directory(), new IndexWriterConfig())) {
                for (int i = 0; i < flushes; i++) {
                    Document doc = new Document();
                    doc.add(new StringField("id", Integer.toString(i), Field.Store.NO));
                    writer.addDocument(doc);
                    writer.flush();
                }
                try (DirectoryReader reader = DirectoryReader.open(writer)) {
                    assertSegmentsReachState(segmentInfos(reader), flushes, IndexWriter.SOURCE_FLUSH, state);
                }

                writer.forceMerge(1);
                writer.commit();
                try (DirectoryReader reader = DirectoryReader.open(writer)) {
                    assertSegmentsReachState(segmentInfos(reader), 1, IndexWriter.SOURCE_MERGE, state);
                }
            }
            // A reader opened from the commit, as a shard opens one on recovery, reads SegmentInfo.dir from the Store too.
            try (DirectoryReader reader = DirectoryReader.open(store.directory())) {
                assertSegmentsReachState(segmentInfos(reader), 1, IndexWriter.SOURCE_MERGE, state);
            }
        } finally {
            store.close();
        }
    }

    private static void assertSegmentsReachState(List<SegmentInfo> segments, int expectedSegments, String expectedSource, StateA state) {
        assertThat(segments, hasSize(expectedSegments));
        for (SegmentInfo segment : segments) {
            assertThat(segment.getDiagnostics().get(IndexWriter.SOURCE), equalTo(expectedSource));
            assertThat(
                ShardCodecStateDirectory.getOrCreate(
                    segment.dir,
                    StateA.class,
                    () -> { throw new AssertionError("state must already exist"); }
                ),
                sameInstance(state)
            );
        }
    }

    private static List<SegmentInfo> segmentInfos(DirectoryReader reader) {
        List<SegmentInfo> infos = new ArrayList<>();
        for (LeafReaderContext leaf : reader.leaves()) {
            SegmentReader segmentReader = Lucene.tryUnwrapSegmentReader(leaf.reader());
            assertThat(segmentReader, notNullValue());
            infos.add(segmentReader.getSegmentInfo().info);
        }
        return infos;
    }

    private static final class StateA implements Accountable {
        @Override
        public long ramBytesUsed() {
            return 1000;
        }
    }

    private static final class StateB implements Accountable {
        @Override
        public long ramBytesUsed() {
            return 2000;
        }
    }
}
