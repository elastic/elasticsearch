/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.reshard;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.Directory;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.shard.ShardSplittingQuery;
import org.elasticsearch.lucene.util.MatchAllBitSet;
import org.elasticsearch.test.ESTestCase;

public class ReshardUnownedBitsetCacheTests extends ESTestCase {

    public void testCloseClearsEntries() throws Exception {
        Directory directory = newDirectory();
        try (IndexWriter iw = new IndexWriter(directory, newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
            var document = new Document();
            document.add(new StringField(IdFieldMapper.NAME, Uid.encodeId("a"), Field.Store.NO));
            iw.addDocument(document);
            iw.commit();
        }

        Settings settings = Settings.builder().put(ReshardUnownedBitsetCache.CACHE_SIZE_SETTING.getKey(), "256mb").build();
        ReshardUnownedBitsetCache cache = new ReshardUnownedBitsetCache(settings);

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            IndexMetadata metadata = IndexMetadata.builder("idx")
                .settings(indexSettings(IndexVersion.current(), 2, 0))
                .numberOfShards(2)
                .numberOfReplicas(0)
                .build();
            ShardSplittingQuery query = new ShardSplittingQuery(metadata, 0, false);

            LeafReaderContext ctx = reader.leaves().get(0);
            cache.getBitSet(query, ctx);
            assertEquals(1, cache.entryCount());
            cache.verifyInternalConsistency();
        } finally {
            cache.close();
            directory.close();
        }
        assertEquals(0, cache.entryCount());
    }

    public void testTinyMaxSizeStillCompletesLookup() throws Exception {
        Directory directory = newDirectory();
        try (IndexWriter iw = new IndexWriter(directory, newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
            var document = new Document();
            document.add(new StringField(IdFieldMapper.NAME, Uid.encodeId("id"), Field.Store.NO));
            iw.addDocument(document);
            iw.commit();
        }

        Settings settings = Settings.builder().put(ReshardUnownedBitsetCache.CACHE_SIZE_SETTING.getKey(), "1b").build();
        ReshardUnownedBitsetCache cache = new ReshardUnownedBitsetCache(settings);

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            IndexMetadata metadata = IndexMetadata.builder("idx")
                .settings(indexSettings(IndexVersion.current(), 2, 0))
                .numberOfShards(2)
                .numberOfReplicas(0)
                .build();
            ShardSplittingQuery query = new ShardSplittingQuery(metadata, 0, false);

            LeafReaderContext ctx = reader.leaves().get(0);
            assertNotNull(cache.getBitSet(query, ctx));
            // Cache is too small to hold an entry
            assertEquals(0, cache.entryCount());
        } finally {
            cache.close();
            directory.close();
        }
    }

    /** Same query used as DocumentSubsetBitsetCache tests for MatchAll rewritten path */
    public void testMatchAllRewrittenUsesMatchAllBitSet() throws Exception {
        Directory directory = newDirectory();
        try (IndexWriter iw = new IndexWriter(directory, newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
            iw.addDocument(new Document());
            iw.commit();
        }

        Settings settings = Settings.builder().put(ReshardUnownedBitsetCache.CACHE_SIZE_SETTING.getKey(), "256mb").build();
        ReshardUnownedBitsetCache cache = new ReshardUnownedBitsetCache(settings);

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            IndexMetadata meta = IndexMetadata.builder("idx")
                .settings(indexSettings(IndexVersion.current(), 2, 0))
                .numberOfShards(2)
                .numberOfReplicas(0)
                .build();
            ShardSplittingQuery query = new ShardSplittingQuery(meta, 222, false);

            LeafReaderContext ctx = reader.leaves().get(0);
            assertTrue(cache.getBitSet(query, ctx) instanceof MatchAllBitSet);
        } finally {
            cache.close();
            directory.close();
        }
    }

    public void testMissThenHitOnSameCoreAndQuery() throws Exception {
        Directory directory = newDirectory();
        try (IndexWriter iw = new IndexWriter(directory, newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
            var document = new Document();
            document.add(new StringField(IdFieldMapper.NAME, Uid.encodeId("only"), Field.Store.NO));
            iw.addDocument(document);
            iw.commit();
        }

        Settings settings = Settings.builder().put(ReshardUnownedBitsetCache.CACHE_SIZE_SETTING.getKey(), "256mb").build();
        ReshardUnownedBitsetCache cache = new ReshardUnownedBitsetCache(settings);

        try {
            DirectoryReader reader1 = DirectoryReader.open(directory);
            DirectoryReader reader2 = DirectoryReader.open(directory);

            IndexMetadata meta = IndexMetadata.builder("idx")
                .settings(indexSettings(IndexVersion.current(), 2, 0))
                .numberOfShards(2)
                .numberOfReplicas(0)
                .build();
            ShardSplittingQuery query = new ShardSplittingQuery(meta, 0, false);

            LeafReaderContext ctx1 = reader1.leaves().get(0);
            LeafReaderContext ctx2 = reader2.leaves().get(0);

            cache.getBitSet(query, ctx1);
            cache.getBitSet(query, ctx2);

            cache.verifyInternalConsistency();
            assertEquals(1L, cache.usageStats().get("misses"));
            assertEquals(1L, cache.usageStats().get("hits"));

            reader1.close();
            reader2.close();
        } finally {
            cache.close();
            directory.close();
        }
    }
}
