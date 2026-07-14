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
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BitSet;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.shard.ShardSplittingQuery;
import org.elasticsearch.lucene.util.MatchAllBitSet;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.sameInstance;

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

    public void testMatchAllRewrittenUsesMatchAllBitSet() throws Exception {
        Directory directory = newDirectory();
        try (IndexWriter iw = new IndexWriter(directory, newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
            var document = new Document();
            document.add(new StringField(IdFieldMapper.NAME, Uid.encodeId("only"), Field.Store.NO));
            iw.addDocument(document);
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

    // When a NULL bitset is returned (we check cached path twice). Note that
    // cache still holds one entry (NULL_MARKER).
    public void testGetBitSetReturnsNullWhenQueryMatchesNoDocuments() throws Exception {
        Directory directory = newDirectory();
        try (IndexWriter iw = new IndexWriter(directory, newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
            var document = new Document();
            document.add(new StringField(IdFieldMapper.NAME, Uid.encodeId("only"), Field.Store.NO));
            iw.addDocument(document);
            iw.commit();
        }
        Settings settings = Settings.builder().put(ReshardUnownedBitsetCache.CACHE_SIZE_SETTING.getKey(), "256mb").build();
        ReshardUnownedBitsetCache cache = new ReshardUnownedBitsetCache(settings);
        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            LeafReaderContext ctx = reader.leaves().get(0);
            assertNull(ReshardUnownedBitsetCache.computeBitSet(Queries.NO_DOCS_INSTANCE, ctx));
            assertNull(cache.getBitSet(Queries.NO_DOCS_INSTANCE, ctx));
            assertEquals(1, cache.entryCount());
            assertNull(cache.getBitSet(Queries.NO_DOCS_INSTANCE, ctx));
            cache.verifyInternalConsistency();
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

        // Same leaf context twice exercises the intended hit path (same core key + same query).
        try (DirectoryReader reader = DirectoryReader.open(directory)) {

            IndexMetadata meta = IndexMetadata.builder("idx")
                .settings(indexSettings(IndexVersion.current(), 2, 0))
                .numberOfShards(2)
                .numberOfReplicas(0)
                .build();
            ShardSplittingQuery query = new ShardSplittingQuery(meta, 0, false);

            LeafReaderContext ctx = reader.leaves().get(0);

            final BitSet bitSet1 = cache.getBitSet(query, ctx);
            final BitSet bitSet1Again = cache.getBitSet(query, ctx);

            assertThat(bitSet1Again, sameInstance(bitSet1));
            assertEquals(1, cache.entryCount());
            assertEquals(1L, cache.usageStats().get("misses"));
            assertEquals(1L, cache.usageStats().get("hits"));
        } finally {
            cache.close();
            directory.close();
        }
    }

    public void testKeysByIndexCleanedWhenComputeFails() throws Exception {
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
            LeafReaderContext ctx = reader.leaves().get(0);
            Query throwingQuery = new ThrowingRewriteQuery();

            ExecutionException ex = expectThrows(ExecutionException.class, () -> cache.getBitSet(throwingQuery, ctx));
            assertTrue(ex.getCause() instanceof IOException);

            cache.verifyInternalConsistency();
            assertEquals(0, cache.entryCount());
        } finally {
            cache.close();
            directory.close();
        }
    }

    private static final class ThrowingRewriteQuery extends Query {
        @Override
        public Query rewrite(IndexSearcher indexSearcher) throws IOException {
            throw new IOException("simulated compute failure");
        }

        @Override
        public String toString(String field) {
            return "throwing";
        }

        @Override
        public void visit(QueryVisitor visitor) {
            visitor.visitLeaf(this);
        }

        @Override
        public boolean equals(Object o) {
            return sameClassAs(o);
        }

        @Override
        public int hashCode() {
            return classHash();
        }
    }
}
