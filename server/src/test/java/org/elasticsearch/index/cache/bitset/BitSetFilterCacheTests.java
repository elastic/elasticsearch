/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.cache.bitset;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BitSet;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.lucene.util.MatchAllBitSet;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.cluster.node.DiscoveryNode.STATELESS_ENABLED_SETTING_NAME;
import static org.elasticsearch.index.IndexSettings.INDEX_FAST_REFRESH_SETTING;
import static org.elasticsearch.index.cache.bitset.BitsetFilterCache.INDEX_LOAD_RANDOM_ACCESS_FILTERS_EAGERLY_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;

public class BitSetFilterCacheTests extends ESTestCase {

    private static final IndexSettings INDEX_SETTINGS = IndexSettingsModule.newIndexSettings("test", Settings.EMPTY);

    private static int matchCount(BitSetProducer producer, IndexReader reader) throws IOException {
        int count = 0;
        for (LeafReaderContext ctx : reader.leaves()) {
            final BitSet bitSet = producer.getBitSet(ctx);
            if (bitSet != null) {
                count += bitSet.cardinality();
            }
        }
        return count;
    }

    public void testInvalidateEntries() throws Exception {
        IndexWriter writer = new IndexWriter(
            new ByteBuffersDirectory(),
            new IndexWriterConfig(new StandardAnalyzer()).setMergePolicy(new LogByteSizeMergePolicy())
        );
        Document document = new Document();
        document.add(new StringField("field", "value", Field.Store.NO));
        writer.addDocument(document);
        writer.commit();

        document = new Document();
        document.add(new StringField("field", "value", Field.Store.NO));
        writer.addDocument(document);
        writer.commit();

        document = new Document();
        document.add(new StringField("field", "value", Field.Store.NO));
        writer.addDocument(document);
        writer.commit();

        DirectoryReader reader = DirectoryReader.open(writer);
        reader = ElasticsearchDirectoryReader.wrap(reader, new ShardId("test", "_na_", 0));

        BitsetFilterCache cache = new BitsetFilterCache(INDEX_SETTINGS, new BitsetFilterCache.Listener() {
            @Override
            public void onCache(ShardId shardId, Accountable accountable) {

            }

            @Override
            public void onRemoval(ShardId shardId, Accountable accountable) {

            }
        });
        BitSetProducer filter = cache.getBitSetProducer(new TermQuery(new Term("field", "value")));
        assertThat(matchCount(filter, reader), equalTo(3));

        // now cached
        assertThat(matchCount(filter, reader), equalTo(3));
        // There are 3 segments
        assertThat(cache.getLoadedFilters().weight(), equalTo(3L));

        writer.forceMerge(1);
        reader.close();
        reader = DirectoryReader.open(writer);
        reader = ElasticsearchDirectoryReader.wrap(reader, new ShardId("test", "_na_", 0));

        assertThat(matchCount(filter, reader), equalTo(3));

        // now cached
        assertThat(matchCount(filter, reader), equalTo(3));
        // Only one segment now, so the size must be 1
        assertThat(cache.getLoadedFilters().weight(), equalTo(1L));

        reader.close();
        writer.close();
        // There is no reference from readers and writer to any segment in the test index, so the size in the fbs cache must be 0
        assertThat(cache.getLoadedFilters().weight(), equalTo(0L));
    }

    public void testListener() throws IOException {
        IndexWriter writer = new IndexWriter(
            new ByteBuffersDirectory(),
            new IndexWriterConfig(new StandardAnalyzer()).setMergePolicy(new LogByteSizeMergePolicy())
        );
        Document document = new Document();
        document.add(new StringField("field", "value", Field.Store.NO));
        writer.addDocument(document);
        writer.commit();
        final DirectoryReader writerReader = DirectoryReader.open(writer);
        final IndexReader reader = ElasticsearchDirectoryReader.wrap(writerReader, new ShardId("test", "_na_", 0));

        final AtomicLong stats = new AtomicLong();
        final AtomicInteger onCacheCalls = new AtomicInteger();
        final AtomicInteger onRemoveCalls = new AtomicInteger();

        final BitsetFilterCache cache = new BitsetFilterCache(INDEX_SETTINGS, new BitsetFilterCache.Listener() {
            @Override
            public void onCache(ShardId shardId, Accountable accountable) {
                onCacheCalls.incrementAndGet();
                stats.addAndGet(accountable.ramBytesUsed());
                if (writerReader != reader) {
                    assertNotNull(shardId);
                    assertEquals("test", shardId.getIndexName());
                    assertEquals(0, shardId.id());
                } else {
                    assertNull(shardId);
                }
            }

            @Override
            public void onRemoval(ShardId shardId, Accountable accountable) {
                onRemoveCalls.incrementAndGet();
                stats.addAndGet(-accountable.ramBytesUsed());
                if (writerReader != reader) {
                    assertNotNull(shardId);
                    assertEquals("test", shardId.getIndexName());
                    assertEquals(0, shardId.id());
                } else {
                    assertNull(shardId);
                }
            }
        });
        BitSetProducer filter = cache.getBitSetProducer(new TermQuery(new Term("field", "value")));
        assertThat(matchCount(filter, reader), equalTo(1));
        assertTrue(stats.get() > 0);
        assertEquals(1, onCacheCalls.get());
        assertEquals(0, onRemoveCalls.get());
        IOUtils.close(reader, writer);
        assertEquals(1, onRemoveCalls.get());
        assertEquals(0, stats.get());
    }

    public void testStats() throws IOException {
        Directory directory = newDirectory();
        IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig());
        int numDocs = randomIntBetween(2000, 5000);
        for (int i = 0; i < numDocs; i++) {
            Document d = new Document();
            d.add(new LongPoint("f", i));
            writer.addDocument(d);
        }
        writer.commit();
        writer.forceMerge(1);
        IndexReader reader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("test", "_na_", 0));
        assertThat(reader.leaves(), hasSize(1));
        assertThat(reader.numDocs(), equalTo(numDocs));

        final AtomicLong stats = new AtomicLong();
        final BitsetFilterCache cache = new BitsetFilterCache(INDEX_SETTINGS, new BitsetFilterCache.Listener() {
            @Override
            public void onCache(ShardId shardId, Accountable accountable) {
                stats.addAndGet(accountable.ramBytesUsed());
            }

            @Override
            public void onRemoval(ShardId shardId, Accountable accountable) {
                stats.addAndGet(-accountable.ramBytesUsed());
            }
        });
        // match all
        Query matchAll = randomBoolean() ? LongPoint.newRangeQuery("f", 0, numDocs + between(0, 1000)) : new MatchAllDocsQuery();
        BitSetProducer bitSetProducer = cache.getBitSetProducer(matchAll);
        BitSet bitset = bitSetProducer.getBitSet(reader.leaves().get(0));
        assertThat(bitset, instanceOf(MatchAllBitSet.class));
        long usedBytes = stats.get();
        assertThat(usedBytes, lessThan(32L));
        // range
        bitSetProducer = cache.getBitSetProducer(LongPoint.newRangeQuery("f", 0, between(1000, 2000)));
        bitSetProducer.getBitSet(reader.leaves().get(0));
        usedBytes = stats.get() - usedBytes;
        assertThat(usedBytes, greaterThan(256L));
        IOUtils.close(cache, reader, writer, directory);
    }

    public void testSetNullListener() {
        try {
            new BitsetFilterCache(INDEX_SETTINGS, null);
            fail("listener can't be null");
        } catch (IllegalArgumentException ex) {
            assertEquals("listener must not be null", ex.getMessage());
            // all is well
        }
    }

    public void testRejectOtherIndex() throws IOException {
        BitsetFilterCache cache = new BitsetFilterCache(INDEX_SETTINGS, new BitsetFilterCache.Listener() {
            @Override
            public void onCache(ShardId shardId, Accountable accountable) {

            }

            @Override
            public void onRemoval(ShardId shardId, Accountable accountable) {

            }
        });

        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());
        writer.addDocument(new Document());
        DirectoryReader reader = DirectoryReader.open(writer);
        writer.close();
        reader = ElasticsearchDirectoryReader.wrap(reader, new ShardId("test2", "_na_", 0));

        BitSetProducer producer = cache.getBitSetProducer(new MatchAllDocsQuery());

        try {
            producer.getBitSet(reader.leaves().get(0));
            fail();
        } catch (IllegalStateException expected) {
            assertEquals("Trying to load bit set for index [test2] with cache of index [test]", expected.getMessage());
        } finally {
            IOUtils.close(reader, dir);
        }
    }

    public void testShouldLoadRandomAccessFiltersEagerly() {
        var values = List.of(true, false);
        for (var hasIndexRole : values) {
            for (var loadFiltersEagerly : values) {
                for (var isStateless : values) {
                    for (var fastRefresh : values) {
                        if (isStateless == false && fastRefresh) {
                            // fast refresh is only relevant for stateless indices
                            continue;
                        }

                        boolean result = BitsetFilterCache.shouldLoadRandomAccessFiltersEagerly(
                            bitsetFilterCacheSettings(isStateless, hasIndexRole, loadFiltersEagerly, fastRefresh)
                        );
                        if (isStateless) {
                            assertEquals(loadFiltersEagerly && ((hasIndexRole && fastRefresh) || hasIndexRole == false), result);
                        } else {
                            assertEquals(loadFiltersEagerly, result);
                        }
                    }
                }
            }
        }
    }

    private IndexSettings bitsetFilterCacheSettings(
        boolean isStateless,
        boolean hasIndexRole,
        boolean loadFiltersEagerly,
        boolean fastRefresh
    ) {
        var indexSettingsBuilder = Settings.builder()
            .put(INDEX_LOAD_RANDOM_ACCESS_FILTERS_EAGERLY_SETTING.getKey(), loadFiltersEagerly)
            .put(INDEX_FAST_REFRESH_SETTING.getKey(), fastRefresh);

        var nodeSettingsBuilder = Settings.builder()
            .putList(
                NodeRoleSettings.NODE_ROLES_SETTING.getKey(),
                hasIndexRole ? DiscoveryNodeRole.INDEX_ROLE.roleName() : DiscoveryNodeRole.SEARCH_ROLE.roleName()
            )
            .put(STATELESS_ENABLED_SETTING_NAME, isStateless);

        return IndexSettingsModule.newIndexSettings(
            new Index("index", IndexMetadata.INDEX_UUID_NA_VALUE),
            indexSettingsBuilder.build(),
            nodeSettingsBuilder.build()
        );
    }
}
