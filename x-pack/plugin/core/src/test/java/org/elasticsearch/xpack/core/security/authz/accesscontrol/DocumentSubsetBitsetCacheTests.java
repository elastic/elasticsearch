/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.authz.accesscontrol;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BitSet;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.hamcrest.Matchers;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DocumentSubsetBitsetCacheTests extends ESTestCase {

    public void testSameBitSetIsReturnedForIdenticalQuery() throws Exception {
        final DocumentSubsetBitsetCache cache = new DocumentSubsetBitsetCache(Settings.EMPTY);
        runTestOnIndex((shardContext, leafContext) -> {
            final Query query1 = QueryBuilders.termQuery("field-1", "value-1").toQuery(shardContext);
            final BitSet bitSet1 = cache.getBitSet(query1, leafContext);
            assertThat(bitSet1, notNullValue());

            final Query query2 = QueryBuilders.termQuery("field-1", "value-1").toQuery(shardContext);
            final BitSet bitSet2 = cache.getBitSet(query2, leafContext);
            assertThat(bitSet2, notNullValue());

            assertThat(bitSet2, Matchers.sameInstance(bitSet1));
        });
    }

    public void testNullBitSetIsReturnedForNonMatchingQuery() throws Exception {
        final DocumentSubsetBitsetCache cache = new DocumentSubsetBitsetCache(Settings.EMPTY);
        runTestOnIndex((shardContext, leafContext) -> {
            final Query query = QueryBuilders.termQuery("does-not-exist", "any-value").toQuery(shardContext);
            final BitSet bitSet = cache.getBitSet(query, leafContext);
            assertThat(bitSet, nullValue());
        });
    }

    public void testNullEntriesAreNotCountedInMemoryUsage() throws Exception {
        final DocumentSubsetBitsetCache cache = new DocumentSubsetBitsetCache(Settings.EMPTY);
        assertThat(cache.ramBytesUsed(), equalTo(0L));

        runTestOnIndex((shardContext, leafContext) -> {
            for (int i = 1; i <= randomIntBetween(3, 6); i++) {
                final Query query = QueryBuilders.termQuery("dne-" + i, "dne- " + i).toQuery(shardContext);
                final BitSet bitSet = cache.getBitSet(query, leafContext);
                assertThat(bitSet, nullValue());
                assertThat(cache.ramBytesUsed(), equalTo(0L));
                assertThat(cache.entryCount(), equalTo(i));
            }
        });
    }

    public void testCacheRespectsMemoryLimit() throws Exception {
        // This value is based on the internal implementation details of lucene's FixedBitSet
        // If the implementation changes, this can be safely updated to match the new ram usage for a single bitset
        final long expectedBytesPerBitSet = 56;

        // Enough to hold exactly 2 bit-sets in the cache
        final long maxCacheBytes = expectedBytesPerBitSet * 2;
        final Settings settings = Settings.builder()
            .put(DocumentSubsetBitsetCache.CACHE_SIZE_SETTING.getKey(), maxCacheBytes + "b")
            .build();
        final DocumentSubsetBitsetCache cache = new DocumentSubsetBitsetCache(settings);
        assertThat(cache.entryCount(), equalTo(0));
        assertThat(cache.ramBytesUsed(), equalTo(0L));

        runTestOnIndex((shardContext, leafContext) -> {
            Query previousQuery = null;
            BitSet previousBitSet = null;
            for (int i = 1; i <= 5; i++) {
                final TermQueryBuilder queryBuilder = QueryBuilders.termQuery("field-" + i, "value-" + i);
                final Query query = queryBuilder.toQuery(shardContext);
                final BitSet bitSet = cache.getBitSet(query, leafContext);
                assertThat(bitSet, notNullValue());
                assertThat(bitSet.ramBytesUsed(), equalTo(expectedBytesPerBitSet));

                // The first time through we have 1 entry, after that we have 2
                final int expectedCount = i == 1 ? 1 : 2;
                assertThat(cache.entryCount(), equalTo(expectedCount));
                assertThat(cache.ramBytesUsed(), equalTo(expectedCount * expectedBytesPerBitSet));

                // Older queries should get evicted, but the query from last iteration should still be cached
                if (previousQuery != null) {
                    assertThat(cache.getBitSet(previousQuery, leafContext), sameInstance(previousBitSet));
                    assertThat(cache.entryCount(), equalTo(expectedCount));
                    assertThat(cache.ramBytesUsed(), equalTo(expectedCount * expectedBytesPerBitSet));
                }
                previousQuery = query;
                previousBitSet = bitSet;

                assertThat(cache.getBitSet(queryBuilder.toQuery(shardContext), leafContext), sameInstance(bitSet));
                assertThat(cache.entryCount(), equalTo(expectedCount));
                assertThat(cache.ramBytesUsed(), equalTo(expectedCount * expectedBytesPerBitSet));
            }

            assertThat(cache.entryCount(), equalTo(2));
            assertThat(cache.ramBytesUsed(), equalTo(2 * expectedBytesPerBitSet));

            cache.clear("testing");

            assertThat(cache.entryCount(), equalTo(0));
            assertThat(cache.ramBytesUsed(), equalTo(0L));
        });
    }

    public void testCacheRespectsAccessTimeExpiry() throws Exception {
        final Settings settings = Settings.builder()
            .put(DocumentSubsetBitsetCache.CACHE_TTL_SETTING.getKey(), "10ms")
            .build();
        final DocumentSubsetBitsetCache cache = new DocumentSubsetBitsetCache(settings);
        assertThat(cache.entryCount(), equalTo(0));
        assertThat(cache.ramBytesUsed(), equalTo(0L));

        runTestOnIndex((shardContext, leafContext) -> {
            final Query query1 = QueryBuilders.termQuery("field-1", "value-1").toQuery(shardContext);
            final BitSet bitSet1 = cache.getBitSet(query1, leafContext);
            assertThat(bitSet1, notNullValue());

            final Query query2 = QueryBuilders.termQuery("field-2", "value-2").toQuery(shardContext);
            assertBusy(() -> {
                // Force the cache to perform eviction
                final BitSet bitSet2 = cache.getBitSet(query2, leafContext);
                assertThat(bitSet2, notNullValue());

                // Loop until the cache has less than 2 items, which mean that something we evicted
                assertThat(cache.entryCount(), Matchers.lessThan(2));

            }, 100, TimeUnit.MILLISECONDS);

            // Check that the original bitset is no longer in the cache (a new instance is returned)
            assertThat(cache.getBitSet(query1, leafContext), not(sameInstance(bitSet1)));
        });
    }

    public void testCacheIsPerIndex() throws Exception {
        final DocumentSubsetBitsetCache cache = new DocumentSubsetBitsetCache(Settings.EMPTY);
        assertThat(cache.entryCount(), equalTo(0));
        assertThat(cache.ramBytesUsed(), equalTo(0L));

        final int iterations = randomIntBetween(3, 10);
        AtomicInteger counter = new AtomicInteger(0);

        final CheckedBiConsumer<QueryShardContext, LeafReaderContext, Exception> consumer = new CheckedBiConsumer<>() {
            @Override
            public void accept(QueryShardContext shardContext, LeafReaderContext leafContext) throws Exception {
                final int count = counter.incrementAndGet();
                final Query query = QueryBuilders.termQuery("field-1", "value-1").toQuery(shardContext);
                final BitSet bitSet = cache.getBitSet(query, leafContext);

                assertThat(bitSet, notNullValue());
                assertThat(cache.entryCount(), equalTo(count));

                if (count < iterations) {
                    // Need to do this nested, or else the cache will be cleared when the index reader is closed
                    runTestOnIndex(this);
                }
            }
        };
        runTestOnIndex(consumer);
    }

    public void testCacheClearEntriesWhenIndexIsClosed() throws Exception {
        final DocumentSubsetBitsetCache cache = new DocumentSubsetBitsetCache(Settings.EMPTY);
        assertThat(cache.entryCount(), equalTo(0));
        assertThat(cache.ramBytesUsed(), equalTo(0L));

        for (int i = 1; i <= randomIntBetween(2, 5); i++) {
            runTestOnIndex((shardContext, leafContext) -> {
                for (int j = 1; j <= randomIntBetween(2, 10); j++) {
                    final Query query = QueryBuilders.termQuery("field-" + j, "value-1").toQuery(shardContext);
                    final BitSet bitSet = cache.getBitSet(query, leafContext);
                    assertThat(bitSet, notNullValue());
                }
                assertThat(cache.entryCount(), not(equalTo(0)));
                assertThat(cache.ramBytesUsed(), not(equalTo(0L)));
            });
            assertThat(cache.entryCount(), equalTo(0));
            assertThat(cache.ramBytesUsed(), equalTo(0L));
        }
    }

    private void runTestOnIndex(CheckedBiConsumer<QueryShardContext, LeafReaderContext, Exception> body) throws Exception {
        final ShardId shardId = new ShardId("idx_" + randomAlphaOfLengthBetween(2, 8), randomAlphaOfLength(12), 0);
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(shardId.getIndex(), Settings.EMPTY);
        final MapperService mapperService = mock(MapperService.class);
        final long nowInMillis = randomNonNegativeLong();

        final Client client = mock(Client.class);
        when(client.settings()).thenReturn(Settings.EMPTY);

        final IndexWriterConfig writerConfig = new IndexWriterConfig(new StandardAnalyzer()).setMergePolicy(NoMergePolicy.INSTANCE);
        try (Directory directory = newDirectory();
             IndexWriter iw = new IndexWriter(directory, writerConfig)) {
            for (int i = 1; i <= 100; i++) {
                Document document = new Document();
                for (int j = 1; j <= 10; j++) {
                    document.add(new StringField("field-" + j, "value-" + i, Field.Store.NO));
                }
                iw.addDocument(document);
            }
            iw.commit();

            try (DirectoryReader directoryReader = DirectoryReader.open(directory)) {
                final LeafReaderContext leaf = directoryReader.leaves().get(0);

                final QueryShardContext context = new QueryShardContext(shardId.id(), indexSettings, null, null, null, mapperService,
                    null, null, xContentRegistry(), writableRegistry(), client, leaf.reader(), () -> nowInMillis, null);

                body.accept(context, leaf);
            }
        }
    }

}
