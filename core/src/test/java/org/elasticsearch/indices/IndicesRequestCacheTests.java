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

package org.elasticsearch.indices;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.cache.request.ShardRequestCache;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

public class IndicesRequestCacheTests extends ESTestCase {

    public void testBasicOperationsCache() throws Exception {
        ShardRequestCache requestCacheStats = new ShardRequestCache();
        IndicesRequestCache cache = new IndicesRequestCache(Settings.EMPTY);
        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());

        writer.addDocument(newDoc(0, "foo"));
        DirectoryReader reader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(writer),
            new ShardId("foo", "bar", 1));
        TermQueryBuilder termQuery = new TermQueryBuilder("id", "0");
        AtomicBoolean indexShard = new AtomicBoolean(true);

        // initial cache
        TestEntity entity = new TestEntity(requestCacheStats, reader, indexShard, 0);
        BytesReference value = cache.getOrCompute(entity, reader, termQuery.buildAsBytes());
        assertEquals("foo", StreamInput.wrap(value).readString());
        assertEquals(0, requestCacheStats.stats().getHitCount());
        assertEquals(1, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertFalse(entity.loadedFromCache());
        assertEquals(1, cache.count());

        // cache hit
        entity = new TestEntity(requestCacheStats, reader, indexShard, 0);
        value = cache.getOrCompute(entity, reader, termQuery.buildAsBytes());
        assertEquals("foo", StreamInput.wrap(value).readString());
        assertEquals(1, requestCacheStats.stats().getHitCount());
        assertEquals(1, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertTrue(entity.loadedFromCache());
        assertEquals(1, cache.count());
        assertTrue(requestCacheStats.stats().getMemorySize().bytesAsInt() > value.length());
        assertEquals(1, cache.numRegisteredCloseListeners());

        // Closing the cache doesn't modify an already returned CacheEntity
        if (randomBoolean()) {
            reader.close();
        } else {
            indexShard.set(false); // closed shard but reader is still open
            cache.clear(entity);
        }
        cache.cleanCache();
        assertEquals(1, requestCacheStats.stats().getHitCount());
        assertEquals(1, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertTrue(entity.loadedFromCache());
        assertEquals(0, cache.count());
        assertEquals(0, requestCacheStats.stats().getMemorySize().bytesAsInt());

        IOUtils.close(reader, writer, dir, cache);
        assertEquals(0, cache.numRegisteredCloseListeners());
    }

    public void testCacheDifferentReaders() throws Exception {
        IndicesRequestCache cache = new IndicesRequestCache(Settings.EMPTY);
        AtomicBoolean indexShard =  new AtomicBoolean(true);
        ShardRequestCache requestCacheStats = new ShardRequestCache();
        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());

        writer.addDocument(newDoc(0, "foo"));
        DirectoryReader reader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));
        TermQueryBuilder termQuery = new TermQueryBuilder("id", "0");

        writer.updateDocument(new Term("id", "0"), newDoc(0, "bar"));
        DirectoryReader secondReader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(writer), new ShardId("foo", "bar", 1));

        // initial cache
        TestEntity entity = new TestEntity(requestCacheStats, reader, indexShard, 0);
        BytesReference value = cache.getOrCompute(entity, reader, termQuery.buildAsBytes());
        assertEquals("foo", StreamInput.wrap(value).readString());
        assertEquals(0, requestCacheStats.stats().getHitCount());
        assertEquals(1, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertFalse(entity.loadedFromCache());
        assertEquals(1, cache.count());
        assertTrue(requestCacheStats.stats().getMemorySize().bytesAsInt() > value.length());
        final int cacheSize = requestCacheStats.stats().getMemorySize().bytesAsInt();
        assertEquals(1, cache.numRegisteredCloseListeners());

        // cache the second
        TestEntity secondEntity = new TestEntity(requestCacheStats, secondReader, indexShard, 0);
        value = cache.getOrCompute(secondEntity, secondReader, termQuery.buildAsBytes());
        assertEquals("bar", StreamInput.wrap(value).readString());
        assertEquals(0, requestCacheStats.stats().getHitCount());
        assertEquals(2, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertFalse(secondEntity.loadedFromCache());
        assertEquals(2, cache.count());
        assertTrue(requestCacheStats.stats().getMemorySize().bytesAsInt() > cacheSize + value.length());
        assertEquals(2, cache.numRegisteredCloseListeners());

        secondEntity = new TestEntity(requestCacheStats, secondReader, indexShard, 0);
        value = cache.getOrCompute(secondEntity, secondReader, termQuery.buildAsBytes());
        assertEquals("bar", StreamInput.wrap(value).readString());
        assertEquals(1, requestCacheStats.stats().getHitCount());
        assertEquals(2, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertTrue(secondEntity.loadedFromCache());
        assertEquals(2, cache.count());

        entity = new TestEntity(requestCacheStats, reader, indexShard, 0);
        value = cache.getOrCompute(entity, reader, termQuery.buildAsBytes());
        assertEquals("foo", StreamInput.wrap(value).readString());
        assertEquals(2, requestCacheStats.stats().getHitCount());
        assertEquals(2, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertTrue(entity.loadedFromCache());
        assertEquals(2, cache.count());

        // Closing the cache doesn't change returned entities
        reader.close();
        cache.cleanCache();
        assertEquals(2, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertTrue(entity.loadedFromCache());
        assertTrue(secondEntity.loadedFromCache());
        assertEquals(1, cache.count());
        assertEquals(cacheSize, requestCacheStats.stats().getMemorySize().bytesAsInt());
        assertEquals(1, cache.numRegisteredCloseListeners());


        // release
        if (randomBoolean()) {
            secondReader.close();
        } else {
            indexShard.set(false); // closed shard but reader is still open
            cache.clear(secondEntity);
        }
        cache.cleanCache();
        assertEquals(2, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertTrue(entity.loadedFromCache());
        assertTrue(secondEntity.loadedFromCache());
        assertEquals(0, cache.count());
        assertEquals(0, requestCacheStats.stats().getMemorySize().bytesAsInt());

        IOUtils.close(secondReader, writer, dir, cache);
        assertEquals(0, cache.numRegisteredCloseListeners());
    }

    public void testEviction() throws Exception {
        final ByteSizeValue size;
        {
            IndicesRequestCache cache = new IndicesRequestCache(Settings.EMPTY);
            AtomicBoolean indexShard = new AtomicBoolean(true);
            ShardRequestCache requestCacheStats = new ShardRequestCache();
            Directory dir = newDirectory();
            IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());

            writer.addDocument(newDoc(0, "foo"));
            DirectoryReader reader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(writer),
                new ShardId("foo", "bar", 1));
            TermQueryBuilder termQuery = new TermQueryBuilder("id", "0");
            TestEntity entity = new TestEntity(requestCacheStats, reader, indexShard, 0);

            writer.updateDocument(new Term("id", "0"), newDoc(0, "bar"));
            DirectoryReader secondReader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(writer),
                new ShardId("foo", "bar", 1));
            TestEntity secondEntity = new TestEntity(requestCacheStats, secondReader, indexShard, 0);

            BytesReference value1 = cache.getOrCompute(entity, reader, termQuery.buildAsBytes());
            assertEquals("foo", StreamInput.wrap(value1).readString());
            BytesReference value2 = cache.getOrCompute(secondEntity, secondReader, termQuery.buildAsBytes());
            assertEquals("bar", StreamInput.wrap(value2).readString());
            size = requestCacheStats.stats().getMemorySize();
            IOUtils.close(reader, secondReader, writer, dir, cache);
        }
        IndicesRequestCache cache = new IndicesRequestCache(Settings.builder()
            .put(IndicesRequestCache.INDICES_CACHE_QUERY_SIZE.getKey(), size.bytes()+1 +"b")
            .build());
        AtomicBoolean indexShard = new AtomicBoolean(true);
        ShardRequestCache requestCacheStats = new ShardRequestCache();
        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());

        writer.addDocument(newDoc(0, "foo"));
        DirectoryReader reader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(writer),
            new ShardId("foo", "bar", 1));
        TermQueryBuilder termQuery = new TermQueryBuilder("id", "0");
        TestEntity entity = new TestEntity(requestCacheStats, reader, indexShard, 0);

        writer.updateDocument(new Term("id", "0"), newDoc(0, "bar"));
        DirectoryReader secondReader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(writer),
            new ShardId("foo", "bar", 1));
        TestEntity secondEntity = new TestEntity(requestCacheStats, secondReader, indexShard, 0);

        writer.updateDocument(new Term("id", "0"), newDoc(0, "baz"));
        DirectoryReader thirdReader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(writer),
            new ShardId("foo", "bar", 1));
        TestEntity thirddEntity = new TestEntity(requestCacheStats, thirdReader, indexShard, 0);

        BytesReference value1 = cache.getOrCompute(entity, reader, termQuery.buildAsBytes());
        assertEquals("foo", StreamInput.wrap(value1).readString());
        BytesReference value2 = cache.getOrCompute(secondEntity, secondReader, termQuery.buildAsBytes());
        assertEquals("bar", StreamInput.wrap(value2).readString());
        logger.info("Memory size: {}", requestCacheStats.stats().getMemorySize());
        BytesReference value3 = cache.getOrCompute(thirddEntity, thirdReader, termQuery.buildAsBytes());
        assertEquals("baz", StreamInput.wrap(value3).readString());
        assertEquals(2, cache.count());
        assertEquals(1, requestCacheStats.stats().getEvictions());
        IOUtils.close(reader, secondReader, thirdReader, writer, dir, cache);
    }

    public void testClearAllEntityIdentity() throws Exception {
        IndicesRequestCache cache = new IndicesRequestCache(Settings.EMPTY);
        AtomicBoolean indexShard =  new AtomicBoolean(true);

        ShardRequestCache requestCacheStats = new ShardRequestCache();
        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());

        writer.addDocument(newDoc(0, "foo"));
        DirectoryReader reader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(writer),
            new ShardId("foo", "bar", 1));
        TermQueryBuilder termQuery = new TermQueryBuilder("id", "0");
        TestEntity entity = new TestEntity(requestCacheStats, reader, indexShard, 0);

        writer.updateDocument(new Term("id", "0"), newDoc(0, "bar"));
        DirectoryReader secondReader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(writer),
            new ShardId("foo", "bar", 1));
        TestEntity secondEntity = new TestEntity(requestCacheStats, secondReader, indexShard, 0);

        writer.updateDocument(new Term("id", "0"), newDoc(0, "baz"));
        DirectoryReader thirdReader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(writer),
            new ShardId("foo", "bar", 1));
        AtomicBoolean differentIdentity =  new AtomicBoolean(true);
        TestEntity thirddEntity = new TestEntity(requestCacheStats, thirdReader, differentIdentity, 0);

        BytesReference value1 = cache.getOrCompute(entity, reader, termQuery.buildAsBytes());
        assertEquals("foo", StreamInput.wrap(value1).readString());
        BytesReference value2 = cache.getOrCompute(secondEntity, secondReader, termQuery.buildAsBytes());
        assertEquals("bar", StreamInput.wrap(value2).readString());
        logger.info("Memory size: {}", requestCacheStats.stats().getMemorySize());
        BytesReference value3 = cache.getOrCompute(thirddEntity, thirdReader, termQuery.buildAsBytes());
        assertEquals("baz", StreamInput.wrap(value3).readString());
        assertEquals(3, cache.count());
        final long hitCount = requestCacheStats.stats().getHitCount();
        // clear all for the indexShard Idendity even though is't still open
        cache.clear(randomFrom(entity, secondEntity));
        cache.cleanCache();
        assertEquals(1, cache.count());
        // third has not been validated since it's a different identity
        value3 = cache.getOrCompute(thirddEntity, thirdReader, termQuery.buildAsBytes());
        assertEquals(hitCount + 1, requestCacheStats.stats().getHitCount());
        assertEquals("baz", StreamInput.wrap(value3).readString());


        IOUtils.close(reader, secondReader, thirdReader, writer, dir, cache);

    }

    public Iterable<Field> newDoc(int id, String value) {
        return Arrays.asList(newField("id", Integer.toString(id), StringField.TYPE_STORED), newField("value", value,
            StringField.TYPE_STORED));
    }

    private class TestEntity extends AbstractIndexShardCacheEntity {
        private final AtomicBoolean standInForIndexShard;
        private final ShardRequestCache shardRequestCache;
        private TestEntity(ShardRequestCache shardRequestCache, DirectoryReader reader, AtomicBoolean standInForIndexShard, int id) {
            super(new Loader() {
                @Override
                public void load(StreamOutput out) throws IOException {
                    IndexSearcher searcher = new IndexSearcher(reader);
                    TopDocs topDocs = searcher.search(new TermQuery(new Term("id", Integer.toString(id))), 1);
                    assertEquals(1, topDocs.totalHits);
                    Document document = reader.document(topDocs.scoreDocs[0].doc);
                    out.writeString(document.get("value"));
                }
            });
            this.standInForIndexShard = standInForIndexShard;
            this.shardRequestCache = shardRequestCache;
        }

        @Override
        protected ShardRequestCache stats() {
            return shardRequestCache;
        }

        @Override
        public boolean isOpen() {
           return standInForIndexShard.get();
        }

        @Override
        public Object getCacheIdentity() {
            return standInForIndexShard;
        }
    }
}
