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
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.cache.RemovalNotification;
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
        DirectoryReader reader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(writer, true),
            new ShardId("foo", "bar", 1));
        TermQueryBuilder termQuery = new TermQueryBuilder("id", "0");
        AtomicBoolean indexShard = new AtomicBoolean(true);
        TestEntity entity = new TestEntity(requestCacheStats, reader, indexShard, 0);

        // initial cache
        BytesReference value = cache.getOrCompute(entity, reader, termQuery.buildAsBytes());
        assertEquals("foo", value.toUtf8());
        assertEquals(0, requestCacheStats.stats().getHitCount());
        assertEquals(1, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertEquals(1, entity.loaded);
        assertEquals(1, cache.count());

        // cache hit
        value = cache.getOrCompute(entity, reader, termQuery.buildAsBytes());
        assertEquals("foo", value.toUtf8());
        assertEquals(1, requestCacheStats.stats().getHitCount());
        assertEquals(1, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertEquals(1, entity.loaded);
        assertEquals(1, cache.count());
        assertTrue(requestCacheStats.stats().getMemorySize().bytesAsInt() > value.length());
        assertEquals(1, cache.numRegisteredCloseListeners());

        // release
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
        assertEquals(1, entity.loaded);
        assertEquals(0, cache.count());
        assertEquals(0, requestCacheStats.stats().getMemorySize().bytesAsInt());

        IOUtils.close(reader, writer, dir, cache);
        assertEquals(0, cache.numRegisteredCloseListeners());
    }

    public void testCacheWithDifferentEntityInstance() throws Exception {
        IndicesRequestCache cache = new IndicesRequestCache(Settings.EMPTY);
        AtomicBoolean indexShard =  new AtomicBoolean(true);
        ShardRequestCache requestCacheStats = new ShardRequestCache();
        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());

        writer.addDocument(newDoc(0, "foo"));
        DirectoryReader reader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(writer, true),
            new ShardId("foo", "bar", 1));
        TermQueryBuilder termQuery = new TermQueryBuilder("id", "0");
        TestEntity entity = new TestEntity(requestCacheStats, reader, indexShard, 0);

        // initial cache
        BytesReference value = cache.getOrCompute(entity, reader, termQuery.buildAsBytes());
        assertEquals("foo", value.toUtf8());
        assertEquals(0, requestCacheStats.stats().getHitCount());
        assertEquals(1, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertEquals(1, entity.loaded);
        assertEquals(1, cache.count());
        assertEquals(1, cache.numRegisteredCloseListeners());
        final int cacheSize = requestCacheStats.stats().getMemorySize().bytesAsInt();

        value = cache.getOrCompute(new TestEntity(requestCacheStats, reader, indexShard, 0), reader, termQuery.buildAsBytes());
        assertEquals("foo", value.toUtf8());
        assertEquals(1, requestCacheStats.stats().getHitCount());
        assertEquals(1, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertEquals(1, entity.loaded);
        assertEquals(1, cache.count());
        assertEquals(cacheSize, requestCacheStats.stats().getMemorySize().bytesAsInt());

        assertEquals(1, cache.numRegisteredCloseListeners());
        IOUtils.close(reader, writer, dir, cache);
    }

    public void testCacheDifferentReaders() throws Exception {
        IndicesRequestCache cache = new IndicesRequestCache(Settings.EMPTY);
        AtomicBoolean indexShard =  new AtomicBoolean(true);
        ShardRequestCache requestCacheStats = new ShardRequestCache();
        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());

        writer.addDocument(newDoc(0, "foo"));
        DirectoryReader reader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(writer, true), new ShardId("foo", "bar", 1));
        TermQueryBuilder termQuery = new TermQueryBuilder("id", "0");
        TestEntity entity = new TestEntity(requestCacheStats, reader, indexShard, 0);

        writer.updateDocument(new Term("id", "0"), newDoc(0, "bar"));
        DirectoryReader secondReader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(writer, true), new ShardId("foo", "bar", 1));
        TestEntity secondEntity = new TestEntity(requestCacheStats, secondReader, indexShard, 0);

        // initial cache
        BytesReference value = cache.getOrCompute(entity, reader, termQuery.buildAsBytes());
        assertEquals("foo", value.toUtf8());
        assertEquals(0, requestCacheStats.stats().getHitCount());
        assertEquals(1, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertEquals(1, entity.loaded);
        assertEquals(1, cache.count());
        assertTrue(requestCacheStats.stats().getMemorySize().bytesAsInt() > value.length());
        final int cacheSize = requestCacheStats.stats().getMemorySize().bytesAsInt();
        assertEquals(1, cache.numRegisteredCloseListeners());

        // cache the second
        value = cache.getOrCompute(secondEntity, secondReader, termQuery.buildAsBytes());
        assertEquals("bar", value.toUtf8());
        assertEquals(0, requestCacheStats.stats().getHitCount());
        assertEquals(2, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertEquals(1, entity.loaded);
        assertEquals(1, secondEntity.loaded);
        assertEquals(2, cache.count());
        assertTrue(requestCacheStats.stats().getMemorySize().bytesAsInt() > cacheSize + value.length());
        assertEquals(2, cache.numRegisteredCloseListeners());



        value = cache.getOrCompute(secondEntity, secondReader, termQuery.buildAsBytes());
        assertEquals("bar", value.toUtf8());
        assertEquals(1, requestCacheStats.stats().getHitCount());
        assertEquals(2, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertEquals(1, entity.loaded);
        assertEquals(1, secondEntity.loaded);
        assertEquals(2, cache.count());

        value = cache.getOrCompute(entity, reader, termQuery.buildAsBytes());
        assertEquals("foo", value.toUtf8());
        assertEquals(2, requestCacheStats.stats().getHitCount());
        assertEquals(2, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertEquals(1, entity.loaded);
        assertEquals(1, secondEntity.loaded);
        assertEquals(2, cache.count());

        reader.close();
        cache.cleanCache();
        assertEquals(2, requestCacheStats.stats().getMissCount());
        assertEquals(0, requestCacheStats.stats().getEvictions());
        assertEquals(1, entity.loaded);
        assertEquals(1, secondEntity.loaded);
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
        assertEquals(1, entity.loaded);
        assertEquals(1, secondEntity.loaded);
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
            DirectoryReader reader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(writer, true),
                new ShardId("foo", "bar", 1));
            TermQueryBuilder termQuery = new TermQueryBuilder("id", "0");
            TestEntity entity = new TestEntity(requestCacheStats, reader, indexShard, 0);

            writer.updateDocument(new Term("id", "0"), newDoc(0, "bar"));
            DirectoryReader secondReader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(writer, true),
                new ShardId("foo", "bar", 1));
            TestEntity secondEntity = new TestEntity(requestCacheStats, secondReader, indexShard, 0);

            BytesReference value1 = cache.getOrCompute(entity, reader, termQuery.buildAsBytes());
            assertEquals("foo", value1.toUtf8());
            BytesReference value2 = cache.getOrCompute(secondEntity, secondReader, termQuery.buildAsBytes());
            assertEquals("bar", value2.toUtf8());
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
        DirectoryReader reader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(writer, true),
            new ShardId("foo", "bar", 1));
        TermQueryBuilder termQuery = new TermQueryBuilder("id", "0");
        TestEntity entity = new TestEntity(requestCacheStats, reader, indexShard, 0);

        writer.updateDocument(new Term("id", "0"), newDoc(0, "bar"));
        DirectoryReader secondReader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(writer, true),
            new ShardId("foo", "bar", 1));
        TestEntity secondEntity = new TestEntity(requestCacheStats, secondReader, indexShard, 0);

        writer.updateDocument(new Term("id", "0"), newDoc(0, "baz"));
        DirectoryReader thirdReader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(writer, true),
            new ShardId("foo", "bar", 1));
        TestEntity thirddEntity = new TestEntity(requestCacheStats, thirdReader, indexShard, 0);

        BytesReference value1 = cache.getOrCompute(entity, reader, termQuery.buildAsBytes());
        assertEquals("foo", value1.toUtf8());
        BytesReference value2 = cache.getOrCompute(secondEntity, secondReader, termQuery.buildAsBytes());
        assertEquals("bar", value2.toUtf8());
        logger.info(requestCacheStats.stats().getMemorySize().toString());
        BytesReference value3 = cache.getOrCompute(thirddEntity, thirdReader, termQuery.buildAsBytes());
        assertEquals("baz", value3.toUtf8());
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
        DirectoryReader reader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(writer, true),
            new ShardId("foo", "bar", 1));
        TermQueryBuilder termQuery = new TermQueryBuilder("id", "0");
        TestEntity entity = new TestEntity(requestCacheStats, reader, indexShard, 0);

        writer.updateDocument(new Term("id", "0"), newDoc(0, "bar"));
        DirectoryReader secondReader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(writer, true),
            new ShardId("foo", "bar", 1));
        TestEntity secondEntity = new TestEntity(requestCacheStats, secondReader, indexShard, 0);

        writer.updateDocument(new Term("id", "0"), newDoc(0, "baz"));
        DirectoryReader thirdReader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(writer, true),
            new ShardId("foo", "bar", 1));
        AtomicBoolean differentIdentity =  new AtomicBoolean(true);
        TestEntity thirddEntity = new TestEntity(requestCacheStats, thirdReader, differentIdentity, 0);

        BytesReference value1 = cache.getOrCompute(entity, reader, termQuery.buildAsBytes());
        assertEquals("foo", value1.toUtf8());
        BytesReference value2 = cache.getOrCompute(secondEntity, secondReader, termQuery.buildAsBytes());
        assertEquals("bar", value2.toUtf8());
        logger.info(requestCacheStats.stats().getMemorySize().toString());
        BytesReference value3 = cache.getOrCompute(thirddEntity, thirdReader, termQuery.buildAsBytes());
        assertEquals("baz", value3.toUtf8());
        assertEquals(3, cache.count());
        final long hitCount = requestCacheStats.stats().getHitCount();
        // clear all for the indexShard Idendity even though is't still open
        cache.clear(randomFrom(entity, secondEntity));
        cache.cleanCache();
        assertEquals(1, cache.count());
        // third has not been validated since it's a different identity
        value3 = cache.getOrCompute(thirddEntity, thirdReader, termQuery.buildAsBytes());
        assertEquals(hitCount + 1, requestCacheStats.stats().getHitCount());
        assertEquals("baz", value3.toUtf8());


        IOUtils.close(reader, secondReader, thirdReader, writer, dir, cache);

    }

    public Iterable<Field> newDoc(int id, String value) {
        return Arrays.asList(newField("id", Integer.toString(id), StringField.TYPE_STORED), newField("value", value,
            StringField.TYPE_STORED));
    }

    private class TestEntity implements IndicesRequestCache.CacheEntity {
        private final DirectoryReader reader;
        private final int id;
        private final AtomicBoolean identity;
        private final ShardRequestCache shardRequestCache;
        private int loaded;
        private TestEntity(ShardRequestCache shardRequestCache, DirectoryReader reader, AtomicBoolean identity, int id) {
            this.reader = reader;
            this.id = id;
            this.identity = identity;
            this.shardRequestCache = shardRequestCache;
        }

        @Override
        public IndicesRequestCache.Value loadValue() throws IOException {
            IndexSearcher searcher = new IndexSearcher(reader);
            TopDocs topDocs = searcher.search(new TermQuery(new Term("id", Integer.toString(this.id))), 1);
            assertEquals(1, topDocs.totalHits);
            Document document = reader.document(topDocs.scoreDocs[0].doc);
            BytesArray value = new BytesArray(document.get("value"));
            loaded++;
            return new IndicesRequestCache.Value(value, value.length());
        }

        @Override
        public void onCached(IndicesRequestCache.Key key, IndicesRequestCache.Value value) {
            shardRequestCache.onCached(key, value);
        }

        @Override
        public boolean isOpen() {
           return identity.get();
        }

        @Override
        public Object getCacheIdentity() {
            return identity;
        }

        @Override
        public void onHit() {
            shardRequestCache.onHit();
        }

        @Override
        public void onMiss() {
            shardRequestCache.onMiss();
        }

        @Override
        public void onRemoval(RemovalNotification<IndicesRequestCache.Key, IndicesRequestCache.Value> notification) {
            shardRequestCache.onRemoval(notification.getKey(), notification.getValue(),
                notification.getRemovalReason() == RemovalNotification.RemovalReason.EVICTED);
        }
    }
}
