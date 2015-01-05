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

package org.elasticsearch.index.cache.filter.weighted;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.TermFilter;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilterCachingPolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.cache.filter.FilterCache;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.cache.filter.IndicesFilterCache;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;

import static org.hamcrest.Matchers.greaterThan;

public class WeightedFilterCacheTests extends ElasticsearchSingleNodeTest {

    public void testGeneratedFilterHonorsEqualsHashCode() {
        IndexService index = createIndex("test");
        FilterCache cache = index.cache().filter();
        assertTrue(cache instanceof WeightedFilterCache);
        final Filter filter = new TermFilter(new Term("foo", "bar"));
        assertEquals(
                cache.doCache(filter, FilterCachingPolicy.ALWAYS_CACHE),
                cache.doCache(filter, FilterCachingPolicy.ALWAYS_CACHE));
    }

    public void testShardStats() throws Exception {
        IndexService index = createIndex("test");
        ShardId shard = index.shard(0).shardId();
        FilterCache cache = index.cache().filter();
        IndicesFilterCache indicesCache = getInstanceFromNode(IndicesFilterCache.class);
        assertEquals(0, indicesCache.getStats(shard).getMemorySizeInBytes());

        Directory dir = new RAMDirectory();
        IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(new KeywordAnalyzer()));
        Document doc = new Document();
        doc.add(new StringField("foo", "bar", Store.NO));
        w.addDocument(doc);
        w.commit();
        w.close();
        DirectoryReader reader = DirectoryReader.open(dir);
        reader = ElasticsearchDirectoryReader.wrap(reader, shard);
        IndexSearcher searcher = new IndexSearcher(reader);
        Filter filter = new TermFilter(new Term("foo", "bar"));
        Filter cached = cache.doCache(filter, FilterCachingPolicy.ALWAYS_CACHE);
        searcher.search(new ConstantScoreQuery(cached), 1);
        assertThat(indicesCache.getStats(shard).getMemorySizeInBytes(), greaterThan(0L));

        cache.clear("none");
        assertEquals(0, indicesCache.getStats(shard).getMemorySizeInBytes());
        searcher.search(new ConstantScoreQuery(cached), 1);
        assertThat(indicesCache.getStats(shard).getMemorySizeInBytes(), greaterThan(0L));

        reader.close();

        assertEquals(0, indicesCache.getStats(shard).getMemorySizeInBytes());

        dir.close();
    }

    public void testActuallyCaches() throws Exception {
        IndexService index = createIndex("test");
        ShardId shard = index.shard(0).shardId();
        FilterCache cache = index.cache().filter();

        Directory dir = new RAMDirectory();
        IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(new KeywordAnalyzer()));
        Document doc = new Document();
        doc.add(new StringField("foo", "bar", Store.NO));
        w.addDocument(doc);
        w.commit();
        w.close();
        DirectoryReader reader = DirectoryReader.open(dir);
        reader = ElasticsearchDirectoryReader.wrap(reader, shard);

        Filter filter = new TermFilter(new Term("foo", "bar"));
        Filter cached = cache.doCache(filter, FilterCachingPolicy.ALWAYS_CACHE);
        final DocIdSet set = cached.getDocIdSet(reader.leaves().get(0), null);
        assertTrue(set.isCacheable());
        assertSame(set, cache.doCache(filter, FilterCachingPolicy.ALWAYS_CACHE).getDocIdSet(reader.leaves().get(0), null));

        reader.close();
        dir.close();
    }

}
