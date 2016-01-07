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

package org.elasticsearch.index.cache.bitset;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.join.BitDocIdSetFilter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

/**
 */
public class BitSetFilterCacheTest extends ESTestCase {

    @Test
    public void testInvalidateEntries() throws Exception {
        IndexWriter writer = new IndexWriter(
                new RAMDirectory(),
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

        IndexReader reader = DirectoryReader.open(writer, false);
        IndexSearcher searcher = new IndexSearcher(reader);

        BitsetFilterCache cache = new BitsetFilterCache(new Index("test"), Settings.EMPTY);
        BitDocIdSetFilter filter = cache.getBitDocIdSetFilter(new QueryWrapperFilter(new TermQuery(new Term("field", "value"))));
        TopDocs docs = searcher.search(new ConstantScoreQuery(filter), 1);
        assertThat(docs.totalHits, equalTo(3));

        // now cached
        docs = searcher.search(new ConstantScoreQuery(filter), 1);
        assertThat(docs.totalHits, equalTo(3));
        // There are 3 segments
        assertThat(cache.getLoadedFilters().size(), equalTo(3l));

        writer.forceMerge(1);
        reader.close();
        reader = DirectoryReader.open(writer, false);
        searcher = new IndexSearcher(reader);

        docs = searcher.search(new ConstantScoreQuery(filter), 1);
        assertThat(docs.totalHits, equalTo(3));

        // now cached
        docs = searcher.search(new ConstantScoreQuery(filter), 1);
        assertThat(docs.totalHits, equalTo(3));
        // Only one segment now, so the size must be 1
        assertThat(cache.getLoadedFilters().size(), equalTo(1l));

        reader.close();
        writer.close();
        // There is no reference from readers and writer to any segment in the test index, so the size in the fbs cache must be 0
        assertThat(cache.getLoadedFilters().size(), equalTo(0l));
    }

    public void testRejectOtherIndex() throws IOException {
        BitsetFilterCache cache = new BitsetFilterCache(new Index("test"), Settings.EMPTY);
        
        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(
                dir,
                newIndexWriterConfig()
        );
        writer.addDocument(new Document());
        DirectoryReader reader = DirectoryReader.open(writer, true);
        writer.close();
        reader = ElasticsearchDirectoryReader.wrap(reader, new ShardId(new Index("test2"), 0));
        
        BitDocIdSetFilter producer = cache.getBitDocIdSetFilter(new QueryWrapperFilter(new MatchAllDocsQuery()));
        
        try {
            producer.getDocIdSet(reader.leaves().get(0));
            fail();
        } catch (IllegalStateException expected) {
            assertEquals("Trying to load bit set for index [test2] with cache of index [test]", expected.getMessage());
        } finally {
            IOUtils.close(reader, dir);
        }
    }
}
