/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.cache.filter;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.*;
import org.apache.lucene.queries.TermFilter;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.XConstantScoreQuery;
import org.elasticsearch.common.lucene.search.XFilteredQuery;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.cache.filter.none.NoneFilterCache;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.IOException;

import static org.elasticsearch.common.settings.ImmutableSettings.Builder.EMPTY_SETTINGS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class FilterCacheTests extends ElasticsearchTestCase {


    @Test
    public void testNoCache() throws Exception {
        verifyCache(new NoneFilterCache(new Index("test"), EMPTY_SETTINGS));
    }

    private void verifyCache(FilterCache filterCache) throws Exception {
        Directory dir = new RAMDirectory();
        IndexWriter indexWriter = new IndexWriter(dir, new IndexWriterConfig(Lucene.VERSION, Lucene.STANDARD_ANALYZER));
        DirectoryReader reader = DirectoryReader.open(indexWriter, true);

        for (int i = 0; i < 100; i++) {
            Document document = new Document();
            document.add(new TextField("id", Integer.toString(i), Field.Store.YES));
            indexWriter.addDocument(document);
        }

        reader = refreshReader(reader);
        IndexSearcher searcher = new IndexSearcher(reader);
        assertThat(Lucene.count(searcher, new ConstantScoreQuery(filterCache.cache(new TermFilter(new Term("id", "1"))))), equalTo(1l));
        assertThat(Lucene.count(searcher, new XFilteredQuery(new MatchAllDocsQuery(), filterCache.cache(new TermFilter(new Term("id", "1"))))), equalTo(1l));

        indexWriter.deleteDocuments(new Term("id", "1"));
        reader = refreshReader(reader);
        searcher = new IndexSearcher(reader);
        TermFilter filter = new TermFilter(new Term("id", "1"));
        Filter cachedFilter = filterCache.cache(filter);
        long constantScoreCount = filter == cachedFilter ? 0 : 1;
        // sadly, when caching based on cacheKey with NRT, this fails, that's why we have DeletionAware one
        assertThat(Lucene.count(searcher, new ConstantScoreQuery(cachedFilter)), equalTo(constantScoreCount));
        assertThat(Lucene.count(searcher, new XConstantScoreQuery(cachedFilter)), equalTo(0l));
        assertThat(Lucene.count(searcher, new XFilteredQuery(new MatchAllDocsQuery(), cachedFilter)), equalTo(0l));

        indexWriter.close();
    }

    private DirectoryReader refreshReader(DirectoryReader reader) throws IOException {
        IndexReader oldReader = reader;
        reader = DirectoryReader.openIfChanged(reader);
        if (reader != oldReader) {
            oldReader.close();
        }
        return reader;
    }
}
