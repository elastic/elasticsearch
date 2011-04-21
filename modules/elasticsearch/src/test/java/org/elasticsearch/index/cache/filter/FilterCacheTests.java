/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.TermFilter;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.cache.filter.none.NoneFilterCache;
import org.elasticsearch.index.cache.filter.soft.SoftFilterCache;
import org.elasticsearch.index.cache.filter.weak.WeakFilterCache;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.elasticsearch.common.lucene.DocumentBuilder.*;
import static org.elasticsearch.common.settings.ImmutableSettings.Builder.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (shay.banon)
 */
public class FilterCacheTests {


    @Test public void testNoCache() throws Exception {
        verifyCache(new NoneFilterCache(new Index("test"), EMPTY_SETTINGS));
    }

    @Test public void testSoftCache() throws Exception {
        verifyCache(new SoftFilterCache(new Index("test"), EMPTY_SETTINGS, new IndexSettingsService(new Index("test"), EMPTY_SETTINGS)));
    }

    @Test public void testWeakCache() throws Exception {
        verifyCache(new WeakFilterCache(new Index("test"), EMPTY_SETTINGS, new IndexSettingsService(new Index("test"), EMPTY_SETTINGS)));
    }

    private void verifyCache(FilterCache filterCache) throws Exception {
        Directory dir = new RAMDirectory();
        IndexWriter indexWriter = new IndexWriter(dir, Lucene.STANDARD_ANALYZER, true, IndexWriter.MaxFieldLength.UNLIMITED);
        IndexReader reader = indexWriter.getReader();

        for (int i = 0; i < 100; i++) {
            indexWriter.addDocument(doc()
                    .add(field("id", Integer.toString(i)))
                    .boost(i).build());
        }

        reader = refreshReader(reader);
        IndexSearcher searcher = new IndexSearcher(reader);
        assertThat(Lucene.count(searcher, new ConstantScoreQuery(filterCache.cache(new TermFilter(new Term("id", "1")))), -1), equalTo(1l));
        assertThat(Lucene.count(searcher, new FilteredQuery(new MatchAllDocsQuery(), filterCache.cache(new TermFilter(new Term("id", "1")))), -1), equalTo(1l));

        indexWriter.deleteDocuments(new Term("id", "1"));
        reader = refreshReader(reader);
        searcher = new IndexSearcher(reader);
        TermFilter filter = new TermFilter(new Term("id", "1"));
        Filter cachedFilter = filterCache.cache(filter);
        long constantScoreCount = filter == cachedFilter ? 0 : 1;
        // sadly, when caching based on cacheKey with NRT, this fails, that's why we have DeletionAware one
        assertThat(Lucene.count(searcher, new ConstantScoreQuery(cachedFilter), -1), equalTo(constantScoreCount));
        assertThat(Lucene.count(searcher, new DeletionAwareConstantScoreQuery(cachedFilter), -1), equalTo(0l));
        assertThat(Lucene.count(searcher, new FilteredQuery(new MatchAllDocsQuery(), cachedFilter), -1), equalTo(0l));

        indexWriter.close();
    }

    private IndexReader refreshReader(IndexReader reader) throws IOException {
        IndexReader oldReader = reader;
        reader = reader.reopen();
        if (reader != oldReader) {
            oldReader.close();
        }
        return reader;
    }
}
