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

package org.elasticsearch.common.lucene.search;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class MatchAllDocsFilterTests extends ElasticsearchTestCase {

    @Test
    public void testMatchAllDocsFilter() throws Exception {
        Directory dir = new RAMDirectory();
        IndexWriter indexWriter = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER));

        Document document = new Document();
        document.add(new TextField("_id", "1", Field.Store.YES));
        document.add(new TextField("text", "lucene", Field.Store.YES));
        indexWriter.addDocument(document);

        document = new Document();
        document.add(new TextField("_id", "2", Field.Store.YES));
        document.add(new TextField("text", "lucene release", Field.Store.YES));
        indexWriter.addDocument(document);

        IndexReader reader = DirectoryReader.open(indexWriter, true);
        IndexSearcher searcher = new IndexSearcher(reader);

        ConstantScoreQuery query = new ConstantScoreQuery(Queries.newMatchAllFilter());
        long count = Lucene.count(searcher, query);
        assertThat(count, equalTo(2l));

        reader.close();
        indexWriter.close();
    }
}
