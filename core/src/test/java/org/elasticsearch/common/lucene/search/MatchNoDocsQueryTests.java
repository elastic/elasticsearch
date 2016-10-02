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
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.RAMDirectory;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class MatchNoDocsQueryTests extends ESTestCase {
    public void testSimple() throws Exception {
        MatchNoDocsQuery query = new MatchNoDocsQuery("field 'title' not found");
        assertThat(query.toString(), equalTo("MatchNoDocsQuery[\"field 'title' not found\"]"));
        Query rewrite = query.rewrite(null);
        assertTrue(rewrite instanceof MatchNoDocsQuery);
        assertThat(rewrite.toString(), equalTo("MatchNoDocsQuery[\"field 'title' not found\"]"));
    }

    public void testSearch() throws Exception {
        IndexWriter writer = new IndexWriter(new RAMDirectory(), new IndexWriterConfig(Lucene.STANDARD_ANALYZER));
        Document doc = new Document();
        doc.add(new Field("field", "aaa bbb ccc ddd", TextField.TYPE_NOT_STORED));
        writer.addDocument(doc);
        IndexReader reader = DirectoryReader.open(writer);
        IndexSearcher searcher = new IndexSearcher(reader);

        Query query = new MatchNoDocsQuery("field not found");
        assertThat(searcher.count(query), equalTo(0));

        BooleanQuery.Builder bq = new BooleanQuery.Builder();
        bq.add(new BooleanClause(new TermQuery(new Term("field", "aaa")), BooleanClause.Occur.SHOULD));
        bq.add(new BooleanClause(new MatchNoDocsQuery("field not found"), BooleanClause.Occur.MUST));
        query = bq.build();
        assertThat(searcher.count(query), equalTo(0));
        assertThat(query.toString(), equalTo("field:aaa +MatchNoDocsQuery[\"field not found\"]"));


        bq = new BooleanQuery.Builder();
        bq.add(new BooleanClause(new TermQuery(new Term("field", "aaa")), BooleanClause.Occur.SHOULD));
        bq.add(new BooleanClause(new MatchNoDocsQuery("field not found"), BooleanClause.Occur.SHOULD));
        query = bq.build();
        assertThat(query.toString(), equalTo("field:aaa MatchNoDocsQuery[\"field not found\"]"));
        assertThat(searcher.count(query), equalTo(1));
        Query rewrite = query.rewrite(reader);
        assertThat(rewrite.toString(), equalTo("field:aaa MatchNoDocsQuery[\"field not found\"]"));
    }
}
