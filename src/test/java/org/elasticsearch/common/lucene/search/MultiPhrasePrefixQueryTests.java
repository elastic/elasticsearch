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
import org.apache.lucene.index.*;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.RAMDirectory;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class MultiPhrasePrefixQueryTests extends ElasticsearchTestCase {

    @Test
    public void simpleTests() throws Exception {
        IndexWriter writer = new IndexWriter(new RAMDirectory(), new IndexWriterConfig(Lucene.VERSION, Lucene.STANDARD_ANALYZER));
        Document doc = new Document();
        doc.add(new Field("field", "aaa bbb ccc ddd", TextField.TYPE_NOT_STORED));
        writer.addDocument(doc);
        IndexReader reader = DirectoryReader.open(writer, true);
        IndexSearcher searcher = new IndexSearcher(reader);

        MultiPhrasePrefixQuery query = new MultiPhrasePrefixQuery();
        query.add(new Term("field", "aa"));
        assertThat(Lucene.count(searcher, query), equalTo(1l));

        query = new MultiPhrasePrefixQuery();
        query.add(new Term("field", "aaa"));
        query.add(new Term("field", "bb"));
        assertThat(Lucene.count(searcher, query), equalTo(1l));

        query = new MultiPhrasePrefixQuery();
        query.setSlop(1);
        query.add(new Term("field", "aaa"));
        query.add(new Term("field", "cc"));
        assertThat(Lucene.count(searcher, query), equalTo(1l));

        query = new MultiPhrasePrefixQuery();
        query.setSlop(1);
        query.add(new Term("field", "xxx"));
        assertThat(Lucene.count(searcher, query), equalTo(0l));
    }
}