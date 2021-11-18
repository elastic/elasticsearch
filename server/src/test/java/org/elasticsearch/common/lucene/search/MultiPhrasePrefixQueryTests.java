/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.lucene.search;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class MultiPhrasePrefixQueryTests extends ESTestCase {
    public void testSimple() throws Exception {
        IndexWriter writer = new IndexWriter(new ByteBuffersDirectory(), new IndexWriterConfig(Lucene.STANDARD_ANALYZER));
        Document doc = new Document();
        doc.add(new Field("field", "aaa bbb ccc ddd", TextField.TYPE_NOT_STORED));
        writer.addDocument(doc);
        IndexReader reader = DirectoryReader.open(writer);
        IndexSearcher searcher = new IndexSearcher(reader);

        MultiPhrasePrefixQuery query = new MultiPhrasePrefixQuery("field");
        query.add(new Term("field", "aa"));
        assertThat(searcher.count(query), equalTo(1));

        query = new MultiPhrasePrefixQuery("field");
        query.add(new Term("field", "aaa"));
        query.add(new Term("field", "bb"));
        assertThat(searcher.count(query), equalTo(1));

        query = new MultiPhrasePrefixQuery("field");
        query.setSlop(1);
        query.add(new Term("field", "aaa"));
        query.add(new Term("field", "cc"));
        assertThat(searcher.count(query), equalTo(1));

        query = new MultiPhrasePrefixQuery("field");
        query.setSlop(1);
        query.add(new Term("field", "xxx"));
        assertThat(searcher.count(query), equalTo(0));
    }
}
