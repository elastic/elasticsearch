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

package org.elasticsearch.deps.lucene;

import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.util.lucene.Lucene;
import org.testng.annotations.Test;

import java.util.ArrayList;

import static org.elasticsearch.util.lucene.DocumentBuilder.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy
 */
public class SimpleLuceneTests {

    @Test public void testSimpleNumericOps() throws Exception {
        Directory dir = new RAMDirectory();
        IndexWriter indexWriter = new IndexWriter(dir, Lucene.STANDARD_ANALYZER, true, IndexWriter.MaxFieldLength.UNLIMITED);

        indexWriter.addDocument(doc().add(field("_id", "1")).add(new NumericField("test", Field.Store.YES, true).setIntValue(2)).build());

        IndexSearcher searcher = new IndexSearcher(indexWriter.getReader());
        TopDocs topDocs = searcher.search(new TermQuery(new Term("_id", "1")), 1);
        Document doc = searcher.doc(topDocs.scoreDocs[0].doc);
        Fieldable f = doc.getFieldable("test");
        assertThat(f.stringValue(), equalTo("2"));

        topDocs = searcher.search(new TermQuery(new Term("test", NumericUtils.intToPrefixCoded(2))), 1);
        doc = searcher.doc(topDocs.scoreDocs[0].doc);
        f = doc.getFieldable("test");
        assertThat(f.stringValue(), equalTo("2"));

        indexWriter.close();
    }

    /**
     * Here, we verify that the order that we add fields to a document counts, and not the lexi order
     * of the field. This means that heavily accessed fields that use field selector should be added
     * first (with load and break).
     */
    @Test public void testOrdering() throws Exception {
        Directory dir = new RAMDirectory();
        IndexWriter indexWriter = new IndexWriter(dir, Lucene.STANDARD_ANALYZER, true, IndexWriter.MaxFieldLength.UNLIMITED);

        indexWriter.addDocument(doc()
                .add(field("_id", "1"))
                .add(field("#id", "1")).build());

        IndexSearcher searcher = new IndexSearcher(indexWriter.getReader());
        TopDocs topDocs = searcher.search(new TermQuery(new Term("_id", "1")), 1);
        final ArrayList<String> fieldsOrder = new ArrayList<String>();
        Document doc = searcher.doc(topDocs.scoreDocs[0].doc, new FieldSelector() {
            @Override public FieldSelectorResult accept(String fieldName) {
                fieldsOrder.add(fieldName);
                return FieldSelectorResult.LOAD;
            }
        });

        assertThat(fieldsOrder.size(), equalTo(2));
        assertThat(fieldsOrder.get(0), equalTo("_id"));
        assertThat(fieldsOrder.get(1), equalTo("#id"));

        indexWriter.close();
    }

    @Test public void testBoost() throws Exception {
        Directory dir = new RAMDirectory();
        IndexWriter indexWriter = new IndexWriter(dir, Lucene.STANDARD_ANALYZER, true, IndexWriter.MaxFieldLength.UNLIMITED);

        for (int i = 0; i < 100; i++) {
            // TODO (just setting the boost value does not seem to work...)
            StringBuilder value = new StringBuilder().append("value");
            for (int j = 0; j < i; j++) {
                value.append(" ").append("value");
            }
            indexWriter.addDocument(doc()
                    .add(field("id", Integer.toString(i)))
                    .add(field("value", value.toString()))
                    .boost(i).build());
        }

        IndexSearcher searcher = new IndexSearcher(indexWriter.getReader());
        TermQuery query = new TermQuery(new Term("value", "value"));
        TopDocs topDocs = searcher.search(query, 100);
        assertThat(100, equalTo(topDocs.totalHits));
        for (int i = 0; i < topDocs.scoreDocs.length; i++) {
            Document doc = searcher.doc(topDocs.scoreDocs[i].doc);
//            System.out.println(doc.get("id") + ": " + searcher.explain(query, topDocs.scoreDocs[i].doc));
            assertThat(doc.get("id"), equalTo(Integer.toString(100 - i - 1)));
        }

        indexWriter.close();
    }
}
