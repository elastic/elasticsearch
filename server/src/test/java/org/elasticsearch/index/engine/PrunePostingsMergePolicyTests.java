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

package org.elasticsearch.index.engine;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.ShuffleForcedMergePolicy;
import org.apache.lucene.index.SoftDeletesRetentionMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class PrunePostingsMergePolicyTests extends ESTestCase {

    public void testPrune() throws IOException {
        try (Directory dir = newDirectory()) {
            IndexWriterConfig iwc = newIndexWriterConfig();
            iwc.setSoftDeletesField("_soft_deletes");
            MergePolicy mp = new SoftDeletesRetentionMergePolicy("_soft_deletes", MatchAllDocsQuery::new,
                new PrunePostingsMergePolicy(newLogMergePolicy(), "id"));
            iwc.setMergePolicy(new ShuffleForcedMergePolicy(mp));
            boolean sorted = randomBoolean();
            if (sorted) {
                iwc.setIndexSort(new Sort(new SortField("sort", SortField.Type.INT)));
            }
            int numUniqueDocs = randomIntBetween(1, 100);
            int numDocs = randomIntBetween(numUniqueDocs, numUniqueDocs * 5);

            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                for (int i = 0; i < numDocs ; i++) {
                    if (rarely()) {
                        writer.flush();
                    }
                    if (rarely()) {
                        writer.forceMerge(1, false);
                    }
                    int id = i % numUniqueDocs;
                    Document doc = new Document();
                    doc.add(new StringField("id", "" + id, Field.Store.NO));
                    doc.add(newTextField("text", "the quick brown fox", Field.Store.YES));
                    doc.add(new NumericDocValuesField("sort", i));
                    writer.softUpdateDocument(new Term("id", "" + id), doc, new NumericDocValuesField("_soft_deletes", 1));
                    if (i == 0) {
                        // make sure we have at least 2 segments to ensure we do an actual merge to kick out all postings for
                        // soft deletes
                        writer.flush();
                    }
                }

                writer.forceMerge(1);
                try (DirectoryReader reader = DirectoryReader.open(writer)) {
                    LeafReader leafReader = reader.leaves().get(0).reader();
                    assertEquals(numDocs, leafReader.maxDoc());
                    Terms id = leafReader.terms("id");
                    TermsEnum iterator = id.iterator();
                    for (int i = 0; i < numUniqueDocs; i++) {
                        assertTrue(iterator.seekExact(new BytesRef("" + i)));
                        assertEquals(1, iterator.docFreq());
                    }
                    iterator = leafReader.terms("text").iterator();
                    assertTrue(iterator.seekExact(new BytesRef("quick")));
                    assertEquals(leafReader.maxDoc(), iterator.docFreq());
                    int numValues = 0;
                    NumericDocValues sort = leafReader.getNumericDocValues("sort");
                    while (sort.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                        if (sorted) {
                            assertEquals(sort.docID(), sort.longValue());
                        } else {
                            assertTrue(sort.longValue() >= 0);
                            assertTrue(sort.longValue() < numDocs);
                        }
                        numValues++;
                    }
                    assertEquals(numValues, numDocs);
                }
                {
                    // prune away a single ID
                    Document doc = new Document();
                    doc.add(new StringField("id", "test", Field.Store.NO));
                    writer.deleteDocuments(new Term("id", "test"));
                    writer.flush();
                    writer.forceMerge(1);
                    writer.updateNumericDocValue(new Term("id", "test"), "_soft_deletes", 1);// delete it
                    writer.flush();
                    writer.forceMerge(1);

                    try (DirectoryReader reader = DirectoryReader.open(writer)) {
                        LeafReader leafReader = reader.leaves().get(0).reader();
                        assertEquals(numDocs, leafReader.maxDoc());
                        Terms id = leafReader.terms("id");
                        TermsEnum iterator = id.iterator();
                        assertEquals(numUniqueDocs, id.size());
                        for (int i = 0; i < numUniqueDocs; i++) {
                            assertTrue(iterator.seekExact(new BytesRef("" + i)));
                            assertEquals(1, iterator.docFreq());
                        }
                        assertFalse(iterator.seekExact(new BytesRef("test")));
                        iterator = leafReader.terms("text").iterator();
                        assertTrue(iterator.seekExact(new BytesRef("quick")));
                        assertEquals(leafReader.maxDoc(), iterator.docFreq());
                    }
                }

                { // drop all ids
                    // first add a doc such that we can force merge
                    Document doc = new Document();
                    doc.add(new StringField("id", "" + 0, Field.Store.NO));
                    doc.add(newTextField("text", "the quick brown fox", Field.Store.YES));
                    doc.add(new NumericDocValuesField("sort", 0));
                    writer.softUpdateDocument(new Term("id", "" + 0), doc, new NumericDocValuesField("_soft_deletes", 1));
                    for (int i = 0; i < numUniqueDocs; i++) {
                        writer.updateNumericDocValue(new Term("id", "" + i), "_soft_deletes", 1);
                    }
                    writer.flush();
                    writer.forceMerge(1);


                    try (DirectoryReader reader = DirectoryReader.open(writer)) {
                        LeafReader leafReader = reader.leaves().get(0).reader();
                        assertEquals(numDocs+1, leafReader.maxDoc());
                        assertEquals(0, leafReader.numDocs());
                        assertNull(leafReader.terms("id"));
                        TermsEnum iterator = leafReader.terms("text").iterator();
                        assertTrue(iterator.seekExact(new BytesRef("quick")));
                        assertEquals(leafReader.maxDoc(), iterator.docFreq());
                    }
                }
            }

        }
    }
}
