/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.BaseIndexFileFormatTestCase;
import org.elasticsearch.common.logging.LogConfigurator;

import java.io.IOException;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

abstract class BaseKnnBitVectorsFormatTestCase extends BaseIndexFileFormatTestCase {

    static {
        LogConfigurator.loadLog4jPlugins();
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized
    }

    @Override
    protected void addRandomFields(Document doc) {
        doc.add(new KnnByteVectorField("v2", randomVector(30), similarityFunction));
    }

    protected VectorSimilarityFunction similarityFunction;

    protected VectorSimilarityFunction randomSimilarity() {
        return VectorSimilarityFunction.values()[random().nextInt(VectorSimilarityFunction.values().length)];
    }

    byte[] randomVector(int dims) {
        byte[] vector = new byte[dims];
        random().nextBytes(vector);
        return vector;
    }

    public void testRandom() throws Exception {
        IndexWriterConfig iwc = newIndexWriterConfig();
        if (random().nextBoolean()) {
            iwc.setIndexSort(new Sort(new SortField("sortkey", SortField.Type.INT)));
        }
        String fieldName = "field";
        try (Directory dir = newDirectory(); IndexWriter iw = new IndexWriter(dir, iwc)) {
            int numDoc = atLeast(100);
            int dimension = atLeast(10);
            if (dimension % 2 != 0) {
                dimension++;
            }
            byte[] scratch = new byte[dimension];
            int numValues = 0;
            byte[][] values = new byte[numDoc][];
            for (int i = 0; i < numDoc; i++) {
                if (random().nextInt(7) != 3) {
                    // usually index a vector value for a doc
                    values[i] = randomVector(dimension);
                    ++numValues;
                }
                if (random().nextBoolean() && values[i] != null) {
                    // sometimes use a shared scratch array
                    System.arraycopy(values[i], 0, scratch, 0, scratch.length);
                    add(iw, fieldName, i, scratch, similarityFunction);
                } else {
                    add(iw, fieldName, i, values[i], similarityFunction);
                }
                if (random().nextInt(10) == 2) {
                    // sometimes delete a random document
                    int idToDelete = random().nextInt(i + 1);
                    iw.deleteDocuments(new Term("id", Integer.toString(idToDelete)));
                    // and remember that it was deleted
                    if (values[idToDelete] != null) {
                        values[idToDelete] = null;
                        --numValues;
                    }
                }
                if (random().nextInt(10) == 3) {
                    iw.commit();
                }
            }
            int numDeletes = 0;
            try (IndexReader reader = DirectoryReader.open(iw)) {
                int valueCount = 0, totalSize = 0;
                for (LeafReaderContext ctx : reader.leaves()) {
                    ByteVectorValues vectorValues = ctx.reader().getByteVectorValues(fieldName);
                    if (vectorValues == null) {
                        continue;
                    }
                    totalSize += vectorValues.size();
                    StoredFields storedFields = ctx.reader().storedFields();
                    int docId;
                    KnnVectorValues.DocIndexIterator iterator = vectorValues.iterator();
                    while ((docId = iterator.nextDoc()) != NO_MORE_DOCS) {
                        byte[] v = vectorValues.vectorValue(iterator.index());
                        assertEquals(dimension, v.length);
                        String idString = storedFields.document(docId).getField("id").stringValue();
                        int id = Integer.parseInt(idString);
                        if (ctx.reader().getLiveDocs() == null || ctx.reader().getLiveDocs().get(docId)) {
                            assertArrayEquals(idString, values[id], v);
                            ++valueCount;
                        } else {
                            ++numDeletes;
                            assertNull(values[id]);
                        }
                    }
                }
                assertEquals(numValues, valueCount);
                assertEquals(numValues, totalSize - numDeletes);
            }
        }
    }

    private void add(IndexWriter iw, String field, int id, byte[] vector, VectorSimilarityFunction similarity) throws IOException {
        add(iw, field, id, random().nextInt(100), vector, similarity);
    }

    private void add(IndexWriter iw, String field, int id, int sortKey, byte[] vector, VectorSimilarityFunction similarityFunction)
        throws IOException {
        Document doc = new Document();
        if (vector != null) {
            doc.add(new KnnByteVectorField(field, vector, similarityFunction));
        }
        doc.add(new NumericDocValuesField("sortkey", sortKey));
        String idString = Integer.toString(id);
        doc.add(new StringField("id", idString, Field.Store.YES));
        Term idTerm = new Term("id", idString);
        iw.updateDocument(idTerm, doc);
    }

}
