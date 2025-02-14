/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class MemoryEstimationMergeTests extends ESTestCase {

    public void testMerge() throws IOException {
        try (Directory dir = newDirectory()) {
            IndexWriterConfig iwc = newIndexWriterConfig();
            int numDocs = 1000;
            int vectorDims = 100;

            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                for (int i = 0; i < numDocs; i++) {
                    if (rarely()) {
                        writer.flush();
                    }
                    if (rarely()) {
                        writer.forceMerge(1, false);
                    }
                    Document doc = new Document();
                    doc.add(new StringField("id", "" + i, Field.Store.NO));
                    doc.add(newTextField("text", "the quick brown fox", Field.Store.YES));
                    doc.add(new NumericDocValuesField("sort", i));
                    doc.add(new KnnFloatVectorField("floatVector", floatVector(vectorDims)));
                    writer.addDocument(doc);
                    if (i == numDocs / 2) {
                        writer.flush();
                    }
                }

                writer.forceMerge(1);

            }

        }
    }

    private float[] floatVector(int vectorDims) {
        return new float[vectorDims];
    }
}
