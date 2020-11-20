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
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.elasticsearch.test.ESTestCase;

public class SegmentsStatsTests extends ESTestCase {

    public void testFileExtensionDescriptions() throws Exception {
        try (Directory dir = newDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig()
                    .setUseCompoundFile(false)
                    .setMergePolicy(NoMergePolicy.INSTANCE))) {
                // Create a Lucene index that uses all features
                Document doc = new Document();
                StringField id = new StringField("id", "1", Store.YES);
                doc.add(id);
                // Points
                doc.add(new LongPoint("lp", 42L));
                // Doc values
                doc.add(new NumericDocValuesField("ndv", 42L));
                // Inverted index, term vectors and stored fields
                FieldType ft = new FieldType(TextField.TYPE_STORED);
                ft.setStoreTermVectors(true);
                doc.add(new Field("if", "elasticsearch", ft));
                w.addDocument(doc);
                id.setStringValue("2");
                w.addDocument(doc);
                // Create a live docs file
                w.deleteDocuments(new Term("id", "2"));
            }

            for (String file : dir.listAll()) {
                final String extension = IndexFileNames.getExtension(file);
                if (extension != null) {
                    assertTrue(SegmentsStats.FILE_DESCRIPTIONS.get(extension) != null);
                }
            }
        }
    }

}
