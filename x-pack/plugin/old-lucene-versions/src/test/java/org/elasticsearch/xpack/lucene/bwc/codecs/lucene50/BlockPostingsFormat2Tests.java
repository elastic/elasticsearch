/*
 * @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modifications copyright (C) 2021 Elasticsearch B.V.
 */
package org.elasticsearch.xpack.lucene.bwc.codecs.lucene50;

import org.apache.lucene.backward_codecs.lucene50.Lucene50PostingsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.test.ESTestCase;

/** Tests special cases of BlockPostingsFormat */
public class BlockPostingsFormat2Tests extends ESTestCase {
    Directory dir;
    RandomIndexWriter iw;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        dir = newFSDirectory(createTempDir("testDFBlockSize"));
        IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
        iwc.setCodec(TestUtil.alwaysPostingsFormat(new Lucene50RWPostingsFormat()));
        iw = new RandomIndexWriter(random(), dir, iwc);
        iw.setDoRandomForceMerge(false); // we will ourselves
    }

    @Override
    public void tearDown() throws Exception {
        iw.close();
        TestUtil.checkIndex(dir); // for some extra coverage, checkIndex before we forceMerge
        IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
        iwc.setCodec(TestUtil.alwaysPostingsFormat(new Lucene50RWPostingsFormat()));
        iwc.setOpenMode(OpenMode.APPEND);
        IndexWriter iw = new IndexWriter(dir, iwc);
        iw.forceMerge(1);
        iw.close();
        dir.close(); // just force a checkindex for now
        super.tearDown();
    }

    private Document newDocument() {
        Document doc = new Document();
        for (IndexOptions option : IndexOptions.values()) {
            if (option == IndexOptions.NONE) {
                continue;
            }
            FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
            // turn on tvs for a cross-check, since we rely upon checkindex in this test (for now)
            ft.setStoreTermVectors(true);
            ft.setStoreTermVectorOffsets(true);
            ft.setStoreTermVectorPositions(true);
            ft.setStoreTermVectorPayloads(true);
            ft.setIndexOptions(option);
            doc.add(new Field(option.toString(), "", ft));
        }
        return doc;
    }

    /** tests terms with df = blocksize */
    public void testDFBlockSize() throws Exception {
        Document doc = newDocument();
        for (int i = 0; i < Lucene50PostingsFormat.BLOCK_SIZE; i++) {
            for (IndexableField f : doc.getFields()) {
                ((Field) f).setStringValue(f.name() + " " + f.name() + "_2");
            }
            iw.addDocument(doc);
        }
    }

    /** tests terms with df % blocksize = 0 */
    public void testDFBlockSizeMultiple() throws Exception {
        Document doc = newDocument();
        for (int i = 0; i < Lucene50PostingsFormat.BLOCK_SIZE * 16; i++) {
            for (IndexableField f : doc.getFields()) {
                ((Field) f).setStringValue(f.name() + " " + f.name() + "_2");
            }
            iw.addDocument(doc);
        }
    }

    /** tests terms with ttf = blocksize */
    public void testTTFBlockSize() throws Exception {
        Document doc = newDocument();
        for (int i = 0; i < Lucene50PostingsFormat.BLOCK_SIZE / 2; i++) {
            for (IndexableField f : doc.getFields()) {
                ((Field) f).setStringValue(f.name() + " " + f.name() + " " + f.name() + "_2 " + f.name() + "_2");
            }
            iw.addDocument(doc);
        }
    }

    /** tests terms with ttf % blocksize = 0 */
    public void testTTFBlockSizeMultiple() throws Exception {
        Document doc = newDocument();
        for (int i = 0; i < Lucene50PostingsFormat.BLOCK_SIZE / 2; i++) {
            for (IndexableField f : doc.getFields()) {
                String proto = (f.name()
                    + " "
                    + f.name()
                    + " "
                    + f.name()
                    + " "
                    + f.name()
                    + " "
                    + f.name()
                    + "_2 "
                    + f.name()
                    + "_2 "
                    + f.name()
                    + "_2 "
                    + f.name()
                    + "_2");
                StringBuilder val = new StringBuilder();
                for (int j = 0; j < 16; j++) {
                    val.append(proto);
                    val.append(" ");
                }
                ((Field) f).setStringValue(val.toString());
            }
            iw.addDocument(doc);
        }
    }
}
