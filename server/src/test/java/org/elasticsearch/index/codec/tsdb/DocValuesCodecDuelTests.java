/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.tests.index.ForceMergePolicy;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

public class DocValuesCodecDuelTests extends ESTestCase {

    private static final String FIELD_1 = "string_field_1";
    private static final String FIELD_2 = "string_field_2";
    private static final String FIELD_3 = "number_field_3";
    private static final String FIELD_4 = "number_field_4";
    private static final String FIELD_5 = "binary_field_5";

    public void testDuel() throws IOException {
        try (var baselineDirectory = newDirectory(); var contenderDirectory = newDirectory()) {
            int numDocs = randomIntBetween(256, 32768);

            IndexWriterConfig baselineConfig = newIndexWriterConfig();
            baselineConfig.setMergePolicy(new ForceMergePolicy(newLogMergePolicy()));
            baselineConfig.setCodec(TestUtil.alwaysDocValuesFormat(new Lucene90DocValuesFormat()));
            IndexWriterConfig contenderConf = newIndexWriterConfig();
            contenderConf.setCodec(TestUtil.alwaysDocValuesFormat(new ES87TSDBDocValuesFormat()));
            contenderConf.setMergePolicy(new ForceMergePolicy(newLogMergePolicy()));

            try (
                var baselineIw = new RandomIndexWriter(random(), baselineDirectory, baselineConfig);
                var contenderIw = new RandomIndexWriter(random(), contenderDirectory, contenderConf)
            ) {
                for (int i = 0; i < numDocs; i++) {
                    Document doc = new Document();
                    if (rarely() == false) {
                        doc.add(new SortedDocValuesField(FIELD_1, newBytesRef(randomUnicodeOfLength(8))));
                    }
                    if (rarely() == false) {
                        int numValues = randomIntBetween(1, 32);
                        for (int j = 0; j < numValues; j++) {
                            doc.add(new SortedSetDocValuesField(FIELD_2, newBytesRef(randomUnicodeOfLength(8))));
                            doc.add(new SortedNumericDocValuesField(FIELD_3, randomLong()));
                        }
                    }
                    if (rarely() == false) {
                        doc.add(new NumericDocValuesField(FIELD_4, randomLong()));
                    }
                    if (rarely() == false) {
                        doc.add(new BinaryDocValuesField(FIELD_5, newBytesRef(randomUnicodeOfLength(8))));
                    }
                    baselineIw.addDocument(doc);
                    contenderIw.addDocument(doc);
                }
                baselineIw.forceMerge(1);
                contenderIw.forceMerge(1);
            }
            try (var baselineIr = DirectoryReader.open(baselineDirectory); var contenderIr = DirectoryReader.open(contenderDirectory)) {
                assertEquals(1, baselineIr.leaves().size());
                assertEquals(1, contenderIr.leaves().size());

                var baseLeafReader = baselineIr.leaves().get(0).reader();
                var contenderLeafReader = contenderIr.leaves().get(0).reader();
                assertEquals(baseLeafReader.maxDoc(), contenderLeafReader.maxDoc());

                Integer[] docIdsToAdvanceTo = randomSet(1, 1 + randomInt(numDocs / 10), () -> randomInt(numDocs - 1)).toArray(
                    Integer[]::new
                );
                Arrays.sort(docIdsToAdvanceTo);

                assertSortedDocValues(baseLeafReader, contenderLeafReader, docIdsToAdvanceTo);
                assertSortedSetDocValues(baseLeafReader, contenderLeafReader, docIdsToAdvanceTo);
                assertSortedNumericDocValues(baseLeafReader, contenderLeafReader, docIdsToAdvanceTo);
                assertNumericDocValues(baseLeafReader, contenderLeafReader, docIdsToAdvanceTo);
                assertBinaryDocValues(baseLeafReader, contenderLeafReader, docIdsToAdvanceTo);
            }
        }
    }

    private void assertSortedDocValues(LeafReader baselineReader, LeafReader contenderReader, Integer[] docIdsToAdvanceTo)
        throws IOException {
        // test nextDoc()
        {
            var baseline = baselineReader.getSortedDocValues(FIELD_1);
            var contender = contenderReader.getSortedDocValues(FIELD_1);
            assertEquals(baseline.getValueCount(), contender.getValueCount());
            for (int baselineDocId = baseline.nextDoc(); baselineDocId != NO_MORE_DOCS; baselineDocId = baseline.nextDoc()) {
                int contentedDocId = contender.nextDoc();
                assertEquals(baselineDocId, contentedDocId);
                assertEquals(baselineDocId, baseline.docID());
                assertEquals(contentedDocId, contender.docID());
                assertEquals(baseline.ordValue(), contender.ordValue());
                assertEquals(baseline.lookupOrd(baseline.ordValue()), contender.lookupOrd(contender.ordValue()));
            }
        }
        // test advance()
        {
            var baseline = baselineReader.getSortedDocValues(FIELD_1);
            var contender = contenderReader.getSortedDocValues(FIELD_1);
            assertEquals(baseline.getValueCount(), contender.getValueCount());
            for (int docId : docIdsToAdvanceTo) {
                int baselineTarget = baseline.advance(docId);
                int contenderTarget = contender.advance(docId);
                assertEquals(baselineTarget, baseline.docID());
                assertEquals(contenderTarget, contender.docID());
                assertEquals(baselineTarget, contenderTarget);
                assertEquals(baseline.ordValue(), contender.ordValue());
                assertEquals(baseline.lookupOrd(baseline.ordValue()), contender.lookupOrd(contender.ordValue()));
            }
        }
        // test advanceExact()
        {
            var baseline = baselineReader.getSortedDocValues(FIELD_1);
            var contender = contenderReader.getSortedDocValues(FIELD_1);
            assertEquals(baseline.getValueCount(), contender.getValueCount());
            for (int docId : docIdsToAdvanceTo) {
                boolean baselineFound = baseline.advanceExact(docId);
                boolean contenderFound = contender.advanceExact(docId);
                assertEquals(baselineFound, contenderFound);
                if (baselineFound) {
                    assertEquals(docId, baseline.docID());
                    assertEquals(docId, contender.docID());
                    assertEquals(baseline.docID(), contender.docID());
                    assertEquals(baseline.ordValue(), contender.ordValue());
                    assertEquals(baseline.lookupOrd(baseline.ordValue()), contender.lookupOrd(contender.ordValue()));
                }
            }
        }
    }

    private void assertSortedSetDocValues(LeafReader baselineReader, LeafReader contenderReader, Integer[] docIdsToAdvanceTo)
        throws IOException {
        // test nextDoc()
        {
            var baseline = baselineReader.getSortedSetDocValues(FIELD_2);
            var contender = contenderReader.getSortedSetDocValues(FIELD_2);
            assertEquals(baseline.getValueCount(), contender.getValueCount());
            for (int baselineDocId = baseline.nextDoc(); baselineDocId != NO_MORE_DOCS; baselineDocId = baseline.nextDoc()) {
                int contentedDocId = contender.nextDoc();
                assertEquals(baselineDocId, contentedDocId);
                assertEquals(baselineDocId, baseline.docID());
                assertEquals(contentedDocId, contender.docID());
                assertEquals(baseline.docValueCount(), contender.docValueCount());
                for (int i = 0; i < baseline.docValueCount(); i++) {
                    long baselineOrd = baseline.nextOrd();
                    long contenderOrd = contender.nextOrd();
                    assertEquals(baselineOrd, contenderOrd);
                    assertEquals(baseline.lookupOrd(baselineOrd), contender.lookupOrd(contenderOrd));
                }
            }
        }
        // test advance()
        {
            var baseline = baselineReader.getSortedSetDocValues(FIELD_2);
            var contender = contenderReader.getSortedSetDocValues(FIELD_2);
            assertEquals(baseline.getValueCount(), contender.getValueCount());
            for (int docId : docIdsToAdvanceTo) {
                int baselineTarget = baseline.advance(docId);
                int contenderTarget = contender.advance(docId);
                assertEquals(baselineTarget, contenderTarget);
                assertEquals(baselineTarget, baseline.docID());
                assertEquals(contenderTarget, contender.docID());
                assertEquals(baseline.docValueCount(), contender.docValueCount());
                for (int i = 0; i < baseline.docValueCount(); i++) {
                    long baselineOrd = baseline.nextOrd();
                    long contenderOrd = contender.nextOrd();
                    assertEquals(baselineOrd, contenderOrd);
                    assertEquals(baseline.lookupOrd(baselineOrd), contender.lookupOrd(contenderOrd));
                }
            }
        }
        // test advanceExact()
        {
            var baseline = baselineReader.getSortedSetDocValues(FIELD_2);
            var contender = contenderReader.getSortedSetDocValues(FIELD_2);
            assertEquals(baseline.getValueCount(), contender.getValueCount());
            for (int docId : docIdsToAdvanceTo) {
                boolean baselineFound = baseline.advanceExact(docId);
                boolean contenderFound = contender.advanceExact(docId);
                assertEquals(baselineFound, contenderFound);
                assertEquals(baseline.docValueCount(), contender.docValueCount());
                for (int i = 0; i < baseline.docValueCount(); i++) {
                    long baselineOrd = baseline.nextOrd();
                    long contenderOrd = contender.nextOrd();
                    assertEquals(baselineOrd, contenderOrd);
                    assertEquals(baseline.lookupOrd(baselineOrd), contender.lookupOrd(contenderOrd));
                }
            }
        }
    }

    private void assertSortedNumericDocValues(LeafReader baselineReader, LeafReader contenderReader, Integer[] docIdsToAdvanceTo)
        throws IOException {
        // test nextDoc()
        {
            var baseline = baselineReader.getSortedNumericDocValues(FIELD_3);
            var contender = contenderReader.getSortedNumericDocValues(FIELD_3);
            for (int baselineDocId = baseline.nextDoc(); baselineDocId != NO_MORE_DOCS; baselineDocId = baseline.nextDoc()) {
                int contentedDocId = contender.nextDoc();
                assertEquals(baselineDocId, contentedDocId);
                assertEquals(baselineDocId, baseline.docID());
                assertEquals(contentedDocId, contender.docID());
                assertEquals(baseline.docValueCount(), contender.docValueCount());
                for (int i = 0; i < baseline.docValueCount(); i++) {
                    long baselineValue = baseline.nextValue();
                    long contenderValue = contender.nextValue();
                    assertEquals(baselineValue, contenderValue);
                }
            }
        }
        // test advance()
        {
            var baseline = baselineReader.getSortedNumericDocValues(FIELD_3);
            var contender = contenderReader.getSortedNumericDocValues(FIELD_3);
            for (int docId : docIdsToAdvanceTo) {
                int baselineTarget = baseline.advance(docId);
                int contenderTarget = contender.advance(docId);
                assertEquals(baselineTarget, contenderTarget);
                assertEquals(baseline.docValueCount(), contender.docValueCount());
                for (int i = 0; i < baseline.docValueCount(); i++) {
                    long baselineValue = baseline.nextValue();
                    long contenderValue = contender.nextValue();
                    assertEquals(baselineValue, contenderValue);
                }
            }
        }
        // test advanceExact()
        {
            var baseline = baselineReader.getSortedNumericDocValues(FIELD_3);
            var contender = contenderReader.getSortedNumericDocValues(FIELD_3);
            for (int docId : docIdsToAdvanceTo) {
                boolean baselineResult = baseline.advanceExact(docId);
                boolean contenderResult = contender.advanceExact(docId);
                assertEquals(baselineResult, contenderResult);
                assertEquals(baseline.docValueCount(), contender.docValueCount());
                for (int i = 0; i < baseline.docValueCount(); i++) {
                    long baselineValue = baseline.nextValue();
                    long contenderValue = contender.nextValue();
                    assertEquals(baselineValue, contenderValue);
                }
            }
        }
    }

    private void assertNumericDocValues(LeafReader baselineReader, LeafReader contenderReader, Integer[] docIdsToAdvanceTo)
        throws IOException {
        // test nextDoc()
        {
            var baseline = baselineReader.getNumericDocValues(FIELD_4);
            var contender = contenderReader.getNumericDocValues(FIELD_4);
            for (int baselineDocId = baseline.nextDoc(); baselineDocId != NO_MORE_DOCS; baselineDocId = baseline.nextDoc()) {
                int contentedDocId = contender.nextDoc();
                assertEquals(baselineDocId, contentedDocId);
                assertEquals(baselineDocId, baseline.docID());
                assertEquals(contentedDocId, contender.docID());
                assertEquals(baseline.longValue(), contender.longValue());
            }
        }
        // test advance()
        {
            var baseline = baselineReader.getNumericDocValues(FIELD_4);
            var contender = contenderReader.getNumericDocValues(FIELD_4);
            for (int docId : docIdsToAdvanceTo) {
                int baselineTarget = baseline.advance(docId);
                int contenderTarget = contender.advance(docId);
                assertEquals(baselineTarget, contenderTarget);
                assertEquals(baseline.longValue(), contender.longValue());
            }
        }
        // test advanceExact()
        {
            var baseline = baselineReader.getNumericDocValues(FIELD_4);
            var contender = contenderReader.getNumericDocValues(FIELD_4);
            for (int docId : docIdsToAdvanceTo) {
                boolean baselineResult = baseline.advanceExact(docId);
                boolean contenderResult = contender.advanceExact(docId);
                assertEquals(baselineResult, contenderResult);
                assertEquals(baseline.longValue(), contender.longValue());
            }
        }
    }

    private void assertBinaryDocValues(LeafReader baselineReader, LeafReader contenderReader, Integer[] docIdsToAdvanceTo)
        throws IOException {
        // test nextDoc()
        {
            var baseline = baselineReader.getBinaryDocValues(FIELD_5);
            var contender = contenderReader.getBinaryDocValues(FIELD_5);
            for (int baselineDocId = baseline.nextDoc(); baselineDocId != NO_MORE_DOCS; baselineDocId = baseline.nextDoc()) {
                int contentedDocId = contender.nextDoc();
                assertEquals(baselineDocId, contentedDocId);
                assertEquals(baselineDocId, baseline.docID());
                assertEquals(contentedDocId, contender.docID());
                assertEquals(baseline.binaryValue(), contender.binaryValue());
            }
        }
        // test advance()
        {
            var baseline = baselineReader.getBinaryDocValues(FIELD_5);
            var contender = contenderReader.getBinaryDocValues(FIELD_5);
            for (int docId : docIdsToAdvanceTo) {
                int baselineTarget = baseline.advance(docId);
                int contenderTarget = contender.advance(docId);
                assertEquals(baselineTarget, contenderTarget);
                assertEquals(baseline.binaryValue(), contender.binaryValue());
            }
        }
        // test advanceExact()
        {
            var baseline = baselineReader.getBinaryDocValues(FIELD_5);
            var contender = contenderReader.getBinaryDocValues(FIELD_5);
            for (int docId : docIdsToAdvanceTo) {
                boolean baselineResult = baseline.advanceExact(docId);
                boolean contenderResult = contender.advanceExact(docId);
                assertEquals(baselineResult, contenderResult);
                assertEquals(baseline.binaryValue(), contender.binaryValue());
            }
        }
    }
}
