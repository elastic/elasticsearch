/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.tests.index.ForceMergePolicy;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.codec.Elasticsearch900Lucene101Codec;
import org.elasticsearch.index.codec.Elasticsearch910Lucene102Codec;
import org.elasticsearch.index.codec.tsdb.ES87TSDBDocValuesFormatTests.TestES87TSDBDocValuesFormat;
import org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesFormat;
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

    @SuppressWarnings("checkstyle:LineLength")
    public void testDuel() throws IOException {
        try (var baselineDirectory = newDirectory(); var contenderDirectory = newDirectory()) {
            int numDocs = randomIntBetween(256, 32768);

            var mergePolicy = new ForceMergePolicy(newLogMergePolicy());
            var baselineConfig = newIndexWriterConfig();
            baselineConfig.setMergePolicy(mergePolicy);
            baselineConfig.setCodec(TestUtil.alwaysDocValuesFormat(new Lucene90DocValuesFormat()));
            var contenderConf = newIndexWriterConfig();
            contenderConf.setMergePolicy(mergePolicy);
            Codec codec = new Elasticsearch910Lucene102Codec() {

                final DocValuesFormat docValuesFormat = randomBoolean()
                    ? new ES819TSDBDocValuesFormat()
                    : new TestES87TSDBDocValuesFormat();

                @Override
                public DocValuesFormat getDocValuesFormatForField(String field) {
                    return docValuesFormat;
                }
            };
            contenderConf.setCodec(codec);
            contenderConf.setMergePolicy(mergePolicy);

            try (
                var baselineIw = new RandomIndexWriter(random(), baselineDirectory, baselineConfig);
                var contenderIw = new RandomIndexWriter(random(), contenderDirectory, contenderConf)
            ) {
                boolean field1MissingOften = rarely();
                boolean field2And3MissingOften = rarely();
                boolean field4MissingOften = rarely();
                boolean field5MissingOften = rarely();

                String reuseStr = null;
                if (randomInt(5) == 1) {
                    reuseStr = randomUnicodeOfLength(20);
                }
                Long reuseLng = null;
                if (randomInt(5) == 4) {
                    reuseLng = randomLong();
                }

                for (int i = 0; i < numDocs; i++) {
                    Document doc = new Document();
                    if (field1MissingOften ? randomBoolean() : rarely() == false) {
                        String value = reuseStr != null && randomBoolean() ? reuseStr : randomUnicodeOfLength(20);
                        doc.add(new SortedDocValuesField(FIELD_1, newBytesRef(value)));
                    }
                    if (field2And3MissingOften ? randomBoolean() : rarely() == false) {
                        int numValues = randomIntBetween(1, 32);
                        for (int j = 0; j < numValues; j++) {
                            String strValue = reuseStr != null && randomBoolean() ? reuseStr : randomUnicodeOfLength(20);
                            doc.add(new SortedSetDocValuesField(FIELD_2, newBytesRef(strValue)));
                            long lngValue = reuseLng != null && randomBoolean() ? reuseLng : randomLong();
                            doc.add(new SortedNumericDocValuesField(FIELD_3, lngValue));
                        }
                    }
                    if (field4MissingOften ? randomBoolean() : rarely() == false) {
                        long value = reuseLng != null && randomBoolean() ? reuseLng : randomLong();
                        doc.add(new NumericDocValuesField(FIELD_4, value));
                    }
                    if (field5MissingOften ? randomBoolean() : rarely() == false) {
                        String value = reuseStr != null && randomBoolean() ? reuseStr : randomUnicodeOfLength(20);
                        doc.add(new BinaryDocValuesField(FIELD_5, newBytesRef(value)));
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
                assertDocIds(baseline, baselineDocId, contender, contentedDocId);
                assertEquals(baseline.ordValue(), contender.ordValue());
                assertEquals(baseline.lookupOrd(baseline.ordValue()), contender.lookupOrd(contender.ordValue()));
            }
        }
        // test advance()
        {
            var baseline = baselineReader.getSortedDocValues(FIELD_1);
            var contender = contenderReader.getSortedDocValues(FIELD_1);
            assertEquals(baseline.getValueCount(), contender.getValueCount());
            for (int i = 0; i < docIdsToAdvanceTo.length; i++) {
                int docId = docIdsToAdvanceTo[i];
                int baselineTarget = assertAdvance(docId, baselineReader, contenderReader, baseline, contender);
                if (baselineTarget == NO_MORE_DOCS) {
                    break;
                }
                assertEquals(baseline.ordValue(), contender.ordValue());
                assertEquals(baseline.lookupOrd(baseline.ordValue()), contender.lookupOrd(contender.ordValue()));
                i = shouldSkipDocIds(i, docId, baselineTarget, docIdsToAdvanceTo);
                if (i == -1) {
                    break;
                }
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
                assertEquals(baseline.docID(), contender.docID());
                if (baselineFound) {
                    assertEquals(docId, baseline.docID());
                    assertEquals(docId, contender.docID());
                    assertEquals(baseline.docID(), contender.docID());
                    assertEquals(baseline.ordValue(), contender.ordValue());
                    assertEquals(baseline.lookupOrd(baseline.ordValue()), contender.lookupOrd(contender.ordValue()));
                }
            }
        }
        // Test termsEnum()
        BytesRef seekTo = null;
        {
            var baseline = baselineReader.getSortedDocValues(FIELD_1);
            var contender = contenderReader.getSortedDocValues(FIELD_1);
            assertEquals(baseline.getValueCount(), contender.getValueCount());
            var baseTE = baseline.termsEnum();
            var contenderTE = contender.termsEnum();
            for (BytesRef baseTerm = baseTE.next(); baseTerm != null; baseTerm = baseTE.next()) {
                BytesRef contenderTerm = contenderTE.next();
                if (seekTo == null || rarely()) {
                    seekTo = BytesRef.deepCopyOf(baseTerm);
                }
                assertEquals(baseTerm, contenderTerm);
            }
        }
        // Test termsEnum() with seek.
        {
            var baseline = baselineReader.getSortedDocValues(FIELD_1);
            var contender = contenderReader.getSortedDocValues(FIELD_1);
            assertEquals(baseline.getValueCount(), contender.getValueCount());
            var baseTE = baseline.termsEnum();
            var contenderTE = contender.termsEnum();

            if (randomBoolean()) {
                assertTrue(baseTE.seekExact(seekTo));
                assertTrue(contenderTE.seekExact(seekTo));
            } else {
                var status = baseTE.seekCeil(seekTo);
                assertEquals(TermsEnum.SeekStatus.FOUND, status);
                status = contenderTE.seekCeil(seekTo);
                assertEquals(TermsEnum.SeekStatus.FOUND, status);
            }
            assertEquals(baseTE.term(), contenderTE.term());
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
                assertDocIds(baseline, baselineDocId, contender, contentedDocId);
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
            for (int i = 0; i < docIdsToAdvanceTo.length; i++) {
                int docId = docIdsToAdvanceTo[i];
                int baselineTarget = assertAdvance(docId, baselineReader, contenderReader, baseline, contender);
                if (baselineTarget == NO_MORE_DOCS) {
                    break;
                }
                assertEquals(baseline.docValueCount(), contender.docValueCount());
                for (int j = 0; j < baseline.docValueCount(); j++) {
                    long baselineOrd = baseline.nextOrd();
                    long contenderOrd = contender.nextOrd();
                    assertEquals(baselineOrd, contenderOrd);
                    assertEquals(baseline.lookupOrd(baselineOrd), contender.lookupOrd(contenderOrd));
                }
                i = shouldSkipDocIds(i, docId, baselineTarget, docIdsToAdvanceTo);
                if (i == -1) {
                    break;
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
                assertEquals(baseline.docID(), contender.docID());
                if (baselineFound) {
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
        // Test termsEnum()
        BytesRef seekTo = null;
        {
            var baseline = baselineReader.getSortedSetDocValues(FIELD_2);
            var contender = contenderReader.getSortedSetDocValues(FIELD_2);
            assertEquals(baseline.getValueCount(), contender.getValueCount());
            var baseTE = baseline.termsEnum();
            var contenderTE = contender.termsEnum();
            for (BytesRef baseTerm = baseTE.next(); baseTerm != null; baseTerm = baseTE.next()) {
                BytesRef contenderTerm = contenderTE.next();
                if (seekTo == null || rarely()) {
                    seekTo = BytesRef.deepCopyOf(baseTerm);
                }
                assertEquals(baseTerm, contenderTerm);
            }
        }
        // Test termsEnum() with seek.
        {
            var baseline = baselineReader.getSortedSetDocValues(FIELD_2);
            var contender = contenderReader.getSortedSetDocValues(FIELD_2);
            assertEquals(baseline.getValueCount(), contender.getValueCount());
            var baseTE = baseline.termsEnum();
            var contenderTE = contender.termsEnum();

            if (randomBoolean()) {
                assertTrue(baseTE.seekExact(seekTo));
                assertTrue(contenderTE.seekExact(seekTo));
            } else {
                var status = baseTE.seekCeil(seekTo);
                assertEquals(TermsEnum.SeekStatus.FOUND, status);
                status = contenderTE.seekCeil(seekTo);
                assertEquals(TermsEnum.SeekStatus.FOUND, status);
            }
            assertEquals(baseTE.term(), contenderTE.term());
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
                assertDocIds(baseline, baselineDocId, contender, contentedDocId);
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
            for (int i = 0; i < docIdsToAdvanceTo.length; i++) {
                int docId = docIdsToAdvanceTo[i];
                int baselineTarget = assertAdvance(docId, baselineReader, contenderReader, baseline, contender);
                if (baselineTarget == NO_MORE_DOCS) {
                    break;
                }
                assertEquals(baseline.docValueCount(), contender.docValueCount());
                for (int j = 0; j < baseline.docValueCount(); j++) {
                    long baselineValue = baseline.nextValue();
                    long contenderValue = contender.nextValue();
                    assertEquals(baselineValue, contenderValue);
                }
                i = shouldSkipDocIds(i, docId, baselineTarget, docIdsToAdvanceTo);
                if (i == -1) {
                    break;
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
                assertEquals(baseline.docID(), contender.docID());
                if (baselineResult) {
                    assertEquals(baseline.docValueCount(), contender.docValueCount());
                    for (int i = 0; i < baseline.docValueCount(); i++) {
                        long baselineValue = baseline.nextValue();
                        long contenderValue = contender.nextValue();
                        assertEquals(baselineValue, contenderValue);
                    }
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
                assertDocIds(baseline, baselineDocId, contender, contentedDocId);
                assertEquals(baseline.longValue(), contender.longValue());
            }
        }
        // test advance()
        {
            var baseline = baselineReader.getNumericDocValues(FIELD_4);
            var contender = contenderReader.getNumericDocValues(FIELD_4);
            for (int i = 0; i < docIdsToAdvanceTo.length; i++) {
                int docId = docIdsToAdvanceTo[i];
                int baselineTarget = assertAdvance(docId, baselineReader, contenderReader, baseline, contender);
                if (baselineTarget == NO_MORE_DOCS) {
                    break;
                }
                assertEquals(baseline.longValue(), contender.longValue());
                i = shouldSkipDocIds(i, docId, baselineTarget, docIdsToAdvanceTo);
                if (i == -1) {
                    break;
                }
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
                assertEquals(baseline.docID(), contender.docID());
                if (baselineResult) {
                    assertEquals(baseline.longValue(), contender.longValue());
                }
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
                assertDocIds(baseline, baselineDocId, contender, contentedDocId);
                assertEquals(baseline.binaryValue(), contender.binaryValue());
            }
        }
        // test advance()
        {
            var baseline = baselineReader.getBinaryDocValues(FIELD_5);
            var contender = contenderReader.getBinaryDocValues(FIELD_5);
            for (int i = 0; i < docIdsToAdvanceTo.length; i++) {
                int docId = docIdsToAdvanceTo[i];
                int baselineTarget = assertAdvance(docId, baselineReader, contenderReader, baseline, contender);
                if (baselineTarget != NO_MORE_DOCS) {
                    break;
                }
                assertEquals(baseline.binaryValue(), contender.binaryValue());
                i = shouldSkipDocIds(i, docId, baselineTarget, docIdsToAdvanceTo);
                if (i == -1) {
                    break;
                }
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
                assertEquals(baseline.docID(), contender.docID());
                if (baselineResult) {
                    assertEquals(baseline.binaryValue(), contender.binaryValue());
                }
            }
        }
    }

    private static int assertAdvance(
        int docId,
        LeafReader baselineReader,
        LeafReader contenderReader,
        DocIdSetIterator baseline,
        DocIdSetIterator contender
    ) throws IOException {
        assert docId < baselineReader.maxDoc() : "exhausted DocIdSetIterator yields undefined behaviour";
        assert docId > baseline.docID()
            : "target must be greater then the current docId in DocIdSetIterator, otherwise this can yield undefined behaviour";
        int baselineTarget = baseline.advance(docId);
        assert docId < contenderReader.maxDoc() : "exhausted DocIdSetIterator yields undefined behaviour";
        assert docId > contender.docID()
            : "target must be greater then the current docId in DocIdSetIterator, otherwise this can yield undefined behaviour";
        int contenderTarget = contender.advance(docId);
        assertDocIds(baseline, baselineTarget, contender, contenderTarget);
        return baselineTarget;
    }

    private static int shouldSkipDocIds(int i, int docId, int baselineTarget, Integer[] docIdsToAdvanceTo) {
        if (i < (docIdsToAdvanceTo.length - 1) && baselineTarget > docId) {
            for (int j = i + 1; j < docIdsToAdvanceTo.length; j++) {
                int nextDocId = docIdsToAdvanceTo[j];
                if (nextDocId > baselineTarget) {
                    return j - 1; // -1 because the loop from which this method is invoked executes: i++
                }
            }
            return -1;
        } else {
            return i;
        }
    }

    private static void assertDocIds(DocIdSetIterator baseline, int baselineDocId, DocIdSetIterator contender, int contenderDocId) {
        assertEquals(baselineDocId, contenderDocId);
        assertEquals(baselineDocId, baseline.docID());
        assertEquals(contenderDocId, contender.docID());
    }
}
