/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.internal;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FieldUsageTrackingDirectoryReaderTests extends ESTestCase {

    public void testTermsMinAndMax() throws IOException {
        Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(null));
        Document doc = new Document();
        StringField fooField = new StringField("foo", "bar", Field.Store.NO);
        doc.add(fooField);
        w.addDocument(doc);
        w.flush();

        DirectoryReader directoryReader = DirectoryReader.open(w);
        for (LeafReaderContext lrc : directoryReader.leaves()) {
            FieldUsageTrackingDirectoryReader.FieldUsageTrackingLeafReader leafReader =
                new FieldUsageTrackingDirectoryReader.FieldUsageTrackingLeafReader(lrc.reader(), new TestFieldUsageNotifier());
            FieldUsageTrackingDirectoryReader.FieldUsageTrackingLeafReader.FieldUsageTrackingTerms terms =
                leafReader.new FieldUsageTrackingTerms("foo", lrc.reader().terms("foo")) {
                    @Override
                    public TermsEnum iterator() {
                        fail("Retrieving min and max should retrieve values from block tree instead of iterating");
                        return null;
                    }
                };
            assertEquals("bar", terms.getMin().utf8ToString());
            assertEquals("bar", terms.getMax().utf8ToString());
        }
        w.close();
        directoryReader.close();
        dir.close();
    }

    public void testDocValuesSkipperUsage() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(null))) {
            Document doc = new Document();
            doc.add(NumericDocValuesField.indexedField("skipped", 42L));
            doc.add(new NumericDocValuesField("plain", 7L));
            w.addDocument(doc);
            w.flush();

            try (DirectoryReader directoryReader = DirectoryReader.open(w)) {
                for (LeafReaderContext lrc : directoryReader.leaves()) {
                    List<String> skipperFields = new ArrayList<>();
                    var leafReader = new FieldUsageTrackingDirectoryReader.FieldUsageTrackingLeafReader(
                        lrc.reader(),
                        new TestFieldUsageNotifier() {
                            @Override
                            public void onDocValuesSkipperUsed(String field) {
                                skipperFields.add(field);
                            }
                        }
                    );
                    assertNull(leafReader.getDocValuesSkipper("plain"));
                    assertNull(leafReader.getDocValuesSkipper("missing"));
                    assertTrue(skipperFields.isEmpty());
                    assertNotNull(leafReader.getDocValuesSkipper("skipped"));
                    assertEquals(List.of("skipped"), skipperFields);
                }
            }
        }
    }

    private static class TestFieldUsageNotifier implements FieldUsageTrackingDirectoryReader.FieldUsageNotifier {
        @Override
        public void onTermsUsed(String field) {

        }

        @Override
        public void onPostingsUsed(String field) {

        }

        @Override
        public void onTermFrequenciesUsed(String field) {

        }

        @Override
        public void onPositionsUsed(String field) {

        }

        @Override
        public void onOffsetsUsed(String field) {

        }

        @Override
        public void onDocValuesUsed(String field) {

        }

        @Override
        public void onDocValuesSkipperUsed(String field) {

        }

        @Override
        public void onStoredFieldsUsed(String field) {

        }

        @Override
        public void onNormsUsed(String field) {

        }

        @Override
        public void onPayloadsUsed(String field) {

        }

        @Override
        public void onPointsUsed(String field) {

        }

        @Override
        public void onTermVectorsUsed(String field) {

        }

        @Override
        public void onKnnVectorsUsed(String field) {

        }
    }
}
