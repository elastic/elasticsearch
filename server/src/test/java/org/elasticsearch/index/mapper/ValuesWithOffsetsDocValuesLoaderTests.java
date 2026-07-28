/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.function.Function;

/**
 * Tests for {@link ValuesWithOffsetsDocValuesLoader}, focusing on edge cases
 * in offset data handling during synthetic source reconstruction.
 */
public class ValuesWithOffsetsDocValuesLoaderTests extends ESTestCase {

    /**
     * Verifies that the loader falls back to sequential doc value output when offset metadata
     * exists but contains no actual offset entries, while values are present. This state arises
     * when maybeRecordEmptyArray creates offset metadata but individual offsets are not recorded
     * (e.g. because addIgnoredFieldFromContext set recordedSource=true, blocking shouldRecordOffset).
     * Without the fallback, count() returns 1 but write() outputs nothing, causing an
     * XContentGenerationException ("Can not write a field name, expecting a value").
     */
    public void testEmptyOffsetsWithValuesFallsBackToSequential() throws IOException {
        BytesRef emptyOffsets = FieldArrayContext.encodeOffsetArray(new FieldArrayContext.Offsets());

        try (Directory directory = newDirectory()) {
            Document doc = new Document();
            doc.add(new SortedSetDocValuesField("field", new BytesRef("a")));
            doc.add(new SortedSetDocValuesField("field", new BytesRef("b")));
            doc.add(new SortedDocValuesField("field.offsets", emptyOffsets));
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
                iw.addDocument(doc);
                iw.commit();
            }

            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                LeafReader leafReader = reader.leaves().get(0).reader();
                ValuesWithOffsetsDocValuesLoader loader = ValuesWithOffsetsDocValuesLoader.sortedSetLoader(
                    leafReader.getSortedSetDocValues("field"),
                    leafReader.getSortedDocValues("field.offsets"),
                    Function.identity()
                );

                assertTrue(loader.advanceToDoc(0));
                assertEquals(2, loader.count());

                XContentBuilder b = JsonXContent.contentBuilder();
                b.startObject();
                b.field("field");
                b.startArray();
                loader.write(b);
                b.endArray();
                b.endObject();
                assertEquals("{\"field\":[\"a\",\"b\"]}", BytesReference.bytes(b).utf8ToString());
            }
        }
    }

    /**
     * Verifies that the loader correctly uses offset data when it is present and non-empty,
     * reordering values according to the recorded offsets.
     */
    public void testNonEmptyOffsetsReorderValues() throws IOException {
        FieldArrayContext ctx = new FieldArrayContext();
        ctx.recordOffset("field.offsets", "b");
        ctx.recordOffset("field.offsets", "a");
        TestDocumentParserContext parserCtx = new TestDocumentParserContext();
        ctx.addToLuceneDocument(parserCtx);
        BytesRef offsets = parserCtx.doc().getField("field.offsets").binaryValue();

        try (Directory directory = newDirectory()) {
            Document doc = new Document();
            doc.add(new SortedSetDocValuesField("field", new BytesRef("a")));
            doc.add(new SortedSetDocValuesField("field", new BytesRef("b")));
            doc.add(new SortedDocValuesField("field.offsets", offsets));
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
                iw.addDocument(doc);
                iw.commit();
            }

            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                LeafReader leafReader = reader.leaves().get(0).reader();
                ValuesWithOffsetsDocValuesLoader loader = ValuesWithOffsetsDocValuesLoader.sortedSetLoader(
                    leafReader.getSortedSetDocValues("field"),
                    leafReader.getSortedDocValues("field.offsets"),
                    Function.identity()
                );

                assertTrue(loader.advanceToDoc(0));
                assertEquals(3, loader.count());

                XContentBuilder b = JsonXContent.contentBuilder();
                b.startObject();
                b.field("field");
                b.startArray();
                loader.write(b);
                b.endArray();
                b.endObject();
                assertEquals("{\"field\":[\"b\",\"a\"]}", BytesReference.bytes(b).utf8ToString());
            }
        }
    }
}
