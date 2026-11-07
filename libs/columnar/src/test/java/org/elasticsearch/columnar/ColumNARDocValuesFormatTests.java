/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.numeric.NumericPipeline;
import org.elasticsearch.columnar.numeric.NumericPipelineSelector;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

public class ColumNARDocValuesFormatTests extends ESTestCase {

    public void testInvalidBlockSizeRejected() {
        final NumericPipelineSelector sel = (f, t) -> NumericPipeline::defaultPipeline;
        for (int bs : new int[] { 0, -1, 64, 127, 384, 640, ColumNARDocValuesFormat.MAX_BLOCK_SIZE * 2 }) {
            final int blockSize = bs;
            expectThrows(
                IllegalArgumentException.class,
                () -> new ColumNARDocValuesFormat(sel, field -> ColumnarFieldType.LONG, blockSize)
            );
        }
    }

    public void testReadOnlySpiConstructorRejectsWrites() throws IOException {
        try (Directory dir = newDirectory()) {
            final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(ColumnarTestUtils.columnarCodec(new ColumNARDocValuesFormat()));
            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                final Document doc = new Document();
                doc.add(new Field("value", new BytesRef(new byte[] { 0 }), ColumnarTestUtils.columnarBinaryFieldType()));
                writer.addDocument(doc);
                final IllegalStateException e = expectThrows(IllegalStateException.class, writer::commit);
                assertThat(e.getMessage(), containsString("read-only SPI constructor"));
            }
        }
    }
}
