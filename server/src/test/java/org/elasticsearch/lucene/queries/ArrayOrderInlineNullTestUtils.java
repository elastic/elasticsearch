/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.queries;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesFormat;
import org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.lucene.tests.util.LuceneTestCase.newIndexWriterConfig;
import static org.apache.lucene.tests.util.LuceneTestCase.random;

final class ArrayOrderInlineNullTestUtils {

    private ArrayOrderInlineNullTestUtils() {}

    static RandomIndexWriter newWriter(org.apache.lucene.store.Directory dir) throws IOException {
        var iwc = newIndexWriterConfig();
        iwc.setCodec(TestUtil.alwaysDocValuesFormat(new ES819TSDBDocValuesFormat()));
        return new RandomIndexWriter(random(), dir, iwc);
    }

    /**
     * Adds a document whose {@code fieldName} carries the given slots in document order. A {@code null} element is a null slot. The blob is
     * only written when at least one non-null value is present, exactly as {@link MultiValuedBinaryDocValuesField.ArrayOrderInlineNull}
     * does on the indexing path.
     */
    static void addDoc(RandomIndexWriter writer, String fieldName, String... slots) throws IOException {
        Document document = new Document();
        List<BytesRef> slotRefs = new ArrayList<>(slots.length);
        boolean hasNonNull = false;
        for (String slot : slots) {
            if (slot == null) {
                slotRefs.add(null);
            } else {
                slotRefs.add(new BytesRef(slot));
                hasNonNull = true;
            }
        }
        if (hasNonNull) {
            document.add(new BinaryDocValuesField(fieldName, MultiValuedBinaryDocValuesField.ArrayOrderInlineNull.encode(slotRefs)));
        }
        document.add(NumericDocValuesField.indexedField(fieldName + ".counts", slots.length));
        writer.addDocument(document);
    }
}
