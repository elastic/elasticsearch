/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues.tracking;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

public class NumericDvSingletonOrSortedTests extends ESTestCase {

    /** A field with no doc values in the segment reads as null, and holds nothing on the breaker. */
    public void testNoDocValues() throws IOException {
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
                final Document doc = new Document();
                doc.add(new StringField("indexed_only", "a", StringField.Store.NO));
                writer.addDocument(doc);
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(1));
                for (String field : new String[] { "indexed_only", "missing" }) {
                    assertNull(field, NumericDvSingletonOrSorted.get(breaker, reader.leaves().get(0), field));
                }
                assertEquals(0, breaker.getUsed());
            }
        }
    }

    /**
     * A field with binary doc values would otherwise read as all nulls, so it fails instead, and releases what it took from the
     * breaker.
     */
    public void testBinaryDocValuesFail() throws IOException {
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
                final Document doc = new Document();
                doc.add(new BinaryDocValuesField("binary", new BytesRef("payload")));
                writer.addDocument(doc);
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(1));
                final IllegalStateException e = expectThrows(
                    IllegalStateException.class,
                    () -> NumericDvSingletonOrSorted.get(breaker, reader.leaves().get(0), "binary")
                );
                assertThat(e.getMessage(), containsString("field [binary] has binary doc values"));
                assertEquals(0, breaker.getUsed());
            }
        }
    }
}
