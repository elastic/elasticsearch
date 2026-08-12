/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

/** The cardinality probe's accept / overflow behavior, ordinal assignment, and the dictionary's wire format. */
public class StringDictionaryTests extends ESTestCase {

    public void testEmptyProbeYieldsNoDictionary() {
        assertNull(new StringDictionary.Builder().build());
    }

    /** Ordinals are handed out in first-seen order, and repeats do not consume a new one. */
    public void testOrdinalsFollowInsertionOrder() {
        StringDictionary.Builder builder = new StringDictionary.Builder();
        builder.add(new BytesRef("nginx"));
        builder.add(new BytesRef("apache"));
        builder.add(new BytesRef("nginx"));
        builder.add(new BytesRef("kafka"));

        StringDictionary dictionary = builder.build();
        assertNotNull(dictionary);
        assertEquals(3, dictionary.size());
        assertEquals(0, dictionary.ordinal(new BytesRef("nginx")));
        assertEquals(1, dictionary.ordinal(new BytesRef("apache")));
        assertEquals(2, dictionary.ordinal(new BytesRef("kafka")));
        assertEquals(new BytesRef("nginx"), dictionary.term(0));
        assertEquals(new BytesRef("apache"), dictionary.term(1));
        assertEquals(new BytesRef("kafka"), dictionary.term(2));
    }

    /** The builder's key must be a copy: a cursor hands back one reused BytesRef for every value. */
    public void testReusedBytesRefIsCopied() {
        StringDictionary.Builder builder = new StringDictionary.Builder();
        byte[] backing = new byte[] { 'a', 'b', 'c' };
        BytesRef reused = new BytesRef(backing, 0, 1); // "a"
        builder.add(reused);
        reused.length = 2; // now "ab", same instance
        builder.add(reused);

        StringDictionary dictionary = builder.build();
        assertEquals(2, dictionary.size());
        assertEquals(new BytesRef("a"), dictionary.term(0));
        assertEquals(new BytesRef("ab"), dictionary.term(1));
    }

    public void testExactlyAtCapIsAccepted() {
        StringDictionary.Builder builder = new StringDictionary.Builder();
        for (int i = 0; i < StringDictionary.MAX_SIZE; i++) {
            builder.add(new BytesRef("term-" + i));
        }
        StringDictionary dictionary = builder.build();
        assertNotNull("a dictionary of exactly MAX_SIZE terms should be kept", dictionary);
        assertEquals(StringDictionary.MAX_SIZE, dictionary.size());
    }

    public void testOneOverCapOverflows() {
        StringDictionary.Builder builder = new StringDictionary.Builder();
        for (int i = 0; i <= StringDictionary.MAX_SIZE; i++) {
            builder.add(new BytesRef("term-" + i));
        }
        assertNull("one distinct term past MAX_SIZE should abandon the dictionary", builder.build());
    }

    /** Once overflowed the builder stays overflowed, even if every later value is already-seen. */
    public void testOverflowIsSticky() {
        StringDictionary.Builder builder = new StringDictionary.Builder();
        for (int i = 0; i <= StringDictionary.MAX_SIZE; i++) {
            builder.add(new BytesRef("term-" + i));
        }
        builder.add(new BytesRef("term-0"));
        assertNull(builder.build());
    }

    /** Repeats never overflow the cap, however many values arrive. */
    public void testRepeatsDoNotOverflow() {
        StringDictionary.Builder builder = new StringDictionary.Builder();
        for (int i = 0; i < StringDictionary.MAX_SIZE * 10; i++) {
            builder.add(new BytesRef("term-" + (i % 4)));
        }
        StringDictionary dictionary = builder.build();
        assertNotNull(dictionary);
        assertEquals(4, dictionary.size());
    }

    public void testWireFormatRoundTrip() throws IOException {
        StringDictionary.Builder builder = new StringDictionary.Builder();
        int distinct = between(1, StringDictionary.MAX_SIZE);
        BytesRef[] expected = new BytesRef[distinct];
        for (int i = 0; i < distinct; i++) {
            // Includes the empty term, which must survive as a zero length.
            expected[i] = new BytesRef(i == 0 ? "" : randomAlphaOfLength(between(1, 40)) + i);
            builder.add(expected[i]);
        }
        StringDictionary dictionary = builder.build();

        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        dictionary.writeTo(out);
        byte[] bytes = out.toArrayCopy();

        StringDictionary read = StringDictionary.readFrom(new ByteArrayDataInput(bytes));
        assertEquals(distinct, read.size());
        for (int i = 0; i < distinct; i++) {
            assertEquals("term " + i, expected[i], read.term(i));
        }
    }
}
