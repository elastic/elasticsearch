/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.TreeSet;

final class PlainSkipTestValues {

    private static final int[] LENGTHS = { 0, 1, 2, 5, 16, 31, 32, 33, 34, 40, 70 };
    private static final byte[] ALPHABET = { 0x00, 0x01, 0x41, 0x42, 0x7F, (byte) 0x80, (byte) 0xFE, (byte) 0xFF };

    private PlainSkipTestValues() {}

    static BytesRef randomValue() {
        return randomValueAfter(new byte[0]);
    }

    static BytesRef randomValueAfter(byte[] stem) {
        if (ESTestCase.between(0, 9) == 0) {
            final byte[] bytes = new byte[ESTestCase.between(1, 40)];
            Arrays.fill(bytes, (byte) 0xFF);
            return new BytesRef(bytes);
        }
        if (ESTestCase.between(0, 19) == 0) {
            return randomLongValue(stem);
        }
        final int length = stem.length + LENGTHS[ESTestCase.between(0, LENGTHS.length - 1)];
        final byte[] bytes = new byte[length];
        System.arraycopy(stem, 0, bytes, 0, stem.length);
        for (int i = stem.length; i < length; i++) {
            bytes[i] = ALPHABET[ESTestCase.between(0, ALPHABET.length - 1)];
        }
        return new BytesRef(bytes);
    }

    static BytesRef randomLongValue(byte[] stem) {
        final byte[] bytes = new byte[stem.length + ESTestCase.between(300, 4000)];
        System.arraycopy(stem, 0, bytes, 0, stem.length);
        for (int i = stem.length; i < bytes.length; i++) {
            bytes[i] = ALPHABET[ESTestCase.between(0, ALPHABET.length - 1)];
        }
        return new BytesRef(bytes);
    }

    static BytesRef maybeOffset(BytesRef value) {
        if (ESTestCase.randomBoolean()) {
            return value;
        }
        final int before = ESTestCase.between(1, 7);
        final byte[] padded = new byte[before + value.length + ESTestCase.between(0, 5)];
        Arrays.fill(padded, (byte) 0xFF);
        System.arraycopy(value.bytes, value.offset, padded, before, value.length);
        return new BytesRef(padded, before, value.length);
    }

    static byte[] randomStem(int length) {
        final byte[] stem = new byte[length];
        for (int i = 0; i < length; i++) {
            stem[i] = ALPHABET[ESTestCase.between(0, ALPHABET.length - 1)];
        }
        return stem;
    }

    static BytesRef[] randomPool(int size) {
        final byte[] stem = randomStem(ESTestCase.between(0, 40));
        final BytesRef[] pool = new BytesRef[size];
        for (int i = 0; i < size; i++) {
            pool[i] = ESTestCase.randomBoolean() ? randomValueAfter(stem) : randomValue();
        }
        return pool;
    }

    static BytesRef[] sortedPool(int size) {
        final byte[] stem = randomStem(ESTestCase.between(0, 40));
        final TreeSet<BytesRef> distinct = new TreeSet<>();
        while (distinct.size() < size) {
            distinct.add(BytesRef.deepCopyOf(ESTestCase.randomBoolean() ? randomValueAfter(stem) : randomValue()));
        }
        return distinct.toArray(new BytesRef[0]);
    }
}
