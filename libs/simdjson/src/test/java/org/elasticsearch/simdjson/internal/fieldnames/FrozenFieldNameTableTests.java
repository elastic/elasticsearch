/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdjson.internal.fieldnames;

import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.simdjson.SimdJsonTestSupport.toBytes;
import static org.elasticsearch.simdjson.SimdJsonTestSupport.toBytesAtOffset;

public class FrozenFieldNameTableTests extends ESTestCase {

    public void testLookupReturnsSameInstance() {
        FrozenFieldNameTable table = new FrozenFieldNameTable();
        FrozenFieldNameTable.Child child = table.makeChild();

        byte[] buf = toBytes("field_name");
        int len = "field_name".length();
        int hash = FieldNameHash.hashName(buf, 0, len);

        String inserted = child.insert(buf, 0, len, hash);
        String looked = child.lookup(buf, 0, len, hash);
        assertSame(inserted, looked);
    }

    public void testLookupBeforeInsertReturnsNull() {
        FrozenFieldNameTable table = new FrozenFieldNameTable();
        FrozenFieldNameTable.Child child = table.makeChild();

        byte[] buf = toBytes("unknown");
        int len = "unknown".length();
        int hash = FieldNameHash.hashName(buf, 0, len);

        assertNull(child.lookup(buf, 0, len, hash));
    }

    public void testInsertCreatesString() {
        FrozenFieldNameTable table = new FrozenFieldNameTable();
        FrozenFieldNameTable.Child child = table.makeChild();

        byte[] buf = toBytes("hello");
        int len = "hello".length();
        int hash = FieldNameHash.hashName(buf, 0, len);

        String result = child.insert(buf, 0, len, hash);
        assertEquals("hello", result);
    }

    public void testFreezeAndLookup() {
        FrozenFieldNameTable table = new FrozenFieldNameTable();
        FrozenFieldNameTable.Child child = table.makeChild();

        String[] names = { "alpha", "beta", "gamma", "delta", "epsilon" };
        for (String name : names) {
            byte[] buf = toBytes(name);
            int len = name.length();
            int hash = FieldNameHash.hashName(buf, 0, len);
            child.insert(buf, 0, len, hash);
        }

        child.freeze();

        for (String name : names) {
            byte[] buf = toBytes(name);
            int len = name.length();
            int hash = FieldNameHash.hashName(buf, 0, len);
            String result = child.lookup(buf, 0, len, hash);
            assertEquals(name, result);
        }
    }

    public void testFreezeIdempotent() {
        FrozenFieldNameTable table = new FrozenFieldNameTable();
        FrozenFieldNameTable.Child child = table.makeChild();

        byte[] buf = toBytes("test");
        int len = "test".length();
        int hash = FieldNameHash.hashName(buf, 0, len);
        child.insert(buf, 0, len, hash);

        child.freeze();
        child.freeze();
        assertTrue(child.isFrozen());
    }

    public void testIsFrozenBeforeAndAfter() {
        FrozenFieldNameTable table = new FrozenFieldNameTable();
        FrozenFieldNameTable.Child child = table.makeChild();

        assertFalse(child.isFrozen());

        byte[] buf = toBytes("x");
        int len = 1;
        int hash = FieldNameHash.hashName(buf, 0, len);
        child.insert(buf, 0, len, hash);

        assertFalse(child.isFrozen());
        child.freeze();
        assertTrue(child.isFrozen());
    }

    public void testLookupWithOffset() {
        FrozenFieldNameTable table = new FrozenFieldNameTable();
        FrozenFieldNameTable.Child child = table.makeChild();

        int offset = 10;
        String name = "offset_field";
        byte[] buf = toBytesAtOffset(name, offset);
        int len = name.length();
        int hash = FieldNameHash.hashName(buf, offset, len);

        String inserted = child.insert(buf, offset, len, hash);
        assertEquals(name, inserted);

        String looked = child.lookup(buf, offset, len, hash);
        assertSame(inserted, looked);
    }

    public void testManyFieldsScaleToHashTable() {
        FrozenFieldNameTable table = new FrozenFieldNameTable();
        FrozenFieldNameTable.Child child = table.makeChild();

        String[] names = new String[200];
        for (int i = 0; i < 200; i++) {
            names[i] = "field_" + i;
            byte[] buf = toBytes(names[i]);
            int len = names[i].length();
            int hash = FieldNameHash.hashName(buf, 0, len);
            child.insert(buf, 0, len, hash);
        }

        child.freeze();

        for (int i = 0; i < 200; i++) {
            byte[] buf = toBytes(names[i]);
            int len = names[i].length();
            int hash = FieldNameHash.hashName(buf, 0, len);
            String result = child.lookup(buf, 0, len, hash);
            assertEquals(names[i], result);
        }
    }

    public void testParentChildMerge() {
        FrozenFieldNameTable table = new FrozenFieldNameTable();

        FrozenFieldNameTable.Child child1 = table.makeChild();
        String[] names = { "one", "two", "three" };
        for (String name : names) {
            byte[] buf = toBytes(name);
            int len = name.length();
            int hash = FieldNameHash.hashName(buf, 0, len);
            child1.insert(buf, 0, len, hash);
        }
        child1.release();

        FrozenFieldNameTable.Child child2 = table.makeChild();
        for (String name : names) {
            byte[] buf = toBytes(name);
            int len = name.length();
            int hash = FieldNameHash.hashName(buf, 0, len);
            String result = child2.lookup(buf, 0, len, hash);
            assertEquals(name, result);
        }
    }

    public void testTwoChildrenMerge() {
        FrozenFieldNameTable table = new FrozenFieldNameTable();

        FrozenFieldNameTable.Child child1 = table.makeChild();
        byte[] bufAlpha = toBytes("alpha");
        int lenAlpha = "alpha".length();
        int hashAlpha = FieldNameHash.hashName(bufAlpha, 0, lenAlpha);
        child1.insert(bufAlpha, 0, lenAlpha, hashAlpha);
        child1.release();

        FrozenFieldNameTable.Child child2 = table.makeChild();
        byte[] bufBeta = toBytes("beta");
        int lenBeta = "beta".length();
        int hashBeta = FieldNameHash.hashName(bufBeta, 0, lenBeta);
        child2.insert(bufBeta, 0, lenBeta, hashBeta);
        child2.release();

        FrozenFieldNameTable.Child child3 = table.makeChild();
        String resultAlpha = child3.lookup(bufAlpha, 0, lenAlpha, hashAlpha);
        assertEquals("alpha", resultAlpha);
    }

    public void testReleaseFreezesIfDirty() {
        FrozenFieldNameTable table = new FrozenFieldNameTable();
        FrozenFieldNameTable.Child child = table.makeChild();

        byte[] buf = toBytes("dirty_field");
        int len = "dirty_field".length();
        int hash = FieldNameHash.hashName(buf, 0, len);
        child.insert(buf, 0, len, hash);

        assertFalse(child.isFrozen());
        child.release();
        assertTrue(child.isFrozen());
    }

    public void testReleaseRefreshesIfNotDirty() {
        FrozenFieldNameTable table = new FrozenFieldNameTable();

        FrozenFieldNameTable.Child child2 = table.makeChild();
        assertFalse(child2.isFrozen());

        FrozenFieldNameTable.Child child1 = table.makeChild();
        byte[] buf = toBytes("shared");
        int len = "shared".length();
        int hash = FieldNameHash.hashName(buf, 0, len);
        child1.insert(buf, 0, len, hash);
        child1.release();

        assertFalse(child2.isFrozen());
        child2.release();
        assertTrue(child2.isFrozen());

        String result = child2.lookup(buf, 0, len, hash);
        assertEquals("shared", result);
    }

    public void testShortFieldNames() {
        FrozenFieldNameTable table = new FrozenFieldNameTable();
        FrozenFieldNameTable.Child child = table.makeChild();

        for (int nameLen = 1; nameLen <= 8; nameLen++) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < nameLen; i++) {
                sb.append((char) ('a' + (i % 26)));
            }
            String name = sb.toString();
            byte[] buf = toBytes(name);
            int hash = FieldNameHash.hashName(buf, 0, nameLen);
            child.insert(buf, 0, nameLen, hash);
        }

        child.freeze();

        for (int nameLen = 1; nameLen <= 8; nameLen++) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < nameLen; i++) {
                sb.append((char) ('a' + (i % 26)));
            }
            String name = sb.toString();
            byte[] buf = toBytes(name);
            int hash = FieldNameHash.hashName(buf, 0, nameLen);
            String result = child.lookup(buf, 0, nameLen, hash);
            assertEquals(name, result);
        }
    }

    public void testLongFieldNames() {
        FrozenFieldNameTable table = new FrozenFieldNameTable();
        FrozenFieldNameTable.Child child = table.makeChild();

        String[] names = { "this_is_a_long_name", "another_long_field_name_here", "field_name_exceeding_8_bytes" };
        for (String name : names) {
            assertTrue(name.length() > 8);
            byte[] buf = toBytes(name);
            int len = name.length();
            int hash = FieldNameHash.hashName(buf, 0, len);
            child.insert(buf, 0, len, hash);
        }

        child.freeze();

        for (String name : names) {
            byte[] buf = toBytes(name);
            int len = name.length();
            int hash = FieldNameHash.hashName(buf, 0, len);
            String result = child.lookup(buf, 0, len, hash);
            assertEquals(name, result);
        }
    }

    public void testEmptyFieldName() {
        FrozenFieldNameTable table = new FrozenFieldNameTable();
        FrozenFieldNameTable.Child child = table.makeChild();

        byte[] buf = toBytes("");
        int hash = FieldNameHash.hashName(buf, 0, 0);
        String inserted = child.insert(buf, 0, 0, hash);
        assertEquals("", inserted);

        String looked = child.lookup(buf, 0, 0, hash);
        assertSame(inserted, looked);
    }

    public void testInsertAfterFreezeStillWorks() {
        FrozenFieldNameTable table = new FrozenFieldNameTable();
        FrozenFieldNameTable.Child child = table.makeChild();

        byte[] buf1 = toBytes("before");
        int len1 = "before".length();
        int hash1 = FieldNameHash.hashName(buf1, 0, len1);
        child.insert(buf1, 0, len1, hash1);

        child.freeze();

        byte[] buf2 = toBytes("after");
        int len2 = "after".length();
        int hash2 = FieldNameHash.hashName(buf2, 0, len2);
        String result = child.insert(buf2, 0, len2, hash2);
        assertEquals("after", result);
    }

    public void testFieldNamesWithSamePrefix8() {
        FrozenFieldNameTable table = new FrozenFieldNameTable();
        FrozenFieldNameTable.Child child = table.makeChild();

        String name1 = "abcdefgh_suffix1";
        String name2 = "abcdefgh_suffix2";
        assertEquals(name1.substring(0, 8), name2.substring(0, 8));

        byte[] buf1 = toBytes(name1);
        int len1 = name1.length();
        int hash1 = FieldNameHash.hashName(buf1, 0, len1);
        child.insert(buf1, 0, len1, hash1);

        byte[] buf2 = toBytes(name2);
        int len2 = name2.length();
        int hash2 = FieldNameHash.hashName(buf2, 0, len2);
        child.insert(buf2, 0, len2, hash2);

        child.freeze();

        String result1 = child.lookup(buf1, 0, len1, hash1);
        String result2 = child.lookup(buf2, 0, len2, hash2);
        assertEquals(name1, result1);
        assertEquals(name2, result2);
        assertNotSame(result1, result2);
    }

    public void testFieldNameCachingAcrossDocs() {
        FrozenFieldNameTable table = new FrozenFieldNameTable();

        FrozenFieldNameTable.Child child1 = table.makeChild();
        String[] docFields = { "timestamp", "message", "level", "source" };
        for (String name : docFields) {
            byte[] buf = toBytes(name);
            int len = name.length();
            int hash = FieldNameHash.hashName(buf, 0, len);
            child1.insert(buf, 0, len, hash);
        }
        child1.freeze();
        child1.release();

        FrozenFieldNameTable.Child child2 = table.makeChild();
        assertTrue(child2.isFrozen());
        for (String name : docFields) {
            byte[] buf = toBytes(name);
            int len = name.length();
            int hash = FieldNameHash.hashName(buf, 0, len);
            String result = child2.lookup(buf, 0, len, hash);
            assertEquals(name, result);
        }
    }
}
