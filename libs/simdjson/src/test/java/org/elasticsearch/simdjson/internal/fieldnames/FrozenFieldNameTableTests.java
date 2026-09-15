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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.simdjson.SimdJsonTestCase.toBytes;
import static org.elasticsearch.simdjson.SimdJsonTestCase.toBytesAtOffset;

// Unit tests for FrozenFieldNameTable insert/lookup, freeze, and parent-child merge.
//
// Lifecycle (see FrozenFieldNameTable):
// - makeChild(): thread-local Child; starts learning if parent has no shared table, else inherits parent's Frozen.
// - insert/lookup: learning phase appends names; frozen phase uses a hash table (insert no longer learns).
// - freeze(): build hash table on this child and try parent.mergeChild (compareAndSet — first wins).
// - release(): freeze if still learning and dirty; else adopt parent shared table if clean; no-op if already frozen.
public class FrozenFieldNameTableTests extends ESTestCase {

    // ---- Basic insert and lookup ----

    // lookup returns the same canonical String instance that insert created.
    public void testLookupReturnsSameInstance() {
        FrozenFieldNameTable.Child child = new FrozenFieldNameTable().makeChild();
        String inserted = insertName(child, "field_name");
        String looked = lookupName(child, "field_name");
        assertSame("lookup must return the same String instance as insert", inserted, looked);
    }

    // Same-instance invariant holds across random field names and lengths.
    public void testLookupReturnsSameInstanceForRandomNames() {
        FrozenFieldNameTable.Child child = new FrozenFieldNameTable().makeChild();
        for (String name : randomDistinctFieldNames(100)) {
            String inserted = insertName(child, name);
            assertSame("lookup must return the same String instance for: " + name, inserted, lookupName(child, name));
        }
    }

    // lookup returns null for a name that was never inserted.
    public void testLookupBeforeInsertReturnsNull() {
        FrozenFieldNameTable.Child child = new FrozenFieldNameTable().makeChild();
        assertNull("lookup before insert must return null", lookupName(child, "unknown"));
    }

    // Unknown random names remain null until inserted.
    public void testLookupBeforeInsertReturnsNullForRandomNames() {
        FrozenFieldNameTable.Child child = new FrozenFieldNameTable().makeChild();
        insertName(child, "present");
        for (int i = 0; i < 100; i++) {
            String missing = randomFieldName();
            if ("present".equals(missing)) {
                continue;
            }
            assertNull("lookup before insert must return null for: " + missing, lookupName(child, missing));
        }
    }

    // insert materializes the field name bytes into a new String.
    public void testInsertCreatesStringFromBufferBytes() {
        FrozenFieldNameTable.Child child = new FrozenFieldNameTable().makeChild();
        for (int i = 0; i < 50; i++) {
            String name = randomFieldName();
            byte[] buf = toBytes(name);
            int hash = FieldNameHash.hashName(buf, 0, buf.length);
            String result = child.insert(buf, 0, buf.length, hash);
            assertEquals("insert must decode field name bytes into a String: " + name, name, result);
        }
    }

    // ---- Freeze ----

    // All names inserted before freeze remain lookup-able after freeze.
    public void testFreezeAndLookup() {
        FrozenFieldNameTable.Child child = new FrozenFieldNameTable().makeChild();
        String[] names = { "alpha", "beta", "gamma", "delta", "epsilon" };
        for (String name : names) {
            insertName(child, name);
        }
        child.freeze();
        for (String name : names) {
            assertEquals("frozen table must still resolve inserted name: " + name, name, lookupName(child, name));
        }
    }

    // Random field names survive freeze and remain lookup-able.
    public void testFreezeAndLookupRandomNames() {
        FrozenFieldNameTable.Child child = new FrozenFieldNameTable().makeChild();
        List<String> names = randomDistinctFieldNames(80);
        for (String name : names) {
            insertName(child, name);
        }
        child.freeze();
        for (String name : names) {
            assertEquals("frozen table must resolve random name: " + name, name, lookupName(child, name));
        }
    }

    // freeze may be called more than once without changing behavior.
    public void testFreezeIdempotent() {
        FrozenFieldNameTable.Child child = new FrozenFieldNameTable().makeChild();
        insertName(child, "test");
        child.freeze();
        child.freeze();
        assertTrue("child must remain frozen after repeated freeze", child.isFrozen());
    }

    // isFrozen is false while learning and true only after freeze (or release).
    public void testIsFrozenBeforeAndAfter() {
        FrozenFieldNameTable.Child child = new FrozenFieldNameTable().makeChild();
        assertFalse("new child must not start frozen", child.isFrozen());
        insertName(child, "x");
        assertFalse("child with pending inserts must not be frozen yet", child.isFrozen());
        child.freeze();
        assertTrue("child must be frozen after freeze()", child.isFrozen());
    }

    // insert and lookup honor a non-zero buffer offset.
    public void testLookupWithOffset() {
        FrozenFieldNameTable.Child child = new FrozenFieldNameTable().makeChild();
        for (String name : randomDistinctFieldNames(50)) {
            int offset = between(1, 32);
            byte[] buf = toBytesAtOffset(name, offset);
            int hash = FieldNameHash.hashName(buf, offset, name.length());
            String inserted = child.insert(buf, offset, name.length(), hash);
            assertEquals("insert with offset must materialize the field name: " + name, name, inserted);
            assertSame(
                "lookup with offset must return the inserted instance: " + name,
                inserted,
                child.lookup(buf, offset, name.length(), hash)
            );
        }
    }

    // Many distinct fields still resolve correctly after freeze.
    public void testManyFieldsScaleToHashTable() {
        FrozenFieldNameTable.Child child = new FrozenFieldNameTable().makeChild();
        List<String> names = randomDistinctFieldNames(200);
        for (String name : names) {
            insertName(child, name);
        }
        child.freeze();
        for (String name : names) {
            assertEquals("large frozen table must resolve: " + name, name, lookupName(child, name));
        }
    }

    // ---- Parent-child merge ----

    // Names learned by child1 are visible to child2 after release merges into the parent.
    public void testParentChildMerge() {
        FrozenFieldNameTable table = new FrozenFieldNameTable();
        FrozenFieldNameTable.Child child1 = table.makeChild();
        String[] names = { "one", "two", "three" };
        for (String name : names) {
            insertName(child1, name);
        }
        child1.release();

        FrozenFieldNameTable.Child child2 = table.makeChild();
        for (String name : names) {
            assertEquals("merged parent cache must resolve name from prior child: " + name, name, lookupName(child2, name));
        }
    }

    // Parent merge works for a batch of random field names from the first child.
    public void testParentChildMergeWithRandomNames() {
        FrozenFieldNameTable table = new FrozenFieldNameTable();
        FrozenFieldNameTable.Child child1 = table.makeChild();
        List<String> names = randomDistinctFieldNames(60);
        for (String name : names) {
            insertName(child1, name);
        }
        child1.release();

        FrozenFieldNameTable.Child child2 = table.makeChild();
        for (String name : names) {
            assertEquals("merged parent cache must resolve random name: " + name, name, lookupName(child2, name));
        }
    }

    // Only the first released child publishes its frozen table to the parent (compareAndSet).
    // A later child inherits that table; insert on an inherited-frozen child does not learn new names.
    public void testTwoChildrenMerge() {
        FrozenFieldNameTable table = new FrozenFieldNameTable();
        FrozenFieldNameTable.Child child1 = table.makeChild();
        insertName(child1, "alpha");
        child1.release();

        FrozenFieldNameTable.Child child2 = table.makeChild();
        insertName(child2, "beta");
        assertNull("insert on inherited-frozen child must not cache new names", lookupName(child2, "beta"));
        child2.release();

        FrozenFieldNameTable.Child child3 = table.makeChild();
        assertEquals("successor child must see the first released child's field via parent", "alpha", lookupName(child3, "alpha"));
        assertNull("second child's field must not be merged after first child wins parent publish", lookupName(child3, "beta"));
    }

    // ---- Release lifecycle ----

    // release() on a dirty child auto-freezes before merging into the parent.
    public void testReleaseFreezesIfDirty() {
        FrozenFieldNameTable.Child child = new FrozenFieldNameTable().makeChild();
        insertName(child, "dirty_field");
        assertFalse("dirty child must not be frozen before release", child.isFrozen());
        child.release();
        assertTrue("release on dirty child must freeze before merge", child.isFrozen());
    }

    // A clean child refreshes from the parent on release without local inserts.
    public void testReleaseRefreshesIfNotDirty() {
        FrozenFieldNameTable table = new FrozenFieldNameTable();
        FrozenFieldNameTable.Child child2 = table.makeChild();
        assertFalse("fresh child must not start frozen", child2.isFrozen());

        FrozenFieldNameTable.Child child1 = table.makeChild();
        insertName(child1, "shared");
        child1.release();

        assertFalse("child2 must stay unfrozen until release", child2.isFrozen());
        child2.release();
        assertTrue("child2 must be frozen after release", child2.isFrozen());
        assertEquals("child2 must refresh parent's field on release", "shared", lookupName(child2, "shared"));
    }

    // ---- Field name shapes ----

    // Insert and lookup succeed for empty, short, and long field names.
    public void testInsertAndLookupFieldNamesOfVariousLengths() {
        FrozenFieldNameTable.Child child = new FrozenFieldNameTable().makeChild();
        List<String> names = new ArrayList<>();
        names.add("");
        for (int len = 1; len <= 40; len++) {
            names.add(randomAlphaOfLength(len));
        }
        addRandomPrefix8Pair(names);

        for (String name : names) {
            insertName(child, name);
        }
        child.freeze();

        for (String name : names) {
            assertEquals("frozen table must resolve name of length " + name.length() + ": " + name, name, lookupName(child, name));
        }
    }

    // Same 8-byte prefix with different suffixes must map to distinct Strings.
    public void testFieldNamesWithSamePrefix8() {
        FrozenFieldNameTable.Child child = new FrozenFieldNameTable().makeChild();
        for (int i = 0; i < 20; i++) {
            String prefix = randomAlphaOfLength(8);
            String name1 = prefix + randomAlphaOfLengthBetween(4, 16);
            String name2 = prefix + randomAlphaOfLengthBetween(4, 16);
            if (name1.equals(name2)) {
                name2 = name2 + "x";
            }
            assertEquals("test names must share the same 8-byte prefix", prefix, name1.substring(0, 8));
            assertEquals("test names must share the same 8-byte prefix", prefix, name2.substring(0, 8));

            insertName(child, name1);
            insertName(child, name2);
            child.freeze();

            String result1 = lookupName(child, name1);
            String result2 = lookupName(child, name2);
            assertEquals("lookup must return first full name", name1, result1);
            assertEquals("lookup must return second full name", name2, result2);
            assertNotSame("names with same prefix8 must still be distinct instances", result1, result2);

            child = new FrozenFieldNameTable().makeChild();
        }
    }

    // insert after freeze still works (lazy growth of the frozen table).
    public void testInsertAfterFreezeStillWorks() {
        FrozenFieldNameTable.Child child = new FrozenFieldNameTable().makeChild();
        insertName(child, "before");
        child.freeze();
        for (String name : randomDistinctFieldNames(20)) {
            assertEquals("insert after freeze must accept new names: " + name, name, insertName(child, name));
        }
    }

    public void testLookupFieldReturnsOrdinalAfterFreeze() {
        FrozenFieldNameTable table = new FrozenFieldNameTable();
        FrozenFieldNameTable.Child child = table.makeChild();

        byte[] buf = toBytes("timestamp");
        int len = "timestamp".length();
        int hash = FieldNameHash.hashName(buf, 0, len);
        child.insert(buf, 0, len, hash);
        child.freeze();

        ResolvedFieldName resolved = child.lookupField(buf, 0, len, hash, FieldNameHash.readPrefix8(buf, 0, len));
        assertNotNull(resolved);
        assertEquals("timestamp", resolved.name());
        assertEquals(0, resolved.ordinal());
    }

    // Multi-doc pattern: child1 learns and releases; child2 starts frozen with parent cache.
    public void testFieldNameCachingAcrossDocs() {
        FrozenFieldNameTable table = new FrozenFieldNameTable();
        FrozenFieldNameTable.Child child1 = table.makeChild();
        List<String> docFields = randomDistinctFieldNames(20);
        for (String name : docFields) {
            insertName(child1, name);
        }
        child1.freeze();
        child1.release();

        FrozenFieldNameTable.Child child2 = table.makeChild();
        assertTrue("next doc child must start frozen from parent cache", child2.isFrozen());
        for (String name : docFields) {
            assertEquals("cached field must resolve on next doc: " + name, name, lookupName(child2, name));
        }
    }

    // End-to-end: learn random names, freeze, lookup, release, and resolve from a sibling child.
    public void testRandomNamesRoundTripThroughFreezeAndRelease() {
        FrozenFieldNameTable table = new FrozenFieldNameTable();
        FrozenFieldNameTable.Child learner = table.makeChild();
        List<String> names = randomDistinctFieldNames(100);
        for (String name : names) {
            String inserted = insertName(learner, name);
            assertSame("pre-freeze lookup must return inserted instance: " + name, inserted, lookupName(learner, name));
        }
        learner.freeze();
        for (String name : names) {
            assertEquals("post-freeze lookup must resolve: " + name, name, lookupName(learner, name));
        }
        learner.release();

        FrozenFieldNameTable.Child successor = table.makeChild();
        for (String name : names) {
            assertEquals("successor child must resolve released name: " + name, name, lookupName(successor, name));
        }
    }

    private static String randomFieldName() {
        return randomAlphaOfLengthBetween(0, 32);
    }

    private static List<String> randomDistinctFieldNames(int count) {
        Set<String> unique = new HashSet<>();
        while (unique.size() < count) {
            unique.add(randomAlphaOfLengthBetween(0, 24) + "_" + unique.size());
        }
        return List.copyOf(unique);
    }

    private static void addRandomPrefix8Pair(List<String> names) {
        String prefix = randomAlphaOfLength(8);
        String name1 = prefix + randomAlphaOfLengthBetween(4, 16);
        String name2 = prefix + randomAlphaOfLengthBetween(4, 16);
        if (name1.equals(name2)) {
            name2 = name2 + "z";
        }
        names.add(name1);
        names.add(name2);
    }

    private static String insertName(FrozenFieldNameTable.Child child, String name) {
        byte[] buf = toBytes(name);
        int hash = FieldNameHash.hashName(buf, 0, buf.length);
        return child.insert(buf, 0, buf.length, hash);
    }

    private static String lookupName(FrozenFieldNameTable.Child child, String name) {
        byte[] buf = toBytes(name);
        int hash = FieldNameHash.hashName(buf, 0, buf.length);
        return child.lookup(buf, 0, buf.length, hash);
    }

    public void testDeferredFreezeWaitsForStableSchema() {
        FrozenFieldNameTable.Child child = new FrozenFieldNameTable().makeChild();

        String[] variantA = { "type", "id", "ts", "val", "label", "active", "count" };
        String[] variantB = { "type", "uid", "score", "tags", "region", "retries" };

        child.beginDocument();
        learnFields(child, variantA);
        child.maybeFreezeAfterDocument();
        assertFalse(child.isFrozen());

        child.beginDocument();
        learnFields(child, variantB);
        child.maybeFreezeAfterDocument();
        assertFalse(child.isFrozen());

        child.beginDocument();
        lookupOnly(child, variantA);
        child.maybeFreezeAfterDocument();
        assertTrue(child.isFrozen());

        for (String name : variantA) {
            byte[] buf = toBytes(name);
            int len = name.length();
            int hash = FieldNameHash.hashName(buf, 0, len);
            ResolvedFieldName resolved = child.lookupField(buf, 0, len, hash, FieldNameHash.readPrefix8(buf, 0, len));
            assertNotNull(resolved);
            assertEquals(name, resolved.name());
            assertTrue(resolved.ordinal() >= 0);
        }
        for (String name : variantB) {
            byte[] buf = toBytes(name);
            int len = name.length();
            int hash = FieldNameHash.hashName(buf, 0, len);
            ResolvedFieldName resolved = child.lookupField(buf, 0, len, hash, FieldNameHash.readPrefix8(buf, 0, len));
            assertNotNull(resolved);
            assertEquals(name, resolved.name());
            assertTrue(resolved.ordinal() >= 0);
        }
    }

    public void testSmallTableUsesOrdinalsWithoutDirectMap() {
        FrozenFieldNameTable.Child child = new FrozenFieldNameTable().makeChild();
        String[] names = { "alpha", "beta", "gamma", "delta", "epsilon" };
        for (String name : names) {
            byte[] buf = toBytes(name);
            int len = name.length();
            int hash = FieldNameHash.hashName(buf, 0, len);
            child.insert(buf, 0, len, hash);
        }
        child.freeze();
        assertNull(child.frozenDirectOrdinals());

        for (int i = 0; i < names.length; i++) {
            byte[] buf = toBytes(names[i]);
            int len = names[i].length();
            int hash = FieldNameHash.hashName(buf, 0, len);
            ResolvedFieldName resolved = child.lookupField(buf, 0, len, hash, FieldNameHash.readPrefix8(buf, 0, len));
            assertEquals(i, resolved.ordinal());
        }
    }

    public void testLargeTableBuildsDirectMap() {
        FrozenFieldNameTable.Child child = new FrozenFieldNameTable().makeChild();
        for (int i = 0; i < FrozenFieldNameTable.DIRECT_MAP_MIN_FIELDS; i++) {
            String name = "field_" + i;
            byte[] buf = toBytes(name);
            int len = name.length();
            int hash = FieldNameHash.hashName(buf, 0, len);
            child.insert(buf, 0, len, hash);
        }
        child.freeze();
        assertNotNull(child.frozenDirectOrdinals());
    }

    /**
     * ClickBench has three {@code (prefix8, len)} collision groups where full byte comparison
     * is still required after hash + prefix match. All groups must resolve to distinct names.
     */
    public void testClickBenchPrefixLenCollisionGroups() {
        FrozenFieldNameTable.Child child = new FrozenFieldNameTable().makeChild();

        String[][] groups = {
            { "ResolutionWidth", "ResolutionDepth" },
            { "UserAgentMajor", "UserAgentMinor" },
            { "SilverlightVersion1", "SilverlightVersion2", "SilverlightVersion3", "SilverlightVersion4" } };

        for (String[] group : groups) {
            assertSamePrefixAndLength(group);
            for (String name : group) {
                byte[] buf = toBytes(name);
                int len = name.length();
                int hash = FieldNameHash.hashName(buf, 0, len);
                child.insert(buf, 0, len, hash);
            }
        }

        child.freeze();

        for (String[] group : groups) {
            for (String name : group) {
                byte[] buf = toBytes(name);
                int len = name.length();
                int hash = FieldNameHash.hashName(buf, 0, len);
                long pfx = FieldNameHash.readPrefix8(buf, 0, len);
                ResolvedFieldName resolved = child.lookupField(buf, 0, len, hash, pfx);
                assertNotNull(name, resolved);
                assertEquals(name, resolved.name());
                assertTrue(name, resolved.ordinal() >= 0);
            }
        }
    }

    /**
     * Fused {@link FieldNameHash#scanFieldName} + {@link FieldNameLookup#lookupField} must agree
     * with {@link FieldNameHash#hashName} for ClickBench collision pairs.
     */
    public void testScanFieldNameLookupFieldForCollisionPairs() {
        FrozenFieldNameTable.Child child = new FrozenFieldNameTable().makeChild();

        String[] collisionNames = {
            "ResolutionWidth",
            "ResolutionDepth",
            "UserAgentMajor",
            "UserAgentMinor",
            "SilverlightVersion1",
            "SilverlightVersion4" };

        for (String name : collisionNames) {
            byte[] buf = toBytes(name);
            int len = name.length();
            child.insert(buf, 0, len, FieldNameHash.hashName(buf, 0, len));
        }
        child.freeze();

        for (String name : collisionNames) {
            ResolvedFieldName resolved = lookupViaScan(child, name);
            assertEquals(name, resolved.name());
            assertTrue(resolved.ordinal() >= 0);
        }
    }

    /**
     * When two names share {@code (prefix8, len)}, the direct-mapped bucket is marked colliding
     * and both names still resolve correctly through the probe loop.
     */
    public void testDirectMapMarksCollidingPrefixLenBucket() {
        FrozenFieldNameTable.Child child = new FrozenFieldNameTable().makeChild();

        String width = "ResolutionWidth";
        String depth = "ResolutionDepth";
        for (int i = 0; i < FrozenFieldNameTable.DIRECT_MAP_MIN_FIELDS - 2; i++) {
            insertName(child, "field_" + i);
        }
        insertName(child, width);
        insertName(child, depth);
        child.freeze();

        int[] directOrdinals = child.frozenDirectOrdinals();
        assertNotNull(directOrdinals);

        byte[] widthBuf = toBytes(width);
        int widthLen = width.length();
        long widthPfx = FieldNameHash.readPrefix8(widthBuf, 0, widthLen);
        int directIdx = FrozenFieldNameTable.directIndex(widthPfx, widthLen, directOrdinals.length);
        assertEquals(-2, directOrdinals[directIdx]);

        ResolvedFieldName resolvedWidth = lookupViaScan(child, width);
        ResolvedFieldName resolvedDepth = lookupViaScan(child, depth);
        assertEquals(width, resolvedWidth.name());
        assertEquals(depth, resolvedDepth.name());
        assertNotEquals(resolvedWidth.ordinal(), resolvedDepth.ordinal());
    }

    /**
     * Repeating the same document shape before all variants are seen freezes early. Unknown
     * names after freeze still parse correctly but carry {@code ordinal = -1}.
     */
    public void testPrematureDeferredFreezeStillCorrect() {
        FrozenFieldNameTable.Child child = new FrozenFieldNameTable().makeChild();

        String[] variantA = { "type", "id", "ts" };
        String[] variantB = { "type", "uid", "score" };

        child.beginDocument();
        learnFields(child, variantA);
        child.maybeFreezeAfterDocument();
        assertFalse(child.isFrozen());

        child.beginDocument();
        lookupOnly(child, variantA);
        child.maybeFreezeAfterDocument();
        assertTrue(child.isFrozen());
        assertNull(child.frozenDirectOrdinals());

        lookupViaScan(child, "type");
        lookupViaScan(child, "id");

        byte[] uidBuf = toBytes("uid");
        int uidLen = "uid".length();
        int uidHash = FieldNameHash.hashName(uidBuf, 0, uidLen);
        assertNull(child.lookup(uidBuf, 0, uidLen, uidHash));

        ResolvedFieldName inserted = child.insertField(uidBuf, 0, uidLen, uidHash);
        assertEquals("uid", inserted.name());
        assertEquals(-1, inserted.ordinal());

        child.beginDocument();
        learnFields(child, variantB);
        child.maybeFreezeAfterDocument();
        assertTrue(child.isFrozen());

        byte[] scoreBuf = toBytes("score");
        int scoreLen = "score".length();
        int scoreHash = FieldNameHash.hashName(scoreBuf, 0, scoreLen);
        assertNull(child.lookup(scoreBuf, 0, scoreLen, scoreHash));
        ResolvedFieldName scoreInserted = child.insertField(scoreBuf, 0, scoreLen, scoreHash);
        assertEquals("score", scoreInserted.name());
        assertEquals(-1, scoreInserted.ordinal());
    }

    private static ResolvedFieldName lookupViaScan(FrozenFieldNameTable.Child child, String name) {
        byte[] nameBytes = toBytes(name);
        byte[] buf = Arrays.copyOf(nameBytes, nameBytes.length + 1);
        buf[nameBytes.length] = '"';
        FieldNameHash.FieldNameScan scan = FieldNameHash.scanFieldName(buf, 0);
        assertNotNull(name, scan);
        assertEquals(name.length(), scan.len());
        assertEquals(FieldNameHash.hashName(nameBytes, 0, name.length()), scan.hash());
        ResolvedFieldName resolved = child.lookupField(buf, 0, scan.len(), scan.hash(), scan.prefix8());
        assertNotNull(name, resolved);
        return resolved;
    }

    private static void assertSamePrefixAndLength(String[] names) {
        byte[] first = toBytes(names[0]);
        int len = names[0].length();
        long prefix8 = FieldNameHash.readPrefix8(first, 0, len);
        for (int i = 1; i < names.length; i++) {
            byte[] buf = toBytes(names[i]);
            assertEquals(len, names[i].length());
            assertEquals(prefix8, FieldNameHash.readPrefix8(buf, 0, len));
        }
    }

    private static void learnFields(FrozenFieldNameTable.Child child, String[] names) {
        for (String name : names) {
            byte[] buf = toBytes(name);
            int len = name.length();
            int hash = FieldNameHash.hashName(buf, 0, len);
            if (child.lookup(buf, 0, len, hash) == null) {
                child.insert(buf, 0, len, hash);
            }
        }
    }

    private static void lookupOnly(FrozenFieldNameTable.Child child, String[] names) {
        for (String name : names) {
            byte[] buf = toBytes(name);
            int len = name.length();
            int hash = FieldNameHash.hashName(buf, 0, len);
            assertNotNull(child.lookup(buf, 0, len, hash));
        }
    }
}
