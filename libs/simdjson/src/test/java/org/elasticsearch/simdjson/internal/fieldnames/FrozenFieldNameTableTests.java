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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;

import static org.elasticsearch.simdjson.SimdJsonTestCase.toBytes;
import static org.elasticsearch.simdjson.SimdJsonTestCase.toBytesAtOffset;

// Unit tests for FrozenFieldNameTable insert/lookup, freeze, and parent-child merge.
//
// Lifecycle (see FrozenFieldNameTable):
// - makeChild(): thread-local Child; starts learning if parent has no shared table, else inherits parent's Frozen.
// - insert/lookup: learning phase appends names; frozen phase uses a hash table (insert no longer learns).
// - freeze(): build hash table on this child and try parent.mergeChild (compareAndSet — first wins).
// - release(): freeze if still learning and dirty; else adopt parent shared table if clean;
//   otherwise publish overflow and re-sync frozen from the parent's current shared table.
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

        // child2 starts frozen on child1's table, so "beta" is new to it and lands in the overflow
        // buffer, which lookup consults so the name is still canonicalized locally.
        FrozenFieldNameTable.Child child2 = table.makeChild();
        insertName(child2, "beta");
        assertEquals("inherited-frozen child must canonicalize a new name via overflow", "beta", lookupName(child2, "beta"));
        child2.release();

        FrozenFieldNameTable.Child child3 = table.makeChild();
        assertEquals("successor child must see the first released child's field", "alpha", lookupName(child3, "alpha"));
        assertEquals("successor child must see the second released child's field", "beta", lookupName(child3, "beta"));
    }

    // ---- Merging names learned after freeze ----

    // A frozen child records names its table lacks and hands them to the parent on release.
    public void testOverflowNamesArePublishedOnRelease() {
        FrozenFieldNameTable table = new FrozenFieldNameTable();
        FrozenFieldNameTable.Child first = table.makeChild();
        insertName(first, "known");
        first.release();
        assertEquals(1, table.sharedNameCount());

        FrozenFieldNameTable.Child second = table.makeChild();
        insertName(second, "learned_later");
        assertEquals("post-freeze miss must be recorded", 1, second.overflowCount());
        assertEquals("publication must wait for release", 1, table.sharedNameCount());

        second.release();
        assertEquals("release must merge the overflow name into the shared table", 2, table.sharedNameCount());
    }

    // Publication must not disturb children already handed out, only ones created afterwards.
    public void testPublishingDoesNotDisturbExistingChildren() {
        FrozenFieldNameTable table = new FrozenFieldNameTable();
        FrozenFieldNameTable.Child seed = table.makeChild();
        insertName(seed, "known");
        seed.release();

        FrozenFieldNameTable.Child bystander = table.makeChild();
        assertEquals("known", lookupName(bystander, "known"));

        FrozenFieldNameTable.Child learner = table.makeChild();
        insertName(learner, "brand_new");
        learner.release();

        assertNull("an existing child must not see a name published after it was created", lookupName(bystander, "brand_new"));
        assertEquals("an existing child must keep the names it already had", "known", lookupName(bystander, "known"));
        assertEquals("a child created after publication must see the new name", "brand_new", lookupName(table.makeChild(), "brand_new"));
    }

    // Names already shared keep their String instance, so publication never changes identity.
    public void testMergePreservesExistingNameIdentity() {
        FrozenFieldNameTable table = new FrozenFieldNameTable();
        FrozenFieldNameTable.Child seed = table.makeChild();
        insertName(seed, "stable");
        seed.release();

        String beforeMerge = lookupName(table.makeChild(), "stable");

        FrozenFieldNameTable.Child learner = table.makeChild();
        insertName(learner, "addition");
        learner.release();

        assertSame("rebuilding the table must reuse the existing String instance", beforeMerge, lookupName(table.makeChild(), "stable"));
    }

    // Republishing is a no-op: a release with nothing new must not rebuild the shared table.
    public void testRepeatedReleaseDoesNotRepublish() {
        FrozenFieldNameTable table = new FrozenFieldNameTable();
        FrozenFieldNameTable.Child seed = table.makeChild();
        insertName(seed, "known");
        seed.release();

        FrozenFieldNameTable.Child learner = table.makeChild();
        insertName(learner, "extra");
        learner.release();

        String afterFirstRelease = lookupName(table.makeChild(), "extra");
        assertEquals(2, table.sharedNameCount());

        learner.release();
        learner.release();
        assertEquals("a release with nothing new must not change the shared table", 2, table.sharedNameCount());
        assertSame("a no-op release must not rebuild the table", afterFirstRelease, lookupName(table.makeChild(), "extra"));
    }

    // The overflow buffer is bounded, so a child seeing many unknown names stops recording.
    public void testOverflowRecordingIsBounded() {
        FrozenFieldNameTable table = new FrozenFieldNameTable();
        FrozenFieldNameTable.Child seed = table.makeChild();
        insertName(seed, "known");
        seed.release();

        FrozenFieldNameTable.Child learner = table.makeChild();
        int beyondCap = FrozenFieldNameTable.Child.MAX_OVERFLOW + randomIntBetween(1, 50);
        for (int i = 0; i < beyondCap; i++) {
            assertEquals(
                "insert must still canonicalize past the cap",
                "high_cardinality_" + i,
                insertName(learner, "high_cardinality_" + i)
            );
        }

        assertEquals("recording must stop at the cap", FrozenFieldNameTable.Child.MAX_OVERFLOW, learner.overflowCount());
        learner.release();
        assertEquals("only recorded names may be published", 1 + FrozenFieldNameTable.Child.MAX_OVERFLOW, table.sharedNameCount());
    }

    // A name that misses past a full overflow buffer is still resolved correctly, but since it is
    // never recorded, every occurrence reallocates a fresh (merely equal) instance.
    public void testOverflowPastCapReallocatesOnEveryOccurrence() {
        FrozenFieldNameTable table = new FrozenFieldNameTable();
        FrozenFieldNameTable.Child seed = table.makeChild();
        insertName(seed, "known");
        seed.release();

        FrozenFieldNameTable.Child learner = table.makeChild();
        for (int i = 0; i < FrozenFieldNameTable.Child.MAX_OVERFLOW; i++) {
            insertName(learner, "filler_" + i);
        }
        assertEquals(FrozenFieldNameTable.Child.MAX_OVERFLOW, learner.overflowCount());

        String first = insertName(learner, "past_cap");
        String second = insertName(learner, "past_cap");
        assertEquals("past_cap", first);
        assertEquals("past_cap", second);
        assertNotSame("a name past the cap is never canonicalized, so repeats must reallocate", first, second);
        assertEquals("recording must remain at the cap", FrozenFieldNameTable.Child.MAX_OVERFLOW, learner.overflowCount());
    }

    // The shared table converges: once it covers the names in use, releases stop rebuilding it.
    public void testMergingConvergesForAStableFieldSet() {
        FrozenFieldNameTable table = new FrozenFieldNameTable();
        List<String> schemaA = randomDistinctFieldNames(randomIntBetween(3, 10));
        List<String> schemaB = randomDistinctFieldNames(randomIntBetween(3, 10));

        FrozenFieldNameTable.Child a = table.makeChild();
        schemaA.forEach(name -> insertName(a, name));
        a.release();

        FrozenFieldNameTable.Child b = table.makeChild();
        schemaB.forEach(name -> insertName(b, name));
        b.release();

        // Both schemas are now shared, so a child parsing either records nothing to publish.
        FrozenFieldNameTable.Child converged = table.makeChild();
        for (String name : schemaA) {
            assertEquals("converged child must resolve schema A: " + name, name, resolveName(converged, name));
        }
        for (String name : schemaB) {
            assertEquals("converged child must resolve schema B: " + name, name, resolveName(converged, name));
        }
        assertEquals("a child that misses nothing must record nothing", 0, converged.overflowCount());
    }

    /**
     * Children on many threads publishing at once must not lose each other's names: the shared
     * table's CAS loop has to retry against whatever won, not overwrite it. The barrier makes all
     * the releases collide, and the seed child ensures every publisher takes the merge path rather
     * than the uncontended hand-off of the very first publish.
     */
    public void testConcurrentPublishersDoNotLoseNames() throws Exception {
        FrozenFieldNameTable table = new FrozenFieldNameTable();
        FrozenFieldNameTable.Child seed = table.makeChild();
        insertName(seed, "seed_field");
        seed.release();

        int threadCount = randomIntBetween(4, 8);
        int namesPerThread = randomIntBetween(5, 15);
        CyclicBarrier barrier = new CyclicBarrier(threadCount);
        List<String> allNames = new ArrayList<>();
        List<Thread> threads = new ArrayList<>();

        for (int t = 0; t < threadCount; t++) {
            List<String> mine = new ArrayList<>();
            for (int n = 0; n < namesPerThread; n++) {
                mine.add("thread" + t + "_field" + n);
            }
            allNames.addAll(mine);
            threads.add(new Thread(() -> {
                FrozenFieldNameTable.Child child = table.makeChild();
                mine.forEach(name -> resolveName(child, name));
                safeAwait(barrier);
                child.release();
            }));
        }

        threads.forEach(Thread::start);
        for (Thread thread : threads) {
            thread.join();
        }

        FrozenFieldNameTable.Child observer = table.makeChild();
        for (String name : allNames) {
            assertEquals("every concurrently published name must survive: " + name, name, lookupName(observer, name));
        }
        assertEquals("seed_field", lookupName(observer, "seed_field"));
        assertEquals("shared table must hold the seed plus every published name", allNames.size() + 1, table.sharedNameCount());
    }

    // ---- union (direct) ----
    //
    // The tests above exercise union() only indirectly, through mergeNames()/mergeChild() and a
    // full Child learn/freeze/release cycle. That is the right coverage for the public lifecycle,
    // but it cannot economically reach every input union() has to handle - most notably a merge
    // that would exceed MAX_SHARED_NAMES, which would otherwise require driving a Child through
    // thousands of individual inserts. These tests call union() directly instead.

    // Merging a set of names all already present must return the exact same instance: no new
    // table is built when there is nothing to add.
    public void testUnionReturnsCurrentUnchangedWhenNothingIsNew() {
        FrozenFieldNameTable table = new FrozenFieldNameTable();
        List<String> names = randomDistinctFieldNames(randomIntBetween(3, 10));
        FrozenFieldNameTable.Frozen current = publish(table, names);

        NameArrays candidates = NameArrays.of(names);
        FrozenFieldNameTable.Frozen merged = table.union(
            current,
            candidates.names(),
            candidates.keys(),
            candidates.lens(),
            0,
            candidates.names().length
        );

        assertSame("union with nothing new must return current unchanged", current, merged);
    }

    // Merging a mix of already-present and brand-new names must build a table holding the union
    // of both, without disturbing the identity of names current already had.
    public void testUnionBuildsSupersetPreservingExistingIdentity() {
        FrozenFieldNameTable table = new FrozenFieldNameTable();
        List<String> existingNames = randomDistinctFieldNames(randomIntBetween(3, 10));
        FrozenFieldNameTable.Frozen current = publish(table, existingNames);

        List<String> newNames = randomDistinctFieldNames(randomIntBetween(3, 10));
        List<String> candidateNames = new ArrayList<>(existingNames);
        candidateNames.addAll(newNames);
        NameArrays candidates = NameArrays.of(candidateNames);

        FrozenFieldNameTable.Frozen merged = table.union(
            current,
            candidates.names(),
            candidates.keys(),
            candidates.lens(),
            0,
            candidates.names().length
        );

        assertNotSame("union with new names must build a new table", current, merged);
        assertEquals(existingNames.size() + newNames.size(), merged.count());
        for (String name : existingNames) {
            byte[] buf = toBytes(name);
            int hash = FieldNameHash.hashName(buf, 0, buf.length);
            assertSame(
                "an existing name must keep its identity across the merge: " + name,
                current.lookup(buf, 0, buf.length, hash),
                merged.lookup(buf, 0, buf.length, hash)
            );
        }
        for (String name : newNames) {
            assertEquals("a new name must resolve from the merged table: " + name, name, lookupIn(merged, name));
        }
    }

    // A merge that would push the shared table past MAX_SHARED_NAMES must be declined - returning
    // current unchanged - even though the candidate genuinely is new.
    public void testUnionDeclinesWhenResultWouldExceedCap() {
        FrozenFieldNameTable table = new FrozenFieldNameTable();
        List<String> namesAtCap = randomDistinctFieldNames(FrozenFieldNameTable.MAX_SHARED_NAMES);
        FrozenFieldNameTable.Frozen current = publish(table, namesAtCap);
        assertEquals(FrozenFieldNameTable.MAX_SHARED_NAMES, current.count());

        NameArrays oneMore = NameArrays.of(List.of(randomValueOtherThanMany(namesAtCap::contains, () -> randomAlphaOfLength(40))));
        FrozenFieldNameTable.Frozen merged = table.union(current, oneMore.names(), oneMore.keys(), oneMore.lens(), 0, 1);

        assertSame("a merge that would exceed the cap must decline and return current unchanged", current, merged);
    }

    // Once RESET_COOLDOWN_NANOS has passed since the table was created, an over-cap merge must
    // reset rather than decline: current - stale winners and all - is discarded, and the result
    // holds only the names from the merge that triggered the reset.
    public void testUnionResetsOnceTheCooldownElapses() {
        long[] clock = { 0L };
        FrozenFieldNameTable table = new FrozenFieldNameTable(() -> clock[0]);
        List<String> namesAtCap = randomDistinctFieldNames(FrozenFieldNameTable.MAX_SHARED_NAMES);
        FrozenFieldNameTable.Frozen current = publish(table, namesAtCap);

        clock[0] += FrozenFieldNameTable.RESET_COOLDOWN_NANOS;

        // Disjoint from namesAtCap by construction: randomDistinctFieldNames always emits an
        // underscore-plus-digits suffix, which randomAlphaOfLength never does.
        List<String> triggeringNames = List.of(randomAlphaOfLength(30), randomAlphaOfLength(31));
        NameArrays candidates = NameArrays.of(triggeringNames);
        FrozenFieldNameTable.Frozen merged = table.union(
            current,
            candidates.names(),
            candidates.keys(),
            candidates.lens(),
            0,
            candidates.names().length
        );

        assertNotSame("once the cooldown has passed, an over-cap merge must reset rather than decline", current, merged);
        assertEquals("a reset must discard current and start over from just this merge's names", triggeringNames.size(), merged.count());
        for (String name : triggeringNames) {
            assertEquals("a name from the merge that triggered the reset must survive it: " + name, name, lookupIn(merged, name));
        }
        assertNull("a reset must discard every name current held", lookupIn(merged, namesAtCap.get(0)));
    }

    // A reset restarts the cooldown, so a second over-cap merge that follows too soon after must
    // decline rather than reset again.
    public void testUnionDoesNotResetAgainWithinCooldownOfAPriorReset() {
        long[] clock = { 0L };
        FrozenFieldNameTable table = new FrozenFieldNameTable(() -> clock[0]);
        List<String> namesAtCap = randomDistinctFieldNames(FrozenFieldNameTable.MAX_SHARED_NAMES);
        FrozenFieldNameTable.Frozen current = publish(table, namesAtCap);

        clock[0] += FrozenFieldNameTable.RESET_COOLDOWN_NANOS;
        NameArrays triggeringNames = NameArrays.of(List.of(randomAlphaOfLength(30)));
        FrozenFieldNameTable.Frozen afterReset = table.union(
            current,
            triggeringNames.names(),
            triggeringNames.keys(),
            triggeringNames.lens(),
            0,
            1
        );
        assertNotSame("the first over-cap merge after the cooldown must reset", current, afterReset);

        // Refill the freshly reset table back up to the cap without advancing the clock again.
        List<String> refill = randomDistinctFieldNames(FrozenFieldNameTable.MAX_SHARED_NAMES - afterReset.count());
        NameArrays refillArrays = NameArrays.of(refill);
        FrozenFieldNameTable.Frozen atCapAgain = table.union(
            afterReset,
            refillArrays.names(),
            refillArrays.keys(),
            refillArrays.lens(),
            0,
            refillArrays.names().length
        );
        assertEquals(FrozenFieldNameTable.MAX_SHARED_NAMES, atCapAgain.count());

        NameArrays oneMore = NameArrays.of(List.of(randomAlphaOfLength(32)));
        FrozenFieldNameTable.Frozen declined = table.union(atCapAgain, oneMore.names(), oneMore.keys(), oneMore.lens(), 0, 1);

        assertSame("a second over-cap merge within the cooldown of the last reset must decline, not reset again", atCapAgain, declined);
    }

    // A reset (see testUnionResetsOnceTheCooldownElapses) discards every name the previous shared
    // table held. That includes names a long-lived, otherwise-dormant child already resolved through
    // its own frozen table - not just overflow-resolved ones - so such a name can stop resolving
    // there entirely, not merely change identity, contradicting a narrower claim release() used to
    // make. Grows the shared table via mergeNames directly rather than through Child.release: once
    // anything is published, a fresh Child inherits it and records further names only through its
    // MAX_OVERFLOW-capped overflow buffer, so driving it through many children could never reach
    // MAX_SHARED_NAMES in one release.
    public void testResetCanUnresolveANameALongLivedChildAlreadyFroze() {
        long[] clock = { 0L };
        FrozenFieldNameTable table = new FrozenFieldNameTable(() -> clock[0]);

        FrozenFieldNameTable.Child longLived = table.makeChild();
        String known = insertName(longLived, "known");
        longLived.release(); // first-ever publish: shared == longLived's own frozen table

        // Grow the shared table up to the cap through ordinary, identity-preserving union growth.
        NameArrays fill = NameArrays.of(randomDistinctFieldNames(FrozenFieldNameTable.MAX_SHARED_NAMES - 1));
        table.mergeNames(fill.names(), fill.keys(), fill.lens(), 0, fill.names().length);
        assertEquals(FrozenFieldNameTable.MAX_SHARED_NAMES, table.sharedNameCount());
        assertSame("growing to the cap must preserve known's identity", known, lookupIn(table.getShared(), "known"));

        // Elapse the cooldown, then push the table over the cap: this resets rather than declines.
        clock[0] += FrozenFieldNameTable.RESET_COOLDOWN_NANOS;
        NameArrays triggering = NameArrays.of(List.of("triggering_name"));
        table.mergeNames(triggering.names(), triggering.keys(), triggering.lens(), 0, 1);
        assertNull("the reset table must not carry over a name it did not itself trigger with", lookupIn(table.getShared(), "known"));

        // longLived has been dormant since its first release; its own frozen still resolves "known".
        assertSame("a dormant child's own frozen table is unaffected until its next release", known, lookupName(longLived, "known"));

        longLived.release(); // re-syncs frozen against the parent's now-reset shared table

        assertNull(
            "a name the previous shared table held - even one this child's own frozen table already "
                + "resolved - must stop resolving once that table is discarded by a reset",
            lookupName(longLived, "known")
        );
    }

    // A single child whose own first-freeze table already exceeds the cap must not publish it
    // wholesale: mergeChild has to decline before ever attempting the CAS.
    public void testFirstPublishDeclinesWhenTheChildAloneExceedsCap() {
        FrozenFieldNameTable table = new FrozenFieldNameTable();
        List<String> tooMany = randomDistinctFieldNames(FrozenFieldNameTable.MAX_SHARED_NAMES + 1);
        FrozenFieldNameTable.Child oversized = table.makeChild();
        tooMany.forEach(name -> insertName(oversized, name));
        oversized.release();

        assertNull("an oversized first donation must not be published", table.getShared());
        assertEquals("declining a donation must not affect the child's own table", 0, table.sharedNameCount());
        for (String name : randomSubsetOf(20, tooMany)) {
            assertEquals("the oversized child must still resolve its own names", name, lookupName(oversized, name));
        }

        // A later, normally-sized child must still be able to publish: declining an oversized
        // donation leaves the shared table available for the next one, not permanently blocked.
        FrozenFieldNameTable.Child normal = table.makeChild();
        insertName(normal, "ordinary_field");
        normal.release();
        assertEquals("declining an oversized donation must not permanently block later publishes", 1, table.sharedNameCount());
    }

    // A child whose own table exceeds the cap must decline even when it finishes learning after
    // another child has already published, and must not fall through to mergeNames/union at all
    // - which matters because, before mergeChild bailed out early, this path could reach
    // resetOrDecline's reset branch and publish a table built from the oversized batch alone,
    // itself over the cap.
    public void testMergeIntoExistingTableDeclinesWhenTheChildAloneExceedsCap() {
        FrozenFieldNameTable table = new FrozenFieldNameTable();

        // Starts learning before anything is published, so it stays in learning mode - unlike a
        // child created after the seed below, which would inherit the seed's table and record any
        // overflow past the cap in its bounded buffer instead of an unbounded learning set.
        FrozenFieldNameTable.Child learner = table.makeChild();
        List<String> tooMany = randomDistinctFieldNames(FrozenFieldNameTable.MAX_SHARED_NAMES + 1);
        tooMany.forEach(name -> insertName(learner, name));

        FrozenFieldNameTable.Frozen seeded = publish(table, randomDistinctFieldNames(randomIntBetween(1, 5)));

        learner.release();

        assertSame("merging an oversized child must leave an already-published table untouched", seeded, table.getShared());
    }

    /** Publishes {@code names} as the shared table via the normal learn/freeze/release path. */
    private static FrozenFieldNameTable.Frozen publish(FrozenFieldNameTable table, List<String> names) {
        FrozenFieldNameTable.Child child = table.makeChild();
        names.forEach(name -> insertName(child, name));
        child.release();
        return table.getShared();
    }

    private static String lookupIn(FrozenFieldNameTable.Frozen frozen, String name) {
        byte[] buf = toBytes(name);
        int hash = FieldNameHash.hashName(buf, 0, buf.length);
        return frozen.lookup(buf, 0, buf.length, hash);
    }

    /** Parallel name/key/length arrays in the shape union() and mergeNames() expect. */
    private record NameArrays(String[] names, byte[][] keys, int[] lens) {
        static NameArrays of(List<String> names) {
            String[] n = names.toArray(new String[0]);
            byte[][] k = new byte[n.length][];
            int[] l = new int[n.length];
            for (int i = 0; i < n.length; i++) {
                k[i] = toBytes(n[i]);
                l[i] = k[i].length;
            }
            return new NameArrays(n, k, l);
        }
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

    // A long-lived frozen child does not see a sibling's publication until its own next release,
    // even though it has nothing new of its own to offer.
    public void testReleaseResyncsFrozenChildWithNamesFromOtherChildren() {
        FrozenFieldNameTable table = new FrozenFieldNameTable();
        FrozenFieldNameTable.Child longLived = table.makeChild();
        insertName(longLived, "known");
        longLived.release();

        FrozenFieldNameTable.Child other = table.makeChild();
        insertName(other, "learned_by_sibling");
        other.release();

        assertNull(
            "a frozen child must not see a sibling's publication before its own release",
            lookupName(longLived, "learned_by_sibling")
        );

        longLived.release();
        assertEquals(
            "release must re-sync frozen from the parent even with nothing of its own to publish",
            "learned_by_sibling",
            lookupName(longLived, "learned_by_sibling")
        );
    }

    // Once a name a child recorded in its own overflow buffer is published by a sibling, the next
    // release must adopt it from the frozen table and drop the now-redundant overflow entry. That
    // adoption can change which String instance the name resolves to: this child's own overflow
    // instance is dropped in favor of whichever instance the sibling published first. Callers are
    // only guaranteed equals(), not ==, across a release - see FieldNameLookup#release.
    public void testReleaseCompactsOverflowOnceSharedCatchesUp() {
        FrozenFieldNameTable table = new FrozenFieldNameTable();
        FrozenFieldNameTable.Child longLived = table.makeChild();
        insertName(longLived, "known");
        longLived.release();

        FrozenFieldNameTable.Child longLivedSameTable = table.makeChild();
        String ownOverflowInstance = insertName(longLivedSameTable, "overflow_name");
        assertEquals("miss must land in the overflow buffer", 1, longLivedSameTable.overflowCount());

        FrozenFieldNameTable.Child sibling = table.makeChild();
        String siblingsInstance = insertName(sibling, "overflow_name");
        sibling.release();
        assertNotSame(
            "the two children learned independently and so must have allocated distinct instances",
            ownOverflowInstance,
            siblingsInstance
        );

        longLivedSameTable.release();
        assertEquals("the overflow entry the sibling already published must be compacted away", 0, longLivedSameTable.overflowCount());
        String resolvedAfterCompaction = lookupName(longLivedSameTable, "overflow_name");
        assertEquals("the name must still resolve, now straight from the frozen table", "overflow_name", resolvedAfterCompaction);
        assertSame(
            "adopting the shared table must resolve to whichever instance the sibling published, not rebuild its own",
            siblingsInstance,
            resolvedAfterCompaction
        );
        assertNotSame(
            "the child's own overflow instance must no longer be the one callers see once a sibling's publication wins",
            ownOverflowInstance,
            resolvedAfterCompaction
        );
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

    // ---- Probing ----

    // A collision at the table's last slot must wrap via (i + 1) & mask to slot 0; a name placed
    // there by build() must still be found by lookup(), not silently lost off the end of the array.
    public void testLookupWrapsAroundEndOfTable() {
        // A table built from a handful of names always gets the minimum size of 32 slots
        // (mask 31, see FrozenFieldNameTable#build), so two distinct names whose hash lands in the
        // last slot are guaranteed to collide there, forcing the second one to wrap to slot 0.
        int mask = 31;
        String name1 = findNameHashingToSlot(mask, mask, Set.of());
        String name2 = findNameHashingToSlot(mask, mask, Set.of(name1));

        FrozenFieldNameTable.Child child = new FrozenFieldNameTable().makeChild();
        insertName(child, name1);
        insertName(child, name2);
        child.freeze();

        assertEquals("name occupying the colliding slot must still resolve", name1, lookupName(child, name1));
        assertEquals("name that wrapped past the end of the table must resolve", name2, lookupName(child, name2));
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

    /**
     * Brute-forces a random field name whose {@link FieldNameHash#hashName} value, masked with
     * {@code mask}, equals {@code slot}. Odds of a hit are {@code 1 / (mask + 1)} per attempt, so
     * for a small mask this succeeds almost immediately; used to construct deliberate hash-table
     * collisions without a test-only seam into the real hashing.
     */
    private static String findNameHashingToSlot(int mask, int slot, Set<String> exclude) {
        for (int attempt = 0; attempt < 100_000; attempt++) {
            String candidate = randomAlphaOfLengthBetween(3, 12);
            if (exclude.contains(candidate)) {
                continue;
            }
            byte[] buf = toBytes(candidate);
            int hash = FieldNameHash.hashName(buf, 0, buf.length);
            if ((hash & mask) == slot) {
                return candidate;
            }
        }
        throw new AssertionError("failed to find a name hashing to slot " + slot + " within the attempt budget");
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

    /**
     * Resolves a name the way the walker does: look up, and insert only on a miss. Tests that care
     * whether a name was <em>new</em> must go through this, since inserting unconditionally would
     * record an overflow entry regardless.
     */
    private static String resolveName(FrozenFieldNameTable.Child child, String name) {
        String hit = lookupName(child, name);
        return hit != null ? hit : insertName(child, name);
    }
}
