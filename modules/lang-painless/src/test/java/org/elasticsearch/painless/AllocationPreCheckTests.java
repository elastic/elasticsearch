/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless;

import org.elasticsearch.painless.spi.PainlessTestScript;
import org.elasticsearch.script.ScriptException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;

/**
 * End-to-end tests for PR 3 compile-time-known allocation pre-checks: {@code new T()}, initialized arrays {@code new T[]{...}},
 * autoboxing, and lambda captures. Each site charges the running counter before allocating and trips the per-context limit
 * (raising an uncatchable {@link PainlessError}, surfaced as a {@link ScriptException}) when the charge exceeds it.
 */
public class AllocationPreCheckTests extends AllocationTestCase {

    public void testInitializedArrayCharged() {
        // new int[]{1,2,3,4} => pad8(16 + 4*4) = 32 bytes.
        assertEquals(AllocSizes.arraySize(int.class, 4), allocatedBytes("int[] a = new int[] {1, 2, 3, 4}; return \"x\";"));
    }

    public void testInitializedArrayTripsLimit() {
        assertTripsLimit("int[] a = new int[] {1, 2, 3}; return \"x\";");
    }

    public void testAutoboxIntCharged() {
        // Boxing an int to Integer (via the def cast) charges 16 bytes.
        assertEquals(AllocSizes.boxSize(Integer.class), allocatedBytes("def o = 5; return \"x\";"));
    }

    public void testAutoboxLongCharged() {
        // Boxing a long to Long charges 24 bytes.
        assertEquals(AllocSizes.boxSize(Long.class), allocatedBytes("def o = 5L; return \"x\";"));
    }

    public void testAutoboxTripsLimit() {
        assertTripsLimit("def o = 5; return \"x\";");
    }

    public void testForEachOverIterableChargesIterator() {
        // Guards the loop's own codegen path, which does not go through visitInvokeCall.
        long iterator = AllocationEstimators.iteratorBytes(List.of());
        long list = allocatedBytes("List l = new ArrayList(); return \"x\";");
        long listAndLoop = allocatedBytes("long n = 0; List l = new ArrayList(); for (def e : l) { n++; } return \"x\";");

        assertEquals(iterator, listAndLoop - list);
    }

    public void testForEachOverIterableChargesIteratorOncePerLoopNotPerElement() {
        long empty = allocatedBytes("long n = 0; List l = new ArrayList(); for (def e : l) { n++; } return \"x\";");
        long threeElements = allocatedBytes(
            "long n = 0; List l = new ArrayList(); l.add(1); l.add(2); l.add(3); for (def e : l) { n++; } return \"x\";"
        );
        long threeElementsNoLoop = allocatedBytes("List l = new ArrayList(); l.add(1); l.add(2); l.add(3); return \"x\";");

        // Iterating three elements costs the same iterator as iterating none.
        assertEquals(threeElements - threeElementsNoLoop, empty - allocatedBytes("List l = new ArrayList(); return \"x\";"));
    }

    public void testForEachOverDefIterableChargesIterator() {
        // Charged as an inline constant, since DefBootstrap.ITERATOR has no annotation to read.
        long list = allocatedBytes("def l = new ArrayList(); return \"x\";");
        long listAndLoop = allocatedBytes("long n = 0; def l = new ArrayList(); for (def e : l) { n++; } return \"x\";");

        assertEquals(AllocSizes.ITERATOR_BYTES, listAndLoop - list);
    }

    public void testForEachOverDefArrayChargesIterator() {
        // A def array builds a ValueIterator wrapper, charged the same constant.
        long array = allocatedBytes("def a = new int[] {1, 2, 3}; return \"x\";");
        long arrayAndLoop = allocatedBytes("long n = 0; def a = new int[] {1, 2, 3}; for (def e : a) { n++; } return \"x\";");

        assertEquals(AllocSizes.ITERATOR_BYTES, arrayAndLoop - array);
    }

    public void testForEachOverArrayChargesNoIterator() {
        // Arrays use an index loop, so there is no iterator.
        long array = allocatedBytes("int[] a = new int[] {1, 2, 3}; return \"x\";");
        long arrayAndLoop = allocatedBytes("long n = 0; int[] a = new int[] {1, 2, 3}; for (int e : a) { n++; } return \"x\";");

        assertEquals(array, arrayAndLoop);
    }

    public void testExplicitIteratorCallCharged() {
        long list = allocatedBytes("List l = new ArrayList(); return \"x\";");
        long listAndIterator = allocatedBytes("List l = new ArrayList(); Iterator i = l.iterator(); return \"x\";");

        assertEquals(AllocationEstimators.iteratorBytes(List.of()), listAndIterator - list);
    }

    public void testStringLiteralNotCharged() {
        // A constant-pool string load is not a runtime allocation and must not be charged.
        assertEquals(0L, allocatedBytes("String s = \"literal\"; return \"x\";"));
    }

    public void testLambdaCaptureCharged() {
        // Creating the lambda allocates a capture object, which is charged in the enclosing method.
        assertThat(allocatedBytes("Optional.empty().orElseGet(() -> 1); return \"x\";"), greaterThan(0L));
    }

    public void testLambdaCaptureTripsLimit() {
        assertTripsLimit("Optional.empty().orElseGet(() -> 1); return \"x\";");
    }

    public void testRegexFindCharged() {
        // '=~' builds a Matcher and the read-limited wrapper around the input. /o/ has no capturing groups.
        assertEquals(AllocSizes.matcherBytes(0), allocatedBytes("boolean b = 'foo' =~ /o/; return \"x\";"));
    }

    public void testRegexMatchCharged() {
        assertEquals(AllocSizes.matcherBytes(0), allocatedBytes("boolean b = 'foo' ==~ /foo/; return \"x\";"));
    }

    public void testRegexOperatorChargedOncePerUse() {
        // Each use builds its own Matcher, so two uses cost twice as much.
        assertEquals(
            2 * AllocSizes.matcherBytes(0),
            allocatedBytes("boolean b = 'foo' ==~ /foo/; boolean c = 'bar' =~ /a/; return \"x\";")
        );
    }

    public void testRegexLiteralGroupCountChargedExactly() {
        // A literal pattern is a compile-time constant, so its real group count sizes the Matcher's arrays.
        String twelveGroups = "boolean b = 'abcdefghijkl' ==~ /(a)(b)(c)(d)(e)(f)(g)(h)(i)(j)(k)(l)/; return \"x\";";

        assertEquals(AllocSizes.matcherBytes(12), allocatedBytes(twelveGroups));
        assertThat(AllocSizes.matcherBytes(12), greaterThan(AllocSizes.matcherBytes(0)));
    }

    public void testRegexOperatorOnPatternVariableUsesTheBound() {
        // The group count of a Pattern held in a variable is not known at compile time, so the bound is charged instead.
        assertEquals(AllocSizes.MATCHER_BYTES, allocatedBytes("Pattern p = /a/; boolean b = 'a' =~ p; return \"x\";"));
    }

    public void testRegexLiteralNotCharged() {
        // The regex literal is a static constant on the script class, so naming one allocates nothing.
        assertEquals(0L, allocatedBytes("Pattern p = /foo/; return \"x\";"));
        assertEquals(0L, allocatedBytes("def p = /foo/; return \"x\";"));
    }

    public void testChargedByteValuesArePinned() {
        // Concrete numbers, so editing a formula cannot change a charge without a test noticing.
        assertEquals(232L, allocatedBytes("boolean b = 'foo' ==~ /foo/; return \"x\";"));
        assertEquals(136L, allocatedBytes("List l = ['a', 'b', 'c']; return \"x\";"));
        assertEquals(208L, allocatedBytes("Map m = [:]; return \"x\";"));
    }

    public void testRegexOperatorTripsLimit() {
        assertTripsLimit("boolean b = 'foo' ==~ /foo/; return \"x\";");
    }

    public void testListLiteralCharged() {
        // A list literal builds an ArrayList and adds each element, none of which goes through the estimator path.
        assertEquals(AllocationEstimators.listLiteralBytes(3), allocatedBytes("List l = ['a', 'b', 'c']; return \"x\";"));
    }

    public void testEmptyListLiteralCharged() {
        assertEquals(AllocationEstimators.listLiteralBytes(0), allocatedBytes("List l = []; return \"x\";"));
    }

    public void testListLiteralChargeGrowsPastDefaultCapacity() {
        // Up to the default capacity the charge is flat; past it the backing array grows with the element count.
        assertEquals(allocatedBytes("List l = []; return \"x\";"), allocatedBytes("List l = ['a', 'b', 'c']; return \"x\";"));

        String twelve = "List l = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l']; return \"x\";";
        assertEquals(AllocationEstimators.listLiteralBytes(12), allocatedBytes(twelve));
        assertThat(allocatedBytes(twelve), greaterThan(AllocationEstimators.listLiteralBytes(0)));
    }

    public void testListLiteralTripsLimit() {
        assertTripsLimit("List l = ['a', 'b', 'c']; return \"x\";");
    }

    public void testMapLiteralCharged() {
        // A map literal builds a HashMap and puts each entry, also bypassing the estimator path.
        assertEquals(AllocationEstimators.mapLiteralBytes(2), allocatedBytes("Map m = ['a': 'b', 'c': 'd']; return \"x\";"));
    }

    public void testEmptyMapLiteralCharged() {
        assertEquals(AllocationEstimators.mapLiteralBytes(0), allocatedBytes("Map m = [:]; return \"x\";"));
    }

    public void testMapLiteralChargeGrowsWithTable() {
        // Thirteen entries pass three quarters of the default table, so the table doubles and the charge jumps.
        String thirteen = "Map m = ['a': 1, 'b': 1, 'c': 1, 'd': 1, 'e': 1, 'f': 1, 'g': 1, 'h': 1, 'i': 1, 'j': 1, 'k': 1, "
            + "'l': 1, 'm': 1]; return \"x\";";
        long boxed = 13 * AllocSizes.boxSize(Integer.class);

        assertEquals(AllocationEstimators.mapLiteralBytes(13) + boxed, allocatedBytes(thirteen));

        // The thirteenth entry costs more than the twelfth, because it also doubles the table.
        long twelfthEntry = AllocationEstimators.mapLiteralBytes(12) - AllocationEstimators.mapLiteralBytes(11);
        long thirteenthEntry = AllocationEstimators.mapLiteralBytes(13) - AllocationEstimators.mapLiteralBytes(12);
        assertThat(thirteenthEntry, greaterThan(twelfthEntry));
    }

    public void testMapLiteralTripsLimit() {
        assertTripsLimit("Map m = ['a': 'b']; return \"x\";");
    }

    public void testRegexInLoopChargedPerIteration() {
        // Proves the charge is inside the loop body, so a hundred passes cost a hundred Matchers.
        assertEquals(
            100 * AllocSizes.matcherBytes(0),
            allocatedBytes("for (int i = 0; i < 100; i++) { boolean b = 'foo' =~ /o/; } return \"x\";")
        );
    }

    public void testSitesInsideLambdaBodyCharged() {
        // Proves a lambda body charges the enclosing script, not nothing, for both new sites.
        String plain = "Optional.of('x').map(v -> { return v; }); return \"x\";";
        String withSites = "Optional.of('x').map(v -> { boolean b = v =~ /x/; return [v]; }); return \"x\";";

        assertEquals(
            AllocSizes.matcherBytes(0) + AllocationEstimators.listLiteralBytes(1),
            allocatedBytes(withSites) - allocatedBytes(plain)
        );
    }

    public void testSitesInsideLambdaBodyTripLimit() {
        // Proves the limit still bites inside a lambda body.
        assertTripsLimit("Optional.of('x').map(v -> { boolean b = v =~ /x/; return [v]; }); return \"x\";");
    }

    public void testNestedListLiteralsChargedSeparately() {
        // Proves every literal in a nested literal is charged, along with the boxing of its int elements.
        long expected = AllocationEstimators.listLiteralBytes(2) + AllocationEstimators.listLiteralBytes(2) + AllocationEstimators
            .listLiteralBytes(1) + 3 * AllocSizes.boxSize(int.class);

        assertEquals(expected, allocatedBytes("List l = [[1, 2], [3]]; return \"x\";"));
    }

    public void testMapLiteralHoldingAListCharged() {
        // Proves a literal nested in a map value is charged too.
        long expected = AllocationEstimators.mapLiteralBytes(1) + AllocationEstimators.listLiteralBytes(1) + AllocSizes.boxSize(int.class);

        assertEquals(expected, allocatedBytes("Map m = ['a': [1]]; return \"x\";"));
    }

    public void testListLiteralTripsBeforeItsElementsRun() {
        // Proves the charge is a pre-check: a breach stops the script before any element expression runs.
        String source = "def mark(List l) { l.add(1); return 'y'; } List seen = params['seen']; return [mark(seen)];";

        List<Object> allowed = new ArrayList<>();
        Map<String, Object> allowedParams = new HashMap<>();
        allowedParams.put("seen", allowed);
        compile(source, "1mb", allowedParams).execute();
        assertEquals(1, allowed.size());

        List<Object> blocked = new ArrayList<>();
        Map<String, Object> blockedParams = new HashMap<>();
        blockedParams.put("seen", blocked);
        PainlessTestScript script = compile(source, "1b", blockedParams);
        ScriptException e = expectThrows(ScriptException.class, script::execute);
        assertThat(rootMessage(e), containsString("allocation limit exceeded"));
        assertEquals(0, blocked.size());
    }

    public void testRegexWithDefLeftOperandCharged() {
        // Proves a def input charges the same Matcher as a typed one, and nothing extra.
        long plain = allocatedBytes("def s = 'foo'; return \"x\";");
        long withRegex = allocatedBytes("def s = 'foo'; boolean b = s =~ /o/; return \"x\";");

        assertEquals(AllocSizes.matcherBytes(0), withRegex - plain);
    }

    public void testListLiteralOfIntsChargesBoxing() {
        // Proves the list and the boxing of its int elements are both charged.
        assertEquals(
            AllocationEstimators.listLiteralBytes(3) + 3 * AllocSizes.boxSize(int.class),
            allocatedBytes("List l = [1, 2, 3]; return \"x\";")
        );
    }

    /** Joins the messages down a throwable's cause chain, so an assertion can look for the one the limit raises. */
    private static String rootMessage(Throwable throwable) {
        StringBuilder messages = new StringBuilder();

        for (Throwable t = throwable; t != null; t = t.getCause()) {
            messages.append(t.getMessage()).append('\n');
        }

        return messages.toString();
    }
}
