/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

/**
 * Tests for {@link org.elasticsearch.painless.phase.DefaultConstantListOptimizationPhase},
 * which hoists constant list literals into static {@code HashSet} fields when they are
 * the receiver of a {@code .contains()} call.
 */
public class ConstantListOptimizationTests extends ScriptTestCase {

    // --- Correctness: string lists ---

    public void testStringListContainsHit() {
        assertThat(exec("['a', 'b', 'c'].contains('b')"), is(true));
    }

    public void testStringListContainsMiss() {
        assertThat(exec("['a', 'b', 'c'].contains('d')"), is(false));
    }

    // --- Correctness: integer lists ---

    public void testIntegerListContainsHit() {
        assertThat(exec("[1, 2, 3].contains(2)"), is(true));
    }

    public void testIntegerListContainsMiss() {
        assertThat(exec("[1, 2, 3].contains(4)"), is(false));
    }

    // --- Correctness: edge cases ---

    public void testSingleElementListHit() {
        assertThat(exec("['only'].contains('only')"), is(true));
    }

    public void testSingleElementListMiss() {
        assertThat(exec("['only'].contains('other')"), is(false));
    }

    public void testLargeListContainsHit() {
        String listLiteral = IntStream.rangeClosed(1, 30).mapToObj(Integer::toString).collect(Collectors.joining(", ", "[", "]"));
        assertThat(exec(listLiteral + ".contains(15)"), is(true));
    }

    public void testLargeListContainsMiss() {
        String listLiteral = IntStream.rangeClosed(1, 30).mapToObj(Integer::toString).collect(Collectors.joining(", ", "[", "]"));
        assertThat(exec(listLiteral + ".contains(31)"), is(false));
    }

    // --- Correctness: contains with params ---

    public void testContainsWithParamsHit() {
        assertThat(exec("['a', 'b', 'c'].contains(params.target)", Map.of("target", "b"), true), is(true));
    }

    public void testContainsWithParamsMiss() {
        assertThat(exec("['a', 'b', 'c'].contains(params.target)", Map.of("target", "z"), true), is(false));
    }

    // --- Non-optimization cases: must still work correctly ---

    public void testNonConstantElementStillWorks() {
        assertThat(exec("def x = 'a'; [x, 'b', 'c'].contains('a')"), is(true));
    }

    public void testMutableListAssignment() {
        assertThat(exec("def x = [1, 2, 3]; x.add(4); return x.size()"), is(4));
    }

    public void testNonQualifyingMethodSize() {
        assertThat(exec("['a', 'b', 'c'].size()"), is(3));
    }

    public void testNonQualifyingMethodIndexOf() {
        assertThat(exec("['a', 'b', 'c'].indexOf('b')"), is(1));
    }

    // --- Bytecode verification ---

    public void testOptimizationProducesStaticField() {
        String asm = Debugger.toString("['a', 'b', 'c'].contains('b')");
        assertThat(
            "expected a static HashSet field for the hoisted constant, got:\n" + asm,
            asm,
            allOf(containsString("GETSTATIC"), containsString("Ljava/util/HashSet;"))
        );
    }

    public void testNonConstantListIsNotOptimized() {
        String asm = Debugger.toString("def x = 'a'; [x, 'b'].contains('a')");
        assertThat(
            "non-constant list should not be hoisted to a static field, got:\n" + asm,
            asm,
            not(containsString("Ljava/util/HashSet;"))
        );
    }
}
