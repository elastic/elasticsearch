/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.compute.test.TestWarningsSource;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;

/**
 * Tests for {@link Warnings} and their collection into a {@link DriverContext}.
 */
public class WarningsTests extends ESTestCase {
    public void testRegisterCollect() {
        DriverContext dc = collectingContext();
        Warnings warnings = dc.createWarnings(new TestWarningsSource("foo"));
        warnings.registerException(new IllegalArgumentException());
        assertThat(
            collected(dc),
            contains(
                "Line 1:1: evaluation of [foo] failed, treating result as null. Only first 20 failures recorded.",
                "Line 1:1: java.lang.IllegalArgumentException: null"
            )
        );
    }

    public void testRegisterCollectFilled() {
        DriverContext dc = collectingContext();
        Warnings warnings = dc.createWarnings(new TestWarningsSource("foo"));
        for (int i = 0; i < Warnings.MAX_ADDED_WARNINGS + 1000; i++) {
            warnings.registerException(new IllegalArgumentException(Integer.toString(i)));
        }

        String[] expected = new String[21];
        expected[0] = "Line 1:1: evaluation of [foo] failed, treating result as null. Only first 20 failures recorded.";
        for (int i = 0; i < Warnings.MAX_ADDED_WARNINGS; i++) {
            expected[i + 1] = "Line 1:1: java.lang.IllegalArgumentException: " + i;
        }

        assertThat(collected(dc), contains(expected));
    }

    public void testRegisterCollectViews() {
        DriverContext dc = collectingContext();
        Warnings warnings = dc.createWarnings(new TestWarningsSource("foo", "view1"));
        for (int i = 0; i < Warnings.MAX_ADDED_WARNINGS + 1000; i++) {
            warnings.registerException(new IllegalArgumentException(Integer.toString(i)));
        }

        String[] expected = new String[21];
        expected[0] = "Line 1:1 (in view [view1]): evaluation of [foo] failed, treating result as null. Only first 20 failures recorded.";
        for (int i = 0; i < Warnings.MAX_ADDED_WARNINGS; i++) {
            expected[i + 1] = "Line 1:1 (in view [view1]): java.lang.IllegalArgumentException: " + i;
        }

        assertThat(collected(dc), contains(expected));
    }

    public void testRegisterIgnore() {
        DriverContext dc = ignoringContext();
        Warnings warnings = dc.createWarnings(new TestWarningsSource("foo"));
        warnings.registerException(new IllegalArgumentException());
        assertThat(collected(dc), empty());
    }

    public void testRegisterWarningCollect() {
        DriverContext dc = collectingContext();
        Warnings warnings = dc.createWarnings(new TestWarningsSource("foo"));
        warnings.registerWarning("some custom warning");
        assertThat(collected(dc), contains("Line 1:1 [foo]: some custom warning"));
    }

    public void testRegisterWarningDeduplication() {
        DriverContext dc = collectingContext();
        Warnings warnings = dc.createWarnings(new TestWarningsSource("foo"));
        warnings.registerWarning("duplicate warning");
        warnings.registerWarning("duplicate warning");
        warnings.registerWarning("duplicate warning");
        assertThat(collected(dc), contains("Line 1:1 [foo]: duplicate warning"));
    }

    public void testRegisterWarningMultipleDistinct() {
        DriverContext dc = collectingContext();
        Warnings warnings = dc.createWarnings(new TestWarningsSource("foo"));
        warnings.registerWarning("warning A");
        warnings.registerWarning("warning B");
        assertThat(collected(dc), contains("Line 1:1 [foo]: warning A", "Line 1:1 [foo]: warning B"));
    }

    public void testRegisterWarningCollectFilled() {
        DriverContext dc = collectingContext();
        Warnings warnings = dc.createWarnings(new TestWarningsSource("foo"));
        for (int i = 0; i < Warnings.MAX_ADDED_WARNINGS + 1000; i++) {
            warnings.registerWarning("warning " + i);
        }

        String[] expected = new String[Warnings.MAX_ADDED_WARNINGS];
        for (int i = 0; i < Warnings.MAX_ADDED_WARNINGS; i++) {
            expected[i] = "Line 1:1 [foo]: warning " + i;
        }
        assertThat(collected(dc), contains(expected));
    }

    public void testRegisterWarningIgnore() {
        DriverContext dc = ignoringContext();
        Warnings warnings = dc.createWarnings(new TestWarningsSource("foo"));
        warnings.registerWarning("some custom warning");
        assertThat(collected(dc), empty());
    }

    public void testRegisterWarningWithView() {
        DriverContext dc = collectingContext();
        Warnings warnings = dc.createWarnings(new TestWarningsSource("foo", "view1"));
        warnings.registerWarning("some custom warning");
        assertThat(collected(dc), contains("Line 1:1 [foo] (in view [view1]): some custom warning"));
    }

    public void testMixedRegisterExceptionThenWarning() {
        DriverContext dc = collectingContext();
        Warnings warnings = dc.createWarnings(new TestWarningsSource("foo"));
        warnings.registerException(new IllegalArgumentException("bad arg"));
        warnings.registerWarning("custom warning");
        assertThat(
            collected(dc),
            contains(
                "Line 1:1: evaluation of [foo] failed, treating result as null. Only first 20 failures recorded.",
                "Line 1:1: java.lang.IllegalArgumentException: bad arg",
                "Line 1:1 [foo]: custom warning"
            )
        );
    }

    public void testMixedRegisterWarningThenException() {
        DriverContext dc = collectingContext();
        Warnings warnings = dc.createWarnings(new TestWarningsSource("foo"));
        warnings.registerWarning("custom warning");
        warnings.registerException(new IllegalArgumentException("bad arg"));
        assertThat(
            collected(dc),
            contains(
                "Line 1:1 [foo]: custom warning",
                "Line 1:1: evaluation of [foo] failed, treating result as null. Only first 20 failures recorded.",
                "Line 1:1: java.lang.IllegalArgumentException: bad arg"
            )
        );
    }

    public void testMixedSharedLimit() {
        DriverContext dc = collectingContext();
        Warnings warnings = dc.createWarnings(new TestWarningsSource("foo"));
        int halfLimit = Warnings.MAX_ADDED_WARNINGS / 2;
        for (int i = 0; i < halfLimit; i++) {
            warnings.registerWarning("warning " + i);
        }
        for (int i = 0; i < halfLimit + 1000; i++) {
            warnings.registerException(new IllegalArgumentException(Integer.toString(i)));
        }

        // Both types share the same addedWarnings counter, so only MAX_ADDED_WARNINGS total are emitted.
        // The firstExceptionWarning header is always emitted on the first registerException call
        // thanks to the dedicated exceptionWarningEmitted flag.
        String[] expected = new String[1 + Warnings.MAX_ADDED_WARNINGS];
        for (int i = 0; i < halfLimit; i++) {
            expected[i] = "Line 1:1 [foo]: warning " + i;
        }
        expected[halfLimit] = "Line 1:1: evaluation of [foo] failed, treating result as null. Only first 20 failures recorded.";
        for (int i = 0; i < halfLimit; i++) {
            expected[halfLimit + 1 + i] = "Line 1:1: java.lang.IllegalArgumentException: " + i;
        }
        assertThat(collected(dc), contains(expected));
    }

    private static DriverContext collectingContext() {
        return new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, TestBlockFactory.getNonBreakingInstance(), null);
    }

    private static DriverContext ignoringContext() {
        return new DriverContext(
            BigArrays.NON_RECYCLING_INSTANCE,
            TestBlockFactory.getNonBreakingInstance(),
            null,
            null,
            DriverContext.WarningsMode.IGNORE
        );
    }

    private static List<String> collected(DriverContext dc) {
        dc.finish();
        return dc.warnings();
    }
}
