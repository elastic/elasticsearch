/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.test.TestWarningsSource;
import org.elasticsearch.test.ESTestCase;

public class WarningsTests extends ESTestCase {
    public void testRegisterCollect() {
        Warnings warnings = Warnings.createWarnings(DriverContext.WarningsMode.COLLECT, new TestWarningsSource("foo"));
        warnings.registerException(new IllegalArgumentException());
        assertWarnings(
            "Line 1:1: evaluation of [foo] failed, treating result as null. Only first 20 failures recorded.",
            "Line 1:1: java.lang.IllegalArgumentException: null"
        );
    }

    public void testRegisterCollectFilled() {
        Warnings warnings = Warnings.createWarnings(DriverContext.WarningsMode.COLLECT, new TestWarningsSource("foo"));
        for (int i = 0; i < Warnings.MAX_ADDED_WARNINGS + 1000; i++) {
            warnings.registerException(new IllegalArgumentException(Integer.toString(i)));
        }

        String[] expected = new String[21];
        expected[0] = "Line 1:1: evaluation of [foo] failed, treating result as null. Only first 20 failures recorded.";
        for (int i = 0; i < Warnings.MAX_ADDED_WARNINGS; i++) {
            expected[i + 1] = "Line 1:1: java.lang.IllegalArgumentException: " + i;
        }

        assertWarnings(expected);
    }

    public void testRegisterCollectViews() {
        Warnings warnings = Warnings.createWarnings(DriverContext.WarningsMode.COLLECT, new TestWarningsSource("foo", "view1"));
        for (int i = 0; i < Warnings.MAX_ADDED_WARNINGS + 1000; i++) {
            warnings.registerException(new IllegalArgumentException(Integer.toString(i)));
        }

        String[] expected = new String[21];
        expected[0] = "Line 1:1 (in view [view1]): evaluation of [foo] failed, treating result as null. Only first 20 failures recorded.";
        for (int i = 0; i < Warnings.MAX_ADDED_WARNINGS; i++) {
            expected[i + 1] = "Line 1:1 (in view [view1]): java.lang.IllegalArgumentException: " + i;
        }

        assertWarnings(expected);
    }

    public void testRegisterIgnore() {
        Warnings warnings = Warnings.createWarnings(DriverContext.WarningsMode.IGNORE, new TestWarningsSource("foo"));
        warnings.registerException(new IllegalArgumentException());
    }

    public void testRegisterWarningCollect() {
        Warnings warnings = Warnings.createWarnings(DriverContext.WarningsMode.COLLECT, new TestWarningsSource("foo"));
        warnings.registerWarning("some custom warning");
        assertWarnings("Line 1:1 [foo]: some custom warning");
    }

    public void testRegisterWarningDeduplication() {
        Warnings warnings = Warnings.createWarnings(DriverContext.WarningsMode.COLLECT, new TestWarningsSource("foo"));
        warnings.registerWarning("duplicate warning");
        warnings.registerWarning("duplicate warning");
        warnings.registerWarning("duplicate warning");
        assertWarnings("Line 1:1 [foo]: duplicate warning");
    }

    public void testRegisterWarningMultipleDistinct() {
        Warnings warnings = Warnings.createWarnings(DriverContext.WarningsMode.COLLECT, new TestWarningsSource("foo"));
        warnings.registerWarning("warning A");
        warnings.registerWarning("warning B");
        assertWarnings("Line 1:1 [foo]: warning A", "Line 1:1 [foo]: warning B");
    }

    public void testRegisterWarningCollectFilled() {
        Warnings warnings = Warnings.createWarnings(DriverContext.WarningsMode.COLLECT, new TestWarningsSource("foo"));
        for (int i = 0; i < Warnings.MAX_ADDED_WARNINGS + 1000; i++) {
            warnings.registerWarning("warning " + i);
        }

        String[] expected = new String[Warnings.MAX_ADDED_WARNINGS];
        for (int i = 0; i < Warnings.MAX_ADDED_WARNINGS; i++) {
            expected[i] = "Line 1:1 [foo]: warning " + i;
        }
        assertWarnings(expected);
    }

    public void testRegisterWarningIgnore() {
        Warnings warnings = Warnings.createWarnings(DriverContext.WarningsMode.IGNORE, new TestWarningsSource("foo"));
        warnings.registerWarning("some custom warning");
    }

    public void testRegisterWarningWithView() {
        Warnings warnings = Warnings.createWarnings(DriverContext.WarningsMode.COLLECT, new TestWarningsSource("foo", "view1"));
        warnings.registerWarning("some custom warning");
        assertWarnings("Line 1:1 [foo] (in view [view1]): some custom warning");
    }

    public void testRegisterExceptionDeduplication() {
        Warnings warnings = Warnings.createWarnings(DriverContext.WarningsMode.COLLECT, new TestWarningsSource("foo"));
        warnings.registerException(IllegalArgumentException.class, "duplicate");
        warnings.registerException(IllegalArgumentException.class, "duplicate");
        warnings.registerException(IllegalArgumentException.class, "duplicate");

        assertWarnings(
            "Line 1:1: evaluation of [foo] failed, treating result as null. Only first 20 failures recorded.",
            "Line 1:1: java.lang.IllegalArgumentException: duplicate"
        );
    }

    public void testRegisterExceptionDeduplicationDoesNotConsumeLimit() {
        Warnings warnings = Warnings.createWarnings(DriverContext.WarningsMode.COLLECT, new TestWarningsSource("foo"));

        for (int i = 0; i < Warnings.MAX_ADDED_WARNINGS + 1000; i++) {
            warnings.registerException(new IllegalArgumentException("duplicate"));
        }

        for (int i = 0; i < Warnings.MAX_ADDED_WARNINGS; i++) {
            warnings.registerException(new IllegalStateException(Integer.toString(i)));
        }

        String[] expected = new String[1 + Warnings.MAX_ADDED_WARNINGS];
        expected[0] = "Line 1:1: evaluation of [foo] failed, treating result as null. Only first 20 failures recorded.";
        expected[1] = "Line 1:1: java.lang.IllegalArgumentException: duplicate";
        for (int i = 0; i < Warnings.MAX_ADDED_WARNINGS - 1; i++) {
            expected[i + 2] = "Line 1:1: java.lang.IllegalStateException: " + i;
        }

        assertWarnings(expected);
    }

    public void testRegisterExceptionDeduplicationKeepsDifferentExceptionClasses() {
        Warnings warnings = Warnings.createWarnings(DriverContext.WarningsMode.COLLECT, new TestWarningsSource("foo"));

        warnings.registerException(new IllegalArgumentException("same"));
        warnings.registerException(new IllegalStateException("same"));

        assertWarnings(
            "Line 1:1: evaluation of [foo] failed, treating result as null. Only first 20 failures recorded.",
            "Line 1:1: java.lang.IllegalArgumentException: same",
            "Line 1:1: java.lang.IllegalStateException: same"
        );
    }

    public void testRegisterExceptionDeduplicationKeepsDifferentMessages() {
        Warnings warnings = Warnings.createWarnings(DriverContext.WarningsMode.COLLECT, new TestWarningsSource("foo"));

        warnings.registerException(new IllegalArgumentException("one"));
        warnings.registerException(new IllegalArgumentException("two"));

        assertWarnings(
            "Line 1:1: evaluation of [foo] failed, treating result as null. Only first 20 failures recorded.",
            "Line 1:1: java.lang.IllegalArgumentException: one",
            "Line 1:1: java.lang.IllegalArgumentException: two"
        );
    }

    public void testMixedRegisterExceptionThenWarning() {
        Warnings warnings = Warnings.createWarnings(DriverContext.WarningsMode.COLLECT, new TestWarningsSource("foo"));
        warnings.registerException(new IllegalArgumentException("bad arg"));
        warnings.registerWarning("custom warning");
        assertWarnings(
            "Line 1:1: evaluation of [foo] failed, treating result as null. Only first 20 failures recorded.",
            "Line 1:1: java.lang.IllegalArgumentException: bad arg",
            "Line 1:1 [foo]: custom warning"
        );
    }

    public void testMixedRegisterWarningThenException() {
        Warnings warnings = Warnings.createWarnings(DriverContext.WarningsMode.COLLECT, new TestWarningsSource("foo"));
        warnings.registerWarning("custom warning");
        warnings.registerException(new IllegalArgumentException("bad arg"));
        assertWarnings(
            "Line 1:1 [foo]: custom warning",
            "Line 1:1: evaluation of [foo] failed, treating result as null. Only first 20 failures recorded.",
            "Line 1:1: java.lang.IllegalArgumentException: bad arg"
        );
    }

    public void testMixedSharedLimit() {
        Warnings warnings = Warnings.createWarnings(DriverContext.WarningsMode.COLLECT, new TestWarningsSource("foo"));
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
        assertWarnings(expected);
    }
}
