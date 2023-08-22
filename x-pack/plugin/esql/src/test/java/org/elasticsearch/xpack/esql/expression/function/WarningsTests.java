/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.tree.Source;

public class WarningsTests extends ESTestCase {
    public void testRegister() {
        Warnings warnings = new Warnings(new Source(1, 1, "foo"));
        warnings.registerException(new IllegalArgumentException());
        assertCriticalWarnings(
            "Line 1:2: evaluation of [foo] failed, treating result as null. Only first 20 failures recorded.",
            "java.lang.IllegalArgumentException: null"
        );
    }

    public void testRegisterFilled() {
        Warnings warnings = new Warnings(new Source(1, 1, "foo"));
        for (int i = 0; i < Warnings.MAX_ADDED_WARNINGS + 1000; i++) {
            warnings.registerException(new IllegalArgumentException(Integer.toString(i)));
        }

        String[] expected = new String[21];
        expected[0] = "Line 1:2: evaluation of [foo] failed, treating result as null. Only first 20 failures recorded.";
        for (int i = 0; i < Warnings.MAX_ADDED_WARNINGS; i++) {
            expected[i + 1] = "java.lang.IllegalArgumentException: " + i;
        }

        assertCriticalWarnings(expected);
    }
}
