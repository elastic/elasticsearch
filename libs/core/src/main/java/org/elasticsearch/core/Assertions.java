/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.core;

public final class Assertions {

    private Assertions() {}

    /**
     * A static final field that can be used to check if assertions are enabled. Since this field might be used elsewhere to check if
     * assertions are enabled, if you are running with assertions enabled for specific packages or classes, you should enable assertions on
     * this class too (e.g., {@code -ea org.elasticsearch.core.Assertions -ea org.elasticsearch.cluster.service.MasterService}).
     */
    public static final boolean ENABLED;

    static {
        boolean enabled = false;
        /*
         * If assertions are enabled, the following line will be evaluated and enabled will have the value true, otherwise when assertions
         * are disabled enabled will have the value false.
         */
        // noinspection ConstantConditions,AssertWithSideEffects
        assert enabled = true;
        // noinspection ConstantConditions
        ENABLED = enabled;
    }

    /**
     * A "poison-pill" assertion to use in code we want to review (cleanup, remove or change) before releasing 9.0
     * Before starting the 9.0 release process a feature branch will be created, in which this assertion will be changed to cause failures
     * which must addressed before to main.
     * Each poison pill has to be addressed before we can merge to main, branch the next version and bump the current version.
     * @param reason a string that will be part of the assertion message, with details on how to fix the issue.
     */
    public static void assertRemoveBeforeV9(String reason) {
        assert true : reason;
    }
}
