/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.core;

/**
 * Provides a static final field that can be used to check if assertions are enabled. Since this field might be used elsewhere to check if
 * assertions are enabled, if you are running with assertions enabled for specific packages or classes, you should enable assertions on this
 * class too (e.g., {@code -ea org.elasticsearch.core.Assertions -ea org.elasticsearch.cluster.service.MasterService}).
 */
public final class Assertions {

    private Assertions() {}

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

}
