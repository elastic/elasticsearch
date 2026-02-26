/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.qa.test;

import java.util.concurrent.StructuredTaskScope;

import static org.elasticsearch.entitlement.qa.test.EntitlementTest.ExpectedAccess.PLUGINS;

@SuppressWarnings({ "unused" /* called via reflection */, "removal" })
class StructuredTaskScopeActions {
    private StructuredTaskScopeActions() {}

    @EntitlementTest(expectedAccess = PLUGINS)
    static void java_util_concurrent_StructuredTaskScope$$init() {
        try (var scope = new StructuredTaskScope<>()) {
            // opening and closing is enough to test the entitlement check
        }
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void java_util_concurrent_StructuredTaskScope$$init_String_ThreadFactory() {
        try (var scope = new StructuredTaskScope<>("test", Thread.ofVirtual().factory())) {
            // opening and closing is enough to test the entitlement check
        }
    }
}
