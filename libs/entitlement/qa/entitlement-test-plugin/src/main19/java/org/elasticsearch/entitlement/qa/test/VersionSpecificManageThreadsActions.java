/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.qa.test;

import org.elasticsearch.core.SuppressForbidden;

import java.util.concurrent.ForkJoinPool;

import static org.elasticsearch.entitlement.qa.test.EntitlementTest.ExpectedAccess.PLUGINS;

@SuppressForbidden(reason = "testing entitlements")
@SuppressWarnings("unused") // used via reflection
class VersionSpecificManageThreadsActions {
    private VersionSpecificManageThreadsActions() {}

    @EntitlementTest(expectedAccess = PLUGINS)
    static void java_util_concurrent_ForkJoinPool$setParallelism() {
        ForkJoinPool.commonPool().setParallelism(ForkJoinPool.commonPool().getParallelism());
    }
}
