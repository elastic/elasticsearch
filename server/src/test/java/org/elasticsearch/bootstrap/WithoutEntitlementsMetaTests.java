/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.bootstrap;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.ESTestCase.WithoutEntitlements;

import java.io.IOException;
import java.nio.file.Path;

/**
 * A version of {@link EntitlementMetaTests} that tests {@link WithoutEntitlements}.
 *
 * @see EntitlementMetaTests
 * @see WithEntitlementsOnTestCodeMetaTests
 */
@WithoutEntitlements
public class WithoutEntitlementsMetaTests extends ESTestCase {
    /**
     * Without enforcement of entitlements, {@link Elasticsearch#entitlementSelfTest} will fail and throw.
     */
    public void testSelfTestFails() {
        assertThrows(IllegalStateException.class, Elasticsearch::entitlementSelfTest);
    }

    /**
     * A forbidden action called from test code should be allowed,
     * with or without {@link WithoutEntitlements}.
     */
    @SuppressForbidden(reason = "Testing that a forbidden API is allowed under these circumstances")
    public void testForbiddenActionAllowed() throws IOException {
        // If entitlements were enforced, this would throw
        Path.of(".").toRealPath();
    }
}
