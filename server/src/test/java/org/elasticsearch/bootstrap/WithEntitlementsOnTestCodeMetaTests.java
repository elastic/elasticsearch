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
import org.elasticsearch.entitlement.runtime.api.NotEntitledException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.ESTestCase.WithEntitlementsOnTestCode;

import java.nio.file.Path;

/**
 * A version of {@link EntitlementMetaTests} that tests {@link WithEntitlementsOnTestCode}.
 *
 * @see EntitlementMetaTests
 * @see WithoutEntitlementsMetaTests
 */
@WithEntitlementsOnTestCode
public class WithEntitlementsOnTestCodeMetaTests extends ESTestCase {
    /**
     * {@link WithEntitlementsOnTestCode} should not affect this, since the sensitive method
     * is called from server code. The self-test should pass as usual.
     */
    public void testSelfTestPasses() {
        Elasticsearch.entitlementSelfTest();
    }

    @SuppressForbidden(reason = "Testing that a forbidden API is disallowed")
    public void testForbiddenActionDenied() {
        assertThrows(NotEntitledException.class, () -> Path.of(".").toRealPath());
    }
}
