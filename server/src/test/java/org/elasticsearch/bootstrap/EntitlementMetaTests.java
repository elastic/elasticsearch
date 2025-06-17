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

import java.io.IOException;
import java.nio.file.Path;

/**
 * Ensures that unit tests are subject to entitlement checks.
 * This is a "meta test" because it tests that the tests are working:
 * if these tests fail, it means other tests won't be correctly detecting
 * entitlement enforcement errors.
 * <p>
 * It may seem strange to have this test where it is, rather than in the entitlement library.
 * There's a reason for that.
 * <p>
 * To exercise entitlement enforcement, we must attempt an operation that should be denied.
 * This necessitates some operation that fails the entitlement check,
 * and it must be in production code (or else we'd also need {@link WithEntitlementsOnTestCode},
 * and we don't want to require that here).
 * Naturally, there are very few candidates, because most code doesn't fail entitlement checks:
 * really just the entitlement self-test we do at startup. Hence, that's what we use here.
 * <p>
 * Even then, we cannot call this from someplace outside the server, because in other places,
 * we deliberately don't check entitlements on dependencies that aren't used in production,
 * and other places like {@code libs} don't depend on server at runtime. Hence, this test
 * must be in {@code server}, rather than {@code libs/entitlement}, even if the latter
 * seems like a more natural choice.
 *
 * @see WithoutEntitlementsMetaTests
 * @see WithEntitlementsOnTestCodeMetaTests
 */
public class EntitlementMetaTests extends ESTestCase {
    public void testSelfTestPasses() {
        Elasticsearch.entitlementSelfTest();
    }

    @SuppressForbidden(reason = "Testing that a forbidden API is allowed under these circumstances")
    public void testForbiddenActionAllowedInTestCode() throws IOException {
        Path.of(".").toRealPath();
    }
}
