/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy;

import org.elasticsearch.entitlement.runtime.api.NotEntitledException;
import org.elasticsearch.test.ESTestCase;

import java.nio.file.Path;

/**
 * Ensures that unit tests are subject to entitlement checks.
 */
@ESTestCase.WithEntitlementsOnTestCode
public class JUnitEntitlementMetaTests extends ESTestCase {
    public void testForbiddenActionThrows() {
        assertThrows(NotEntitledException.class, () -> Path.of(".").toRealPath());
    }
}
