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

import java.io.IOException;
import java.nio.file.Path;

/**
 * Tests that unit tests are covered by entitlement checks.
 */
public class JUnitMetaTests extends ESTestCase {

    public void testForbiddenActionThrows() {
        assertThrows(NotEntitledException.class, ()-> Path.of(".").toRealPath());
    }

    @WithoutEntitlements
    public void testForbiddenActionAllowed() throws IOException {
        // ("real paths" are also absolute paths)
        System.out.println(Path.of(".").toRealPath());
        assertTrue(Path.of(".").toRealPath().toString().startsWith("/"));
    }
}
