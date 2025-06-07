/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.ESTestCase.WithoutEntitlements;

import java.io.IOException;
import java.nio.file.Path;

@WithoutEntitlements
public class WithoutEntitlementsTests extends ESTestCase {
    public void testForbiddenActionAllowed() throws IOException {
        System.out.println(Path.of(".").toRealPath());
        assertTrue(Path.of(".").toRealPath().toString().startsWith("/")); // ("real paths" are also absolute paths)
    }
}
