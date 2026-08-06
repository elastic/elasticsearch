/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.dissect;

import org.elasticsearch.test.ESTestCase;

/**
 * THROWAWAY probe for verifying the flakiness-detection pre-flight compile gate on a real build.
 *
 * This file intentionally does NOT compile: it references an undefined symbol so that
 * {@code :libs:dissect:compileTestJava} fails. The flakiness detector picks up this newly added
 * {@code *Tests.java}, the pre-flight compile gate compiles the affected source set, and the compile
 * fails - which is exactly the scenario under test. Do not merge; delete once the build is observed.
 */
public class FlakinessGateProbeTests extends ESTestCase {

    public void testCompileGateTripsOnPurpose() {
        // `thisSymbolDoesNotExist` is undefined on purpose, so compilation fails.
        assertEquals("flakiness-gate-probe", thisSymbolDoesNotExist);
    }
}
