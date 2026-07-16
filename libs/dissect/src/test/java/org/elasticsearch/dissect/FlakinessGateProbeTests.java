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
 * DO NOT MERGE. Throwaway probe to verify the flakiness-detection pre-flight
 * compile gate end-to-end: this file intentionally does not compile, so the
 * `flakiness-detection:precompile` step for `:libs:dissect:compileTestJava`
 * fails, the re-run batches are skipped, and the analyze step should still run
 * and record a single `build_failed` outcome. See PR stack #153803/#153806/#153807.
 */
public class FlakinessGateProbeTests extends ESTestCase {
    public void testDeliberateCompileError() {
        // Intentional compile error: this symbol does not exist.
        ThisSymbolDoesNotExist unused = null;
    }
}
