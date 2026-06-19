/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.root;

import org.elasticsearch.test.ESTestCase;

/**
 * Intentional flakiness-detection probe for PR #151713 (flakiness-detection observability). NOT FOR MERGE.
 *
 * <p>This test is deterministically skipped under the regular PR build (where {@code tests.iters} defaults to 1) so it
 * never reddens normal CI, and is ~50% flaky only under the flakiness-detection runner's high iteration count
 * ({@code -Dtests.iters=100}). Over 100 iterations that yields failing runs with overwhelming probability, so the
 * pipeline records a {@code flaky_detected} outcome for the unit batch step.
 */
public class FlakinessDetectionProbeTests extends ESTestCase {

    public void testIntentionalFlakinessProbe() {
        assumeTrue("only runs under the flakiness-detection high-iteration run", Integer.getInteger("tests.iters", 1) > 1);
        assertFalse("intentional ~50% probe failure to exercise flaky_detected", randomBoolean());
    }
}
