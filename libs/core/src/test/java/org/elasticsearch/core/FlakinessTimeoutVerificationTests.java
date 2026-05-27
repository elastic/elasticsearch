/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.core;

import org.elasticsearch.test.ESTestCase;

/**
 * VERIFICATION ONLY — DO NOT MERGE.
 *
 * Exists solely so the flakiness-detection PR pipeline picks up a changed test
 * file and emits a real batch step running this class. The single test sleeps
 * long enough to blow past the wrapNeverFail inner timeout (set to outer-2 via
 * {@code DEFAULT_AGENT_CONFIG.timeoutInMinutes}). The build is expected to:
 *
 * <ul>
 *   <li>have the inner GNU {@code timeout} fire (rc=124 or 137 if SIGKILL is
 *       needed to take down the gradle daemon),</li>
 *   <li>append a Buildkite annotation "timed out after Nm",</li>
 *   <li>exit 0 so step.state stays "passed" and the GitHub commit status for
 *       the step is SUCCESS.</li>
 * </ul>
 *
 * Without the timeout fix this would have ended up as step.state="timed_out"
 * and a red GitHub check on the PR.
 */
public class FlakinessTimeoutVerificationTests extends ESTestCase {

    public void testSleepsPastInnerTimeout() throws InterruptedException {
        // Sleep well beyond the inner timeout (outer 4m → inner 2m in this
        // branch) so the timeout path is exercised regardless of how much
        // setup gradle had to do before the test class started executing.
        Thread.sleep(5L * 60L * 1000L);
    }
}
