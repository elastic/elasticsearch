/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch;

import org.apache.lucene.tests.util.RamUsageTester;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Behavioural tests for {@link TransportVersion#ramBytesUsed()}. This type does not implement Lucene's
 * {@link org.apache.lucene.util.Accountable} because {@code TransportVersion} is referenced from modules that
 * depend on {@code :server} with {@code transitive = false} and therefore do not have Lucene on their compile
 * classpath.
 */
public class TransportVersionRamBytesUsedTests extends ESTestCase {

    public void testRamBytesUsedNeverUnderCountsActualHeap() {
        TransportVersion patch = new TransportVersion("patch-version-name", 2, null);
        TransportVersion instance = new TransportVersion("head-version-name", 1, patch);
        long estimate = instance.ramBytesUsed();
        long actual = RamUsageTester.ramUsed(instance);
        assertThat(
            "estimate under-counts retained heap: estimate=" + estimate + " actual=" + actual,
            estimate,
            greaterThanOrEqualTo(actual)
        );
    }

    /**
     * Non-tautology check: a longer name and patch-version chain must increase the reported size.
     */
    public void testRamBytesUsedGrowsWithNameAndPatchChain() {
        TransportVersion unnamed = new TransportVersion(1);
        TransportVersion patch = new TransportVersion("patch-version-name", 2, null);
        TransportVersion withChain = new TransportVersion("head-version-name", 1, patch);
        assertThat(withChain.ramBytesUsed(), greaterThan(unnamed.ramBytesUsed()));
    }
}
