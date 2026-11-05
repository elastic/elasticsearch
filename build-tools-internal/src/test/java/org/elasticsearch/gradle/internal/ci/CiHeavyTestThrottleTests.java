/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.ci;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class CiHeavyTestThrottleTests {

    @Test
    public void testTasksThatStartClustersAreHeavy() {
        for (String name : new String[] {
            "javaRestTest",
            "yamlRestTest",
            "yamlRestCompatTest",
            "internalClusterTest",
            "pooledInternalClusterTest",
            "csvSpecTests",
            "csvSpecTestsCurrentToCurrent",
            "workloadIdentityYamlRestTest",
            "integTest",
            "bcUpgradeTest" }) {
            assertThat(name, CiHeavyTestThrottle.isHeavy(name), equalTo(true));
        }
        for (String name : new String[] { "test", "test27", "benchmarkTest", "testAutomatonPatterns", "gcsThirdPartyUnitTest" }) {
            assertThat(name, CiHeavyTestThrottle.isHeavy(name), equalTo(false));
        }
    }

    @Test
    public void testDefaultLimitIsHalfTheWorkers() {
        assertThat(CiHeavyTestThrottle.defaultLimit(16), equalTo(8));
        assertThat(CiHeavyTestThrottle.defaultLimit(1), equalTo(1));
    }
}
