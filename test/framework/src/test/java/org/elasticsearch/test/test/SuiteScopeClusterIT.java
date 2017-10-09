/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.test.test;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.TestCluster;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

/**
 * This test ensures that the cluster initializion for suite scope is not influencing
 * the tests random sequence due to initializtion using the same random instance.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE)
public class SuiteScopeClusterIT extends ESIntegTestCase {
    private static int ITER = 0;
    private static long[] SEQUENCE = new long[100];
    private static Long CLUSTER_SEED = null;

    @Test
    @SuppressForbidden(reason = "repeat is a feature here")
    @Repeat(iterations = 10, useConstantSeed = true)
    public void testReproducible() throws IOException {
        if (ITER++ == 0) {
            CLUSTER_SEED = cluster().seed();
            for (int i = 0; i < SEQUENCE.length; i++) {
                SEQUENCE[i] = randomLong();
            }
        } else {
            assertEquals(CLUSTER_SEED, Long.valueOf(cluster().seed()));
            for (int i = 0; i < SEQUENCE.length; i++) {
                assertThat(SEQUENCE[i], equalTo(randomLong()));
            }
        }
    }

    @Override
    protected TestCluster buildTestCluster(Scope scope, long seed) throws IOException {
        // produce some randomness
        int iters = between(1, 100);
        for (int i = 0; i < iters; i++) {
            randomLong();
        }
        return super.buildTestCluster(scope, seed);
    }
}
