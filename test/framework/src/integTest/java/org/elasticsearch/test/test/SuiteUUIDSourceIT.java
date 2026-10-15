/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.test;

import com.carrotsearch.randomizedtesting.annotations.Repeat;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.AfterClass;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

/** Test-method IDs reproduce independently of the fixture indexed in {@link #setupSuiteScopeCluster}. */
@ESIntegTestCase.SuiteScopeTestCase
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2, numClientNodes = 0, supportsDedicatedMasters = false)
public class SuiteUUIDSourceIT extends ESIntegTestCase {

    private static List<GeneratedId> fixtureIds;
    private static List<GeneratedId> expectedMethodIds;

    @Override
    protected void setupSuiteScopeCluster() {
        fixtureIds = indexDocuments("fixture");
        logger.info("suite fixture IDs and shards: {}", fixtureIds);
    }

    @SuppressForbidden(reason = "Constant-seed repetitions verify isolation of method UUIDs from the suite fixture")
    @Repeat(iterations = 3, useConstantSeed = true)
    public void testMethodIdsReproduceWithSuiteFixture() {
        for (var document : fixtureIds) {
            assertTrue(client().prepareGet("fixture", document.id()).get().isExists());
        }
        try {
            final var ids = indexDocuments("method");
            if (expectedMethodIds == null) {
                expectedMethodIds = ids;
            } else {
                assertEquals(expectedMethodIds, ids);
            }
            logger.info("method IDs and shards: {}", ids);
        } finally {
            assertAcked(indicesAdmin().prepareDelete("method"));
        }
    }

    public void testUnrelatedIndexing() {
        try {
            indexDocuments("unrelated");
        } finally {
            assertAcked(indicesAdmin().prepareDelete("unrelated"));
        }
    }

    private List<GeneratedId> indexDocuments(String index) {
        assertAcked(prepareCreate(index).setSettings(indexSettings(3, 0)).setMapping("value", "type=integer"));
        ensureGreen(index);
        final var ids = new ArrayList<GeneratedId>();
        for (String node : Arrays.stream(internalCluster().getNodeNames()).sorted().toList()) {
            for (int i = 0; i < 10; i++) {
                final var response = internalCluster().client(node).prepareIndex(index).setSource("value", i).get();
                assertEquals(DocWriteResponse.Result.CREATED, response.getResult());
                ids.add(new GeneratedId(response.getId(), response.getShardId().id()));
            }
        }
        return List.copyOf(ids);
    }

    @AfterClass
    public static void clearExpectedIds() {
        fixtureIds = null;
        expectedMethodIds = null;
    }

    private record GeneratedId(String id, int shard) {}
}
