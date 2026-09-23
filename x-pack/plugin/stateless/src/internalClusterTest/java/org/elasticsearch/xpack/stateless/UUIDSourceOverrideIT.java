/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless;

import org.elasticsearch.common.TestUUIDSource;
import org.elasticsearch.common.UUIDSource;

import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.startsWith;

/**
 * Verifies that auto-generated document ids are stubbed by {@link TestUUIDSource#withUUIDSource}.
 */
public class UUIDSourceOverrideIT extends AbstractStatelessPluginIntegTestCase {

    public void testStubbedDocumentIds() throws Exception {
        startMasterAndIndexNode();
        startSearchNode();
        final var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);

        final var numDocs = between(5, 50);
        TestUUIDSource.withUUIDSource(new SequentialUUIDSource(), () -> indexDocsAndRefresh(indexName, numDocs));

        assertResponse(prepareSearch(indexName).setSize(numDocs), response -> {
            assertHitCount(response, numDocs);
            for (var hit : response.getHits()) {
                assertThat("document id should come from the stub", hit.getId(), startsWith("stub-"));
            }
        });
    }

    private static class SequentialUUIDSource implements UUIDSource {
        private final AtomicInteger counter = new AtomicInteger();

        @Override
        public String base64UUID() {
            return "stub-" + counter.getAndIncrement();
        }

        @Override
        public String base64TimeBasedKOrderedUUIDWithHash(OptionalInt hash) {
            return base64UUID();
        }
    }
}
