/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.enrich.heappressure;

/**
 * Every document has the same key and the cache is warm, so no lookup is a search; only the enriched documents fill the heap.
 */
public class EnrichRepeatedKeyWarmHeapPressureIT extends EnrichWideDocumentHeapPressureTestCase {

    @Override
    protected int keyIndexForBulkDocument(int i) {
        return 0;
    }

    @Override
    protected boolean primeCache() {
        return true;
    }

    public void testRepeatedKeyOnWarmCacheDoesNotKillTheIngestNode() throws Exception {
        runScenario();
    }
}
