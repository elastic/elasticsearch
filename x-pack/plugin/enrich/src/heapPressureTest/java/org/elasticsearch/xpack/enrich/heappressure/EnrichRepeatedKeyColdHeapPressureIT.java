/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.enrich.heappressure;

/**
 * Every document has the same key and the cache is cold, as on a node that restarted under load. Fixed by charging pipeline growth
 * to indexing pressure (#163380) and sending identical lookups of a batch as one search (#166459).
 */
public class EnrichRepeatedKeyColdHeapPressureIT extends EnrichWideDocumentHeapPressureTestCase {

    @Override
    protected int keyIndexForBulkDocument(int i) {
        return 0;
    }

    @Override
    protected boolean primeCache() {
        return false;
    }

    public void testRepeatedKeyOnColdCacheDoesNotKillTheIngestNode() throws Exception {
        runScenario();
    }
}
