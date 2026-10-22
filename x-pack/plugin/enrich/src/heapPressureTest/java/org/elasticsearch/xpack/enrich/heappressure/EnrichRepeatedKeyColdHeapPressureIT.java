/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.enrich.heappressure;

/**
 * Every document of the bulk carries the same key, with a cold cache: the enrich cache does not coalesce in-flight misses, so all
 * 2,048 lookups go to the enrich index and each response is parsed and copied separately, as on a node that restarted under load.
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
