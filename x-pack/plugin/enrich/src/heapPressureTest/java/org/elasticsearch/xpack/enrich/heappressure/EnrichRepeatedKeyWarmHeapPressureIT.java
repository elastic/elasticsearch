/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.enrich.heappressure;

/**
 * Every document of the bulk carries the same key, and one document went through the pipeline first so the key is a cache hit:
 * every lookup completes synchronously on the dispatching thread with one deep copy, so no lookup responses pile up. The node dies
 * anyway, because every document still retains its re-serialized ~270 KB source for the whole bulk: the accumulation is unbounded
 * even on the hit path, where the enrich cache works exactly as designed.
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
