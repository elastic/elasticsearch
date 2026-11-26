/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.enrich.heappressure;

/**
 * Every document of the bulk carries a distinct key that matches one wide source document, with a cold cache: every lookup misses,
 * the coordinator fans the bulk out in 128-lookup batches, and each response is expanded into 128 distinct maps on a search thread
 * while the re-serialized sources accumulate for the whole bulk. The steady-state shape of a wide-enrich workload.
 */
public class EnrichDistinctKeysHeapPressureIT extends EnrichWideDocumentHeapPressureTestCase {

    @Override
    protected int keyIndexForBulkDocument(int i) {
        return i;
    }

    @Override
    protected boolean primeCache() {
        return false;
    }

    public void testDistinctKeysDoNotKillTheIngestNode() throws Exception {
        runScenario();
    }
}
