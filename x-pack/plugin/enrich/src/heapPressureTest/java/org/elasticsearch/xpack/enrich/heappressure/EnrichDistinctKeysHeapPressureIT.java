/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.enrich.heappressure;

/**
 * Every document has a distinct key and the cache is cold, so every lookup is a search. The scenario with the least headroom.
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
