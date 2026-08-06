/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.index;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;

public class IndexPropertiesTests extends ESTestCase {

    public void testLookupDefaultsToOneShard() {
        IndexProperties props = new IndexProperties(IndexMode.LOOKUP);
        assertEquals(1, props.numberOfShards());
        assertEquals(IndexMode.LOOKUP, props.indexMode());
    }

    public void testNonLookupModesDefaultToZeroShards() {
        for (IndexMode mode : IndexMode.availableModes()) {
            if (mode == IndexMode.LOOKUP) {
                continue;
            }
            IndexProperties props = new IndexProperties(mode);
            assertEquals("expected 0 shards for mode " + mode, 0, props.numberOfShards());
        }
    }

    public void testExplicitShardCountIsPreserved() {
        IndexMode mode = randomFrom(IndexMode.availableModes());
        int count = between(0, 1000);
        IndexProperties props = new IndexProperties(mode, count);
        assertEquals(count, props.numberOfShards());
        assertEquals(mode, props.indexMode());
    }
}
