/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rollup.v2;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.rollup.v2.RollupShardIndexer.BucketKey;

import java.util.List;

public class RollupShardIndexerTests extends ESTestCase {
    public void testBucketKey() {
        long timestamp = randomNonNegativeLong();
        List<Object> groups = List.of("group1", "group2", 3, 4L, 5.0f);
        BucketKey key1 = new BucketKey(timestamp, groups);
        BucketKey key2 = new BucketKey(timestamp, groups);
        assertEquals(key1, key2);
        assertEquals(key1.hashCode(), key2.hashCode());
        assertEquals(key1.toString(), key2.toString());
    }

}
