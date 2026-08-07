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

    public void testShardCountAndModeArePreserved() {
        IndexMode mode = randomFrom(IndexMode.availableModes());
        int count = between(0, 1000);
        IndexProperties props = new IndexProperties(mode, count);
        assertEquals(count, props.numberOfShards());
        assertEquals(mode, props.indexMode());
    }
}
