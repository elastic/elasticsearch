/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;

public class EvilSystemPropertyTests extends ESTestCase {

    @SuppressForbidden(reason = "manipulates system properties for testing")
    public void testMaxNumShards() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> IndexMetadata.buildNumberOfShardsSetting().get(Settings.builder().put("index.number_of_shards", 1025).build())
        );
        assertEquals("Failed to parse value [1025] for setting [index.number_of_shards] must be <= 1024", exception.getMessage());

        Integer numShards = IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(Settings.builder().put("index.number_of_shards", 100).build());
        assertEquals(100, numShards.intValue());
        int limit = randomIntBetween(1, 10);
        System.setProperty("es.index.max_number_of_shards", Integer.toString(limit));
        try {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> IndexMetadata.buildNumberOfShardsSetting().get(Settings.builder().put("index.number_of_shards", 11).build())
            );
            assertEquals("Failed to parse value [11] for setting [index.number_of_shards] must be <= " + limit, e.getMessage());
        } finally {
            System.clearProperty("es.index.max_number_of_shards");
        }
    }
}
