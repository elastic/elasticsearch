/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.diskbbq;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Map;

public class DiskBbqBasicLicenseIT extends ESRestTestCase {
    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.license.self_generated.type", "basic")
        .setting("xpack.security.enabled", "true")
        .user("x_pack_rest_user", "x-pack-test-password")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("x_pack_rest_user", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    /**
     * The default index type for dense_vector with a basic license is int8_hnsw.
     */
    public void testDefaultIndexOptionsForDenseVector() throws IOException {
        final String index = "test_default_index_options";
        final String mapping = """
            {
              "properties": {
                "vector": {
                  "type": "dense_vector",
                  "dims": 5
                }
              }
            }
            """;

        assertTrue(createIndex(index, null, mapping).isAcknowledged());

        Map<String, Object> mappings = getIndexMappingAsMap(index);
        @SuppressWarnings("unchecked")
        Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");
        @SuppressWarnings("unchecked")
        Map<String, Object> vector = (Map<String, Object>) properties.get("vector");

        assertEquals("dense_vector", vector.get("type"));
        assertEquals(5, ((Number) vector.get("dims")).intValue());
        assertEquals(true, vector.get("index"));
        assertEquals("cosine", vector.get("similarity"));
        @SuppressWarnings("unchecked")
        Map<String, Object> indexOptions = (Map<String, Object>) vector.get("index_options");
        assertEquals("int8_hnsw", indexOptions.get("type"));
    }
}
