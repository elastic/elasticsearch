/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.qa.rest.AllSupportedFieldsTestCase;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Map;

/**
 * Simple test for fetching all supported field types.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class AllSupportedFieldsIT extends AllSupportedFieldsTestCase {
    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster(c -> {});

    public AllSupportedFieldsIT(MappedFieldType.FieldExtractPreference extractPreference, IndexMode indexMode) {
        super(extractPreference, indexMode);
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void assertMinimumVersion(TransportVersion coordinatorVersion, Map<String, Object> responseMap) throws IOException {
        Map<String, Object> profile = (Map<String, Object>) responseMap.get("profile");
        Integer minimumVersion = (Integer) profile.get("minimumVersion");
        assertNotNull(minimumVersion);
        int minVersionInt = minimumVersion;
        assertEquals(minVersion().id(), minVersionInt);
        assertEquals(TransportVersion.current().id(), minVersionInt);
    }
}
