/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.mixed;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.qa.rest.AllSupportedFieldsTestCase;
import org.junit.ClassRule;

/**
 * Fetch all field types in a mixed version cluster, simulating a rolling upgrade.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class AllSupportedFieldsIT extends AllSupportedFieldsTestCase {
    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.mixedVersionCluster();

    public AllSupportedFieldsIT(MappedFieldType.FieldExtractPreference extractPreference, IndexMode indexMode) {
        super(extractPreference, indexMode);
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}
