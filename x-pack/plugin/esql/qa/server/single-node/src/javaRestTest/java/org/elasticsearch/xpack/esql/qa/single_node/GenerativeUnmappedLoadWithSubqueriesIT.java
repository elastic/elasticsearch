/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xpack.esql.generator.GenerativeFeature;
import org.elasticsearch.xpack.esql.qa.rest.generative.GenerativeRestTest;
import org.junit.ClassRule;

import java.util.Set;

/**
 * Generative suite exercising {@code unmapped_fields="load"} together with {@code FROM (...)} subqueries, so the
 * random pipelines cover the load + subquery interaction supported since #142033.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
@TestLogging(value = "org.elasticsearch.xpack.esql.plugin.ComputeService", reason = "see plans on failure")
public class GenerativeUnmappedLoadWithSubqueriesIT extends GenerativeRestTest {
    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected boolean supportsSourceFieldMapping() {
        return cluster.getNumNodes() == 1;
    }

    @Override
    protected Set<GenerativeFeature> enabledFeatures() {
        return Set.of(GenerativeFeature.UNMAPPED_FIELDS_LOAD, GenerativeFeature.SUBQUERIES);
    }
}
