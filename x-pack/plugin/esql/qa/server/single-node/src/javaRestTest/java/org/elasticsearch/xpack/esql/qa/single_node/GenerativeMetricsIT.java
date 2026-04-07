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
import org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator;
import org.elasticsearch.xpack.esql.generator.command.CommandGenerator;
import org.elasticsearch.xpack.esql.qa.rest.generative.GenerativeRestTest;
import org.junit.ClassRule;

@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class GenerativeMetricsIT extends GenerativeRestTest {
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
    protected CommandGenerator sourceCommand() {
        return EsqlQueryGenerator.timeSeriesSourceCommand();
    }

    @Override
    protected boolean requiresTimeSeries() {
        return true;
    }
}
