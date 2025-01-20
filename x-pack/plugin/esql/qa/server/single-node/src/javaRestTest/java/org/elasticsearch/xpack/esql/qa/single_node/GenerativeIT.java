/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.qa.rest.generative.GenerativeRestTest;
import org.junit.ClassRule;

@AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/102084")
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class GenerativeIT extends GenerativeRestTest {
    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}
