/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.aggregations;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.core.Booleans;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.ClassRule;

public class AggregationsClientYamlTestSuiteIT extends ESClientYamlSuiteTestCase {
    @ClassRule
    public static ElasticsearchCluster cluster = makeCluster();

    private static ElasticsearchCluster makeCluster() {
        var cluster = ElasticsearchCluster.local().module("aggregations").module("lang-painless").feature(FeatureFlag.TIME_SERIES_MODE);

        // On Serverless, we want to disallow scripted metrics aggs per default.
        // The following override allows us to still run the scripted metrics agg tests without breaking bwc.
        boolean disableAllowListPerDefault = Booleans.parseBoolean(
            System.getProperty("tests.disable_scripted_metric_allow_list_per_default", "false")
        );
        if (disableAllowListPerDefault) {
            return cluster.setting("search.aggs.only_allowed_metric_scripts", "false").build();
        }
        return cluster.build();
    }

    public AggregationsClientYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return ESClientYamlSuiteTestCase.createParameters();
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}
