/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.multi_node;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase;
import org.junit.ClassRule;

import java.util.Arrays;
import java.util.List;

public class RestEsqlIT extends RestEsqlTestCase {
    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster(
        specBuilder -> specBuilder.plugin("mapper-size").plugin("mapper-murmur3")
    );

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @ParametersFactory(argumentFormatting = "%1s")
    public static List<Object[]> modes() {
        return Arrays.stream(Mode.values()).map(m -> new Object[] { m }).toList();
    }

    public RestEsqlIT(Mode mode) {
        super(mode);
    }
}
