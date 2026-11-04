/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.eql.EqlExtraSpecTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.ClassRule;

import java.util.List;

@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class EqlExtraViaEsqlIT extends EqlExtraSpecTestCase {

    @ClassRule
    public static final ElasticsearchCluster cluster = EqlTestCluster.CLUSTER;

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public EqlExtraViaEsqlIT(
        String query,
        String name,
        List<long[]> eventIds,
        String[] joinKeys,
        Integer size,
        Integer maxSamplesPerKey,
        Boolean allowPartialSearchResults,
        Boolean allowPartialSequenceResults,
        Boolean expectShardFailures
    ) {
        super(
            query,
            name,
            eventIds,
            joinKeys,
            size,
            maxSamplesPerKey,
            allowPartialSearchResults,
            allowPartialSequenceResults,
            expectShardFailures
        );
    }

    @Override
    protected ObjectPath runQuery(String index, String query) throws Exception {
        assumeTrue(
            "requires the ES|QL EQL command",
            clusterHasCapability("POST", "/_query", List.of(), EqlViaEsql.CAPABILITIES).orElse(false)
        );
        ObjectPath eql = super.runQuery(index, query);
        EqlViaEsql.Options options = new EqlViaEsql.Options(
            size(),
            fetchSize(),
            resultPosition(),
            timestamp(),
            eventCategory(),
            tiebreaker(),
            maxSamplesPerKey(),
            allowPartialSequenceResults()
        );
        return EqlViaEsql.runAndCompare(client(), index, query, options, idField(), eql);
    }
}
