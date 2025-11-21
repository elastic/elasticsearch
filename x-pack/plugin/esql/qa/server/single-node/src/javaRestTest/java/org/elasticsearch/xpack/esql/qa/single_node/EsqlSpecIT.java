/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.plugin.ComputeService;
import org.elasticsearch.xpack.esql.qa.rest.EsqlSpecTestCase;
import org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;

@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class EsqlSpecIT extends EsqlSpecTestCase {
    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster(
        spec -> spec.plugin("inference-service-test").setting("logger." + ComputeService.class.getName(), "DEBUG") // So we log a profile
    );

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public EsqlSpecIT(String fileName, String groupName, String testName, Integer lineNumber, CsvTestCase testCase, String instructions) {
        super(fileName, groupName, testName, lineNumber, testCase, instructions);
    }

    @Override
    protected boolean enableRoundingDoubleValuesOnAsserting() {
        // This suite runs with more than one node and three shards in serverless
        return cluster.getNumNodes() > 1;
    }

    @Override
    protected boolean supportsSourceFieldMapping() {
        return cluster.getNumNodes() == 1;
    }

    @Override
    protected boolean supportsExponentialHistograms() {
        return RestEsqlTestCase.hasCapabilities(
            client(),
            List.of(EsqlCapabilities.Cap.EXPONENTIAL_HISTOGRAM_PRE_TECH_PREVIEW_V1.capabilityName())
        );
    }

    @Before
    public void configureChunks() throws IOException {
        assumeTrue("test clusters were broken", testClustersOk);
        boolean smallChunks = randomBoolean();
        Request request = new Request("PUT", "/_cluster/settings");
        XContentBuilder builder = JsonXContent.contentBuilder().startObject().startObject("persistent");
        builder.field(PlannerSettings.VALUES_LOADING_JUMBO_SIZE.getKey(), smallChunks ? "1kb" : null);
        request.setJsonEntity(Strings.toString(builder.endObject().endObject()));
        assertOK(client().performRequest(request));
    }
}
