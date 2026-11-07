/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.grpc;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.client.Request;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.esql.datasource.grpc.BlockingSatelliteServer;
import org.elasticsearch.xpack.esql.datasources.Federation;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

/**
 * A node keeps the declaration of a satellite and checks the declaration id the satellite gives in its schema, a call ES makes at
 * every query anyway. While the id is unchanged the capability action is not repeated; when the satellite changes its declaration
 * and its id, the next query of every node sends the fragment the new declaration allows, in either direction, with no wait for a
 * time to live and no failed query. The satellite runs in this JVM and records the fragments it is sent.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class SatelliteDeclarationIdIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .nodes(2)
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting(Federation.FEDERATION_ENABLED.getKey(), "true")
        .setting("xpack.ml.enabled", "false")
        .jvmArg("--add-opens=java.base/java.nio=ALL-UNNAMED")
        .jvmArg("-Darrow.allocation.manager.type=Unsafe")
        .build();

    private static final String SCAN_ONLY = declaration("scan-only", "[\"scan\"]", "[]", "[\"integer\"]");
    private static final String WITH_FILTER = declaration(
        "with-filter",
        "[\"scan\",\"filter\"]",
        "[\"greater_than\"]",
        "[\"integer\",\"boolean\"]"
    );

    private static String declaration(String id, String plan, String functions, String types) {
        return "{\"specVersions\":[0],\"fragmentFormats\":[\"json\"],\"plan\":"
            + plan
            + ",\"functions\":"
            + functions
            + ",\"types\":"
            + types
            + ",\"declarationId\":\""
            + id
            + "\"}";
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    private void register(BlockingSatelliteServer satellite) throws IOException {
        Request source = new Request("PUT", "/_query/data_source/id_src");
        source.setJsonEntity("{\"type\": \"flight\"}");
        client().performRequest(source);
        Request dataset = new Request("PUT", "/_query/dataset/id_ds");
        dataset.setJsonEntity("{\"data_source\": \"id_src\", \"resource\": \"flight://127.0.0.1:" + satellite.port() + "/t\"}");
        client().performRequest(dataset);
    }

    /** Several queries, so that both nodes coordinate some of them. */
    private void runQueries(int count) throws IOException {
        for (int i = 0; i < count; i++) {
            Request query = new Request("POST", "/_query");
            query.setJsonEntity("{\"query\": \"FROM id_ds | WHERE id > 1 | LIMIT 10\"}");
            client().performRequest(query);
        }
    }

    private static List<String> since(BlockingSatelliteServer satellite, int from) {
        return satellite.fragments.subList(from, satellite.fragments.size());
    }

    public void testTheNextQueryUsesTheDeclarationOfTheSatelliteWhateverItsDirection() throws Exception {
        try (BlockingSatelliteServer satellite = new BlockingSatelliteServer()) {
            satellite.capabilities = SCAN_ONLY;
            satellite.schemaDeclarationId = "scan-only";
            register(satellite);

            runQueries(8);
            assertThat("the satellite was sent fragments", satellite.fragments.size(), greaterThan(0));
            for (String fragment : satellite.fragments) {
                assertThat(fragment, not(containsString("\"op\":\"filter\"")));
            }
            assertThat("the declaration was read once per node, not once per query", satellite.capabilityCalls.get(), lessThanOrEqualTo(2));

            int gained = satellite.fragments.size();
            satellite.capabilities = WITH_FILTER;
            satellite.schemaDeclarationId = "with-filter";
            runQueries(8);
            assertThat(since(satellite, gained).size(), greaterThan(0));
            for (String fragment : since(satellite, gained)) {
                assertThat(
                    "a satellite that gained a capability is used for it at once, by every node",
                    fragment,
                    containsString("\"op\":\"filter\"")
                );
            }

            int lost = satellite.fragments.size();
            satellite.capabilities = SCAN_ONLY;
            satellite.schemaDeclarationId = "scan-only";
            runQueries(8);
            assertThat(since(satellite, lost).size(), greaterThan(0));
            for (String fragment : since(satellite, lost)) {
                assertThat(
                    "a satellite that lost one is not sent it, and no query fails",
                    fragment,
                    not(containsString("\"op\":\"filter\""))
                );
            }
        }
    }
}
