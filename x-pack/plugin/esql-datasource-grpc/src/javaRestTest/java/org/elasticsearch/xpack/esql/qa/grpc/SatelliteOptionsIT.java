/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.grpc;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.esql.datasource.grpc.BlockingSatelliteServer;
import org.elasticsearch.xpack.esql.datasources.Federation;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Source options (dataset settings) reach the satellite inside the fragment, on every read of the dataset, and an option the
 * satellite does not declare fails the query instead of being ignored. The satellite runs in this JVM and declares one option,
 * {@code units}, so the test needs no external process.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class SatelliteOptionsIT extends ESRestTestCase {

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

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    private void register(BlockingSatelliteServer satellite, String dataset, String settingsJson) throws IOException {
        Request source = new Request("PUT", "/_query/data_source/opt_src");
        source.setJsonEntity("{\"type\": \"flight\"}");
        try {
            client().performRequest(source);
        } catch (ResponseException e) {
            // already registered by an earlier test of this class
        }
        Request ds = new Request("PUT", "/_query/dataset/" + dataset);
        ds.setJsonEntity(
            "{\"data_source\": \"opt_src\", \"resource\": \"flight://127.0.0.1:"
                + satellite.port()
                + "/t\", \"settings\": "
                + settingsJson
                + "}"
        );
        client().performRequest(ds);
    }

    private Map<String, Object> query(String esql, boolean pushdown) throws IOException {
        Request req = new Request("POST", "/_query");
        req.setJsonEntity("{\"query\": \"" + esql + "\", \"pragma\": {\"satellite_pushdown\": " + pushdown + "}}");
        return entityAsMap(client().performRequest(req));
    }

    private String errorOf(String esql) throws IOException {
        Request req = new Request("POST", "/_query");
        req.setJsonEntity("{\"query\": \"" + esql + "\"}");
        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(req));
        return e.getMessage();
    }

    public void testOptionsReachTheSatelliteOnEveryRead() throws Exception {
        try (BlockingSatelliteServer satellite = new BlockingSatelliteServer()) {
            register(satellite, "opt_ds", "{\"units\": \"fahrenheit\"}");
            try {
                Map<String, Object> pushed = query("FROM opt_ds | KEEP id | LIMIT 5", true);
                assertThat(pushed.get("values"), equalTo(List.of(List.of(0), List.of(1), List.of(2))));
                assertThat(satellite.fragments.get(0), containsString("\"options\":{\"units\":\"fahrenheit\"}"));
                assertThat("the chain is pushed as usual", satellite.fragments.get(0), containsString("\"op\":\"limit\""));

                // with the pushdown off the read is still a fragment, because only a fragment can carry the options
                Map<String, Object> plain = query("FROM opt_ds | KEEP id | LIMIT 5", false);
                assertThat(plain.get("values"), equalTo(pushed.get("values")));
                String scanOnly = satellite.fragments.get(1);
                assertThat(scanOnly, containsString("\"options\":{\"units\":\"fahrenheit\"}"));
                assertThat(scanOnly, containsString("\"plan\":{\"op\":\"scan\""));
                assertThat(scanOnly, org.hamcrest.Matchers.not(containsString("\"limit\"")));
            } finally {
                deleteQuietly("/_query/dataset/opt_ds");
                deleteQuietly("/_query/data_source/opt_src");
            }
        }
    }

    public void testAnOptionTheSatelliteDoesNotDeclareFailsTheQuery() throws Exception {
        try (BlockingSatelliteServer satellite = new BlockingSatelliteServer()) {
            register(satellite, "opt_bad_ds", "{\"color\": \"red\"}");
            try {
                String error = errorOf("FROM opt_bad_ds | KEEP id | LIMIT 5");
                assertThat(error, containsString("does not declare the source option [color]"));
                assertThat(error, containsString("declares [units]"));
                assertThat("nothing was read", satellite.fragments.size(), equalTo(0));
            } finally {
                deleteQuietly("/_query/dataset/opt_bad_ds");
                deleteQuietly("/_query/data_source/opt_src");
            }
        }
    }

    public void testRegistrationRefusesSettingsOfOurs() throws Exception {
        Request source = new Request("PUT", "/_query/data_source/opt_src2");
        source.setJsonEntity("{\"type\": \"flight\"}");
        client().performRequest(source);
        try {
            Request ds = new Request("PUT", "/_query/dataset/opt_reserved_ds");
            ds.setJsonEntity(
                "{\"data_source\": \"opt_src2\", \"resource\": \"flight://127.0.0.1:1/t\","
                    + " \"settings\": {\"endpoint\": \"grpc://elsewhere:2\"}}"
            );
            ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(ds));
            assertThat(e.getMessage(), containsString("is not a source option"));
        } finally {
            deleteQuietly("/_query/data_source/opt_src2");
        }
    }

    public void testASourceReadingNoColumnIsRefusedClearlyWhenItHasOptions() throws Exception {
        try (BlockingSatelliteServer satellite = new BlockingSatelliteServer()) {
            register(satellite, "opt_count_ds", "{\"units\": \"fahrenheit\"}");
            try {
                assertThat(errorOf("FROM opt_count_ds | STATS c = COUNT(*)"), containsString("reads no column of the source"));
            } finally {
                deleteQuietly("/_query/dataset/opt_count_ds");
                deleteQuietly("/_query/data_source/opt_src");
            }
        }
    }

    private void deleteQuietly(String path) throws IOException {
        try {
            client().performRequest(new Request("DELETE", path));
        } catch (ResponseException e) {
            // already gone
        }
    }
}
