/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.grpc;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.datasource.grpc.BlockingSatelliteServer;
import org.elasticsearch.xpack.esql.datasources.Federation;
import org.junit.ClassRule;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;

/**
 * A satellite that asks for a bearer token is queried when the data source holds the token, and only then. The token is a secret
 * setting: stored encrypted, never shown by the data source API, never sent inside a fragment or the descriptor of a read. Both
 * nodes of the cluster query it, so the token reaches the coordinator and the data nodes alike.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class SatelliteAuthIT extends ESRestTestCase {

    private static final String TOKEN = "t0ken-for-the-satellite-4711";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .nodes(2)
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting(Federation.FEDERATION_ENABLED.getKey(), "true")
        .setting("xpack.ml.enabled", "false")
        .keystore("cluster.state.encryption.password.test", "satellite-auth-password")
        .keystore("cluster.state.encryption.active_password_id", "test")
        .jvmArg("--add-opens=java.base/java.nio=ALL-UNNAMED")
        .jvmArg("-Darrow.allocation.manager.type=Unsafe")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    private void dataSource(String name, String token) throws IOException {
        Request request = new Request("PUT", "/_query/data_source/" + name);
        try (XContentBuilder body = JsonXContent.contentBuilder()) {
            body.startObject().field("type", "flight").startObject("settings");
            body.field("ca_certificate", BlockingSatelliteServer.caCertificate());
            if (token != null) {
                body.field("token", token);
            }
            request.setJsonEntity(Strings.toString(body.endObject().endObject()));
        }
        client().performRequest(request);
    }

    private void dataset(String name, String source, int port) throws IOException {
        Request request = new Request("PUT", "/_query/dataset/" + name);
        request.setJsonEntity("{\"data_source\": \"" + source + "\", \"resource\": \"grpc+tls://127.0.0.1:" + port + "/t\"}");
        client().performRequest(request);
    }

    private void query(String dataset) throws IOException {
        // several queries, so that both nodes coordinate some of them and the other one reads
        for (int i = 0; i < 6; i++) {
            Request request = new Request("POST", "/_query");
            request.setJsonEntity("{\"query\": \"FROM " + dataset + " | WHERE id > 1 | LIMIT 10\"}");
            assertEquals(200, client().performRequest(request).getStatusLine().getStatusCode());
        }
    }

    public void testTheTokenOfTheDataSourceReachesTheSatelliteAndNothingElse() throws Exception {
        try (BlockingSatelliteServer satellite = new BlockingSatelliteServer(true)) {
            satellite.requiredToken = TOKEN;
            dataSource("auth_ok", TOKEN);
            dataset("auth_ok_ds", "auth_ok", satellite.port());
            query("auth_ok_ds");
            assertThat(satellite.fragments.size(), greaterThan(0));
            assertEquals("every call carried the token", 0, satellite.unauthenticatedCalls.get());
            for (String fragment : satellite.fragments) {
                assertThat("a fragment never carries a credential", fragment, not(containsString(TOKEN)));
            }
            for (String path : new String[] { "/_query/data_source/auth_ok", "/_query/dataset/auth_ok_ds", "/_query/data_source" }) {
                String shown = EntityUtils.toString(client().performRequest(new Request("GET", path)).getEntity());
                assertThat("the API does not show the token: " + path, shown, not(containsString(TOKEN)));
            }
        }
    }

    public void testAWrongTokenFailsTheQueryWithoutEchoingIt() throws Exception {
        try (BlockingSatelliteServer satellite = new BlockingSatelliteServer(true)) {
            satellite.requiredToken = TOKEN;
            dataSource("auth_bad", "another-token-4712");
            dataset("auth_bad_ds", "auth_bad", satellite.port());
            Request request = new Request("POST", "/_query");
            request.setJsonEntity("{\"query\": \"FROM auth_bad_ds | LIMIT 1\"}");
            ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
            String message = EntityUtils.toString(e.getResponse().getEntity());
            assertThat(message, containsString("did not accept the credentials"));
            assertThat(message, not(containsString("another-token-4712")));
            assertThat(message, not(containsString(TOKEN)));
            assertEquals(0, satellite.fragments.size());
        }
    }

    public void testNoTokenFailsAndRotatingItRestoresTheDataSource() throws Exception {
        try (BlockingSatelliteServer satellite = new BlockingSatelliteServer(true)) {
            satellite.requiredToken = TOKEN;
            dataSource("auth_rot", null);
            dataset("auth_rot_ds", "auth_rot", satellite.port());
            Request request = new Request("POST", "/_query");
            request.setJsonEntity("{\"query\": \"FROM auth_rot_ds | LIMIT 1\"}");
            expectThrows(ResponseException.class, () -> client().performRequest(request));
            dataSource("auth_rot", TOKEN);
            query("auth_rot_ds");
        }
    }

    public void testATokenNeedsATlsResource() throws Exception {
        dataSource("auth_clear", TOKEN);
        Request request = new Request("PUT", "/_query/dataset/auth_clear_ds");
        request.setJsonEntity("{\"data_source\": \"auth_clear\", \"resource\": \"grpc://127.0.0.1:1/t\"}");
        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
        assertThat(e.getMessage(), containsString("needs a flight+tls or grpc+tls resource"));
    }

    /** The credentials can also be refused later than at resolution: here when the fragment is sent. */
    public void testARefusalWhenTheFragmentIsSentFailsTheQueryAsWell() throws Exception {
        try (BlockingSatelliteServer satellite = new BlockingSatelliteServer(true)) {
            dataSource("auth_late", TOKEN);
            dataset("auth_late_ds", "auth_late", satellite.port());
            satellite.rejectFragmentWith = org.apache.arrow.flight.CallStatus.UNAUTHENTICATED.withDescription("token revoked");
            Request request = new Request("POST", "/_query");
            request.setJsonEntity("{\"query\": \"FROM auth_late_ds | WHERE id > 1 | LIMIT 10\"}");
            ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
            assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
            String message = EntityUtils.toString(e.getResponse().getEntity());
            assertThat(message, containsString("did not accept the credentials"));
            assertThat(message, not(containsString("token revoked")));
        }
    }

    /** A profile shows the fragment and the plan: neither may carry the token. */
    public void testTheProfileDoesNotShowTheToken() throws Exception {
        try (BlockingSatelliteServer satellite = new BlockingSatelliteServer(true)) {
            satellite.requiredToken = TOKEN;
            dataSource("auth_prof", TOKEN);
            dataset("auth_prof_ds", "auth_prof", satellite.port());
            Request request = new Request("POST", "/_query");
            request.setJsonEntity("{\"query\": \"FROM auth_prof_ds | WHERE id > 1 | LIMIT 10\", \"profile\": true}");
            String body = EntityUtils.toString(client().performRequest(request).getEntity());
            assertThat(body, not(containsString(TOKEN)));
        }
    }
}
