/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.grpc;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
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
 * A satellite that serves TLS is queried as {@code FROM <dataset>} when the data source holds the certificate authority that issued
 * its certificate, and only then: with no authority, a wrong one, or a resource that says plain gRPC, the query fails. The
 * authority is the PEM text of a data source setting, so the nodes read no file.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class SatelliteTlsIT extends ESRestTestCase {

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

    private Response dataSource(String name, String ca) throws IOException {
        Request request = new Request("PUT", "/_query/data_source/" + name);
        try (XContentBuilder body = JsonXContent.contentBuilder()) {
            body.startObject().field("type", "flight");
            if (ca != null) {
                body.startObject("settings").field("ca_certificate", ca).endObject();
            }
            request.setJsonEntity(Strings.toString(body.endObject()));
        }
        return client().performRequest(request);
    }

    private void dataset(String name, String source, String resource) throws IOException {
        Request request = new Request("PUT", "/_query/dataset/" + name);
        request.setJsonEntity("{\"data_source\": \"" + source + "\", \"resource\": \"" + resource + "\"}");
        client().performRequest(request);
    }

    private Response query(String dataset) throws IOException {
        Request request = new Request("POST", "/_query");
        request.setJsonEntity("{\"query\": \"FROM " + dataset + " | WHERE id > 1 | LIMIT 10\"}");
        return client().performRequest(request);
    }

    public void testTheDataSourceTrustsTheCertificateAuthority() throws Exception {
        try (BlockingSatelliteServer satellite = new BlockingSatelliteServer(true)) {
            dataSource("tls_ok", BlockingSatelliteServer.caCertificate());
            dataset("tls_ok_ds", "tls_ok", "grpc+tls://127.0.0.1:" + satellite.port() + "/t");
            for (int i = 0; i < 4; i++) {
                assertEquals(200, query("tls_ok_ds").getStatusLine().getStatusCode());
            }
            assertThat("the satellite was sent fragments over TLS", satellite.fragments.size(), greaterThan(0));
            String stored = org.elasticsearch.test.rest.ESRestTestCase.entityAsMap(
                client().performRequest(new Request("GET", "/_query/data_source/tls_ok"))
            ).toString();
            assertThat("the authority is public: it is not redacted", stored, containsString("BEGIN CERTIFICATE"));
        }
    }

    public void testWithoutTheRightAuthorityTheQueryFails() throws Exception {
        try (BlockingSatelliteServer satellite = new BlockingSatelliteServer(true)) {
            String resource = "grpc+tls://127.0.0.1:" + satellite.port() + "/t";
            dataSource("tls_other", BlockingSatelliteServer.otherCaCertificate());
            dataset("tls_other_ds", "tls_other", resource);
            ResponseException e = expectThrows(ResponseException.class, () -> query("tls_other_ds"));
            assertThat(e.getMessage(), containsString("Failed to resolve"));
            assertThat(e.getMessage(), not(containsString("BEGIN CERTIFICATE")));
            // the satellite never received a fragment: the connection was refused before any call
            assertThat(satellite.fragments.size(), org.hamcrest.Matchers.equalTo(0));
        }
    }

    public void testATlsSatelliteIsNotQueriedInClear() throws Exception {
        try (BlockingSatelliteServer satellite = new BlockingSatelliteServer(true)) {
            dataSource("tls_plain", null);
            dataset("tls_plain_ds", "tls_plain", "grpc://127.0.0.1:" + satellite.port() + "/t");
            ResponseException e = expectThrows(ResponseException.class, () -> query("tls_plain_ds"));
            assertThat(e.getMessage(), not(containsString("BEGIN CERTIFICATE")));
            assertThat(satellite.fragments.size(), org.hamcrest.Matchers.equalTo(0));
        }
    }

    public void testTlsSettingsOnAPlainResourceAreRefused() throws Exception {
        dataSource("tls_misuse", BlockingSatelliteServer.caCertificate());
        ResponseException e = expectThrows(ResponseException.class, () -> dataset("tls_misuse_ds", "tls_misuse", "grpc://127.0.0.1:1/t"));
        assertThat(e.getMessage(), containsString("flight+tls or grpc+tls"));
    }

    public void testADatasetOnAHostThatIsNotAllowedIsNotRegistered() throws Exception {
        dataSource("tls_host", null);
        ResponseException e = expectThrows(ResponseException.class, () -> dataset("tls_host_ds", "tls_host", "grpc://10.0.0.5:47470/t"));
        assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
        assertThat(e.getMessage(), containsString("is not allowed"));
    }
}
