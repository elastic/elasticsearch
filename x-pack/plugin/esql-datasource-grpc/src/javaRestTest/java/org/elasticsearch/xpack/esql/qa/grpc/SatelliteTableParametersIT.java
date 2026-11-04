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
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.datasource.grpc.BlockingSatelliteServer;
import org.elasticsearch.xpack.esql.datasources.Federation;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Base64;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;

/**
 * Table parameters and forwarded credentials: a dataset setting the satellite declares as a table parameter reaches it in a header on
 * every call about the table, whether or not a fragment is sent, and never inside a fragment; a setting it declares as an option
 * reaches it inside the fragment; the {@code credentials} of the data source reach it in a header over TLS and nowhere else. Both
 * nodes query the satellite, so the settings reach the coordinator and the data nodes alike.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class SatelliteTableParametersIT extends ESRestTestCase {

    private static final String SECRET = "AKIAIOSFODNN7EXAMPLE-secret";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .nodes(2)
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting(Federation.FEDERATION_ENABLED.getKey(), "true")
        .setting("xpack.ml.enabled", "false")
        .keystore("cluster.state.encryption.password.test", "satellite-params-password")
        .keystore("cluster.state.encryption.active_password_id", "test")
        .jvmArg("--add-opens=java.base/java.nio=ALL-UNNAMED")
        .jvmArg("-Darrow.allocation.manager.type=Unsafe")
        .build();

    private static final String DECLARATION = "{\"specVersions\":[0],\"fragmentFormats\":[\"json\"],\"plan\":[\"scan\",\"filter\"],"
        + "\"functions\":[\"greater_than\"],\"types\":[\"integer\",\"boolean\"],\"options\":[\"units\"],"
        + "\"tableParameters\":[\"location\",\"format\"],\"declarationId\":\"params-1\"}";

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    private void register(String prefix, int port, String datasetSettings) throws IOException {
        Request source = new Request("PUT", "/_query/data_source/" + prefix + "_src");
        try (XContentBuilder body = JsonXContent.contentBuilder()) {
            body.startObject().field("type", "flight").startObject("settings");
            body.field("ca_certificate", BlockingSatelliteServer.caCertificate());
            body.field("credentials", "{\"access_key\":\"" + SECRET + "\"}");
            source.setJsonEntity(Strings.toString(body.endObject().endObject()));
        }
        client().performRequest(source);
        Request dataset = new Request("PUT", "/_query/dataset/" + prefix + "_ds");
        dataset.setJsonEntity(
            "{\"data_source\": \""
                + prefix
                + "_src\", \"resource\": \"grpc+tls://127.0.0.1:"
                + port
                + "/t\", \"settings\": "
                + datasetSettings
                + "}"
        );
        client().performRequest(dataset);
    }

    private static Map<String, Object> decode(String header) throws IOException {
        try (
            var parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, Base64.getUrlDecoder().decode(header))
        ) {
            return parser.map();
        }
    }

    private void query(String dataset, boolean profile) throws IOException {
        for (int i = 0; i < 6; i++) {
            Request request = new Request("POST", "/_query");
            request.setJsonEntity("{\"query\": \"FROM " + dataset + " | WHERE id > 1 | LIMIT 10\", \"profile\": " + profile + "}");
            String body = EntityUtils.toString(client().performRequest(request).getEntity());
            assertThat("a profile shows the fragment, which never holds a secret", body, not(containsString(SECRET)));
        }
    }

    public void testTheSettingsAndTheCredentialsReachTheSatelliteWhereTheyBelong() throws Exception {
        try (BlockingSatelliteServer satellite = new BlockingSatelliteServer(true)) {
            satellite.capabilities = DECLARATION;
            satellite.schemaDeclarationId = "params-1";
            register("tp", satellite.port(), "{\"location\": \"s3://bucket/clickbench/\", \"format\": \"parquet\", \"units\": \"metric\"}");
            query("tp_ds", true);

            assertThat(satellite.fragments.size(), greaterThan(0));
            for (String fragment : satellite.fragments) {
                assertThat("an option travels inside the fragment", fragment, containsString("\"units\":\"metric\""));
                assertThat("a table parameter does not", fragment, not(containsString("s3://bucket")));
                assertThat(fragment, not(containsString("parquet")));
                assertThat("a credential never does", fragment, not(containsString(SECRET)));
            }
            Set<String> methods = new java.util.HashSet<>();
            for (BlockingSatelliteServer.CallHeaders call : satellite.headerLog) {
                methods.add(call.method());
                assertNotNull("every call carries the settings: " + call.method(), call.datasetSettings());
                assertThat(
                    decode(call.datasetSettings()),
                    equalTo(Map.of("location", "s3://bucket/clickbench/", "format", "parquet", "units", "metric"))
                );
                assertThat("and the credentials: " + call.method(), decode(call.credentials()), equalTo(Map.of("access_key", SECRET)));
            }
            for (String method : new String[] { "GET_SCHEMA", "DO_ACTION", "GET_FLIGHT_INFO", "DO_GET" }) {
                assertTrue("a call of every kind: " + methods, methods.contains(method));
            }
            String dataset = EntityUtils.toString(client().performRequest(new Request("GET", "/_query/dataset/tp_ds")).getEntity());
            assertThat("the table parameters are plain settings of the dataset", dataset, containsString("s3://bucket/clickbench/"));
            for (String path : new String[] { "/_query/data_source/tp_src", "/_query/dataset/tp_ds", "/_query/data_source" }) {
                String shown = EntityUtils.toString(client().performRequest(new Request("GET", path)).getEntity());
                assertThat("the API does not show the credentials: " + path, shown, not(containsString(SECRET)));
            }
        }
    }

    public void testASettingTheSatelliteDoesNotDeclareFailsTheQuery() throws Exception {
        try (BlockingSatelliteServer satellite = new BlockingSatelliteServer(true)) {
            satellite.capabilities = DECLARATION;
            satellite.schemaDeclarationId = "params-1";
            register("tpbad", satellite.port(), "{\"bucket\": \"other\"}");
            Request request = new Request("POST", "/_query");
            request.setJsonEntity("{\"query\": \"FROM tpbad_ds | LIMIT 1\"}");
            ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
            assertThat(EntityUtils.toString(e.getResponse().getEntity()), containsString("does not declare the dataset setting [bucket]"));
            assertEquals(0, satellite.fragments.size());
        }
    }

    public void testATableParameterIsSentWhenNoFragmentIs() throws Exception {
        try (BlockingSatelliteServer satellite = new BlockingSatelliteServer(true)) {
            satellite.capabilities = DECLARATION;
            satellite.schemaDeclarationId = "params-1";
            register("tpplain", satellite.port(), "{\"location\": \"s3://bucket/plain/\"}");
            Request request = new Request("POST", "/_query");
            // pushdown off: the read is a plain one, with no fragment
            request.setJsonEntity("{\"query\": \"FROM tpplain_ds | LIMIT 5\", \"pragma\": {\"satellite_pushdown\": false}}");
            assertEquals(200, client().performRequest(request).getStatusLine().getStatusCode());
            assertEquals("no fragment was sent", 0, satellite.fragments.size());
            assertTrue(
                "the plain read still carried the table parameters",
                satellite.headerLog.stream().anyMatch(c -> c.method().equals("DO_GET") && c.datasetSettings() != null)
            );
        }
    }
}
