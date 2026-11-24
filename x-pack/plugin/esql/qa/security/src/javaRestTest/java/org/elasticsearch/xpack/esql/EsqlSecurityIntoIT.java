/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.hasCapabilities;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * {@code INTO} under real security. Writing the rows needs the {@code write} index privilege, but refreshing the destination
 * afterwards needs {@code maintenance}, which a user who may only write does not have. The destination turns its own refresh off,
 * so the rows can be searched only if {@code INTO} refreshed it.
 */
public class EsqlSecurityIntoIT extends ESRestTestCase {
    private static final String DESTINATION = "into-dest";
    private static final String PASSWORD = "x-pack-test-password";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.enabled", "true")
        .setting("indices.batch_indexing", "true")
        .rolesFile(Resource.fromClasspath("roles.yml"))
        .user("test-admin", PASSWORD, "test-admin", true)
        .user("into_writer", PASSWORD, "into_write_only", false)
        .user("into_maintainer", PASSWORD, "into_write_and_maintenance", false)
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("test-admin", new SecureString(PASSWORD.toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Before
    public void createDestination() throws IOException {
        assumeTrue("INTO is only available in snapshot builds", hasCapabilities(adminClient(), List.of("into_command")));
        createIndex(
            DESTINATION,
            Settings.builder().put("index.refresh_interval", -1).build(),
            "\"properties\": { \"k\": { \"type\": \"keyword\" } }"
        );
    }

    public void testWriterWithoutMaintenanceKeepsTheRowsButFailsTheRefresh() throws IOException {
        ResponseException e = expectThrows(ResponseException.class, () -> runInto("into_writer", ""));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(403));
        assertThat(
            errorReason(e),
            containsString("INTO [into-dest] wrote [2] documents but the refresh failed: action [indices:admin/refresh] is unauthorized")
        );
        assertThat(errorReason(e), containsString("refreshing requires the [maintenance] privilege on the destination"));
        assertThat(errorReason(e), containsString("use \"refresh\": false to skip it"));

        // The failed refresh does not undo the write.
        refreshDestination();
        assertThat(countDestination(), equalTo(2L));
    }

    public void testWriterWithoutMaintenanceCanSkipTheRefresh() throws IOException {
        assertSummary(runInto("into_writer", " WITH {\"refresh\": false}"));

        assertThat("not searchable until something refreshes the destination", countDestination(), equalTo(0L));
        refreshDestination();
        assertThat(countDestination(), equalTo(2L));
    }

    public void testWriterWithMaintenanceSeesTheRowsWhenTheQueryReturns() throws IOException {
        assertSummary(runInto("into_maintainer", ""));

        assertThat("INTO refreshed the destination", countDestination(), equalTo(2L));
    }

    private static Response runInto(String user, String options) throws IOException {
        String query = "ROW k = [\"a\", \"b\"] | MV_EXPAND k | INTO " + DESTINATION + options;
        Request request = new Request("POST", "_query");
        request.setJsonEntity(Strings.toString(JsonXContent.contentBuilder().startObject().field("query", query).endObject()));
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("es-security-runas-user", user));
        return client().performRequest(request);
    }

    private static void assertSummary(Response response) throws IOException {
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        assertThat(entityAsMap(response).get("values"), equalTo(List.of(List.of(2, 2))));
    }

    @SuppressWarnings("unchecked")
    private static String errorReason(ResponseException e) throws IOException {
        Map<String, Object> error = (Map<String, Object>) entityAsMap(e.getResponse()).get("error");
        return (String) error.get("reason");
    }

    private static void refreshDestination() throws IOException {
        client().performRequest(new Request("POST", DESTINATION + "/_refresh"));
    }

    private static long countDestination() throws IOException {
        return ((Number) entityAsMap(client().performRequest(new Request("GET", DESTINATION + "/_count"))).get("count")).longValue();
    }
}
