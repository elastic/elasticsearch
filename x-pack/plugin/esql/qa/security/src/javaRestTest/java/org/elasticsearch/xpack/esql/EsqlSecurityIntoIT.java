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
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.hasCapabilities;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

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
        // Test clusters randomly start with indices.batch_indexing off, which INTO rejects. The flag has to be on for the setting to
        // be accepted at all, which a release build does not do on its own.
        .feature(FeatureFlag.BATCH_INDEXING)
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.enabled", "true")
        .setting("indices.batch_indexing", "true")
        .rolesFile(Resource.fromClasspath("roles.yml"))
        .user("test-admin", PASSWORD, "test-admin", true)
        .user("into_writer", PASSWORD, "into_write_only", false)
        .user("into_maintainer", PASSWORD, "into_write_and_maintenance", false)
        .user("into_maintainer_elsewhere", PASSWORD, "into_write_and_maintenance_elsewhere", false)
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

    /**
     * The rows are stored, so the query succeeds. A failure would invite a retry that stores them again, so the refresh that the
     * caller may not do is a warning, which says that the rows were written and only the refresh failed.
     */
    public void testWriterWithoutMaintenanceKeepsTheRowsAndWarnsThatTheRefreshFailed() throws IOException {
        Response response = runInto("into_writer", "");
        assertSummary(response);
        assertThat(
            warning(response),
            containsString(
                "index refresh for INTO [into-dest] failed after [2] documents were written, so they may not be searchable yet: "
                    + "action [indices:admin/refresh] is unauthorized"
            )
        );
        assertThat(warning(response), containsString("INTO includes an index refresh that requires the [maintenance] privilege"));
        // The header escapes the quotes of the option, so only what follows it is matched
        assertThat(warning(response), containsString(": false in future INTO queries to skip it"));
        assertThat(warning(response), containsString("re-running this one writes the documents again"));

        assertThat("the refresh did not happen", countDestination(), equalTo(0L));
        refreshDestination();
        assertThat(countDestination(), equalTo(2L));
    }

    /**
     * The refresh action is allowed for this user, on an index that is not the destination, so the action-level check passes. The
     * destination must then be refused, not silently dropped from the refresh with the rows left unsearchable and no warning.
     */
    public void testMaintenanceOnAnotherIndexDoesNotRefreshTheDestination() throws IOException {
        Response response = runInto("into_maintainer_elsewhere", "");
        assertSummary(response);
        assertThat(warning(response), containsString("index refresh for INTO [into-dest] failed after [2] documents were written"));

        assertThat("the refresh did not happen", countDestination(), equalTo(0L));
        refreshDestination();
        assertThat(countDestination(), equalTo(2L));
    }

    public void testWriterWithoutMaintenanceCanSkipTheRefresh() throws IOException {
        Response response = runInto("into_writer", " WITH {\"refresh\": false}");
        assertSummary(response);
        assertThat("skipping the refresh is not a failure to warn about", response.getWarnings(), empty());

        assertThat("not searchable until something refreshes the destination", countDestination(), equalTo(0L));
        refreshDestination();
        assertThat(countDestination(), equalTo(2L));
    }

    public void testWriterWithMaintenanceSeesTheRowsWhenTheQueryReturns() throws IOException {
        Response response = runInto("into_maintainer", "");
        assertSummary(response);
        assertThat(response.getWarnings(), empty());

        assertThat("INTO refreshed the destination", countDestination(), equalTo(2L));
    }

    private static Response runInto(String user, String options) throws IOException {
        String query = "ROW k = [\"a\", \"b\"] | MV_EXPAND k | INTO " + DESTINATION + options;
        Request request = new Request("POST", "_query");
        request.setJsonEntity(Strings.toString(JsonXContent.contentBuilder().startObject().field("query", query).endObject()));
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder().addHeader("es-security-runas-user", user).setWarningsHandler(WarningsHandler.PERMISSIVE)
        );
        return client().performRequest(request);
    }

    private static void assertSummary(Response response) throws IOException {
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        assertThat(entityAsMap(response).get("values"), equalTo(List.of(List.of(2, 2))));
    }

    private static String warning(Response response) {
        assertThat(response.getWarnings(), hasSize(1));
        return response.getWarnings().getFirst();
    }

    private static void refreshDestination() throws IOException {
        client().performRequest(new Request("POST", DESTINATION + "/_refresh"));
    }

    private static long countDestination() throws IOException {
        return ((Number) entityAsMap(client().performRequest(new Request("GET", DESTINATION + "/_count"))).get("count")).longValue();
    }
}
