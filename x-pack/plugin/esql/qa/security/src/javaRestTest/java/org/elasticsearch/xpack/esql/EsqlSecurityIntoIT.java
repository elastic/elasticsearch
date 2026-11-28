/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.apache.http.HttpStatus;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.cluster.LogType;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.action.EsqlIntoDestinationAction;
import org.elasticsearch.xpack.esql.action.EsqlIntoReplaceDestinationAction;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.hasCapabilities;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * {@code INTO} under real security. Writing the rows needs the {@code write} index privilege, but refreshing the destination
 * afterwards needs {@code maintenance}, which a user who may only write does not have. The destination turns its own refresh off,
 * so the rows can be searched only if {@code INTO} refreshed it.
 * <p>
 * A ROW source needs no index privilege, so the destination is authorized by {@code INTO} itself, before anything about it is
 * reported: a caller who may not write gets the same 403 whether or not the destination exists.
 */
public class EsqlSecurityIntoIT extends ESRestTestCase {
    private static final String DESTINATION = "into-dest";
    private static final String PASSWORD = "x-pack-test-password";
    private static final String EXISTING = "into-existing";
    private static final String READ_ONLY_USER = "into_read_only_user";
    private static final String CREATE_DOC_USER = "into_create_doc_user";
    private static final String CREATE_DOC_ELSEWHERE_USER = "into_create_doc_elsewhere_user";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        // Test clusters randomly start with indices.batch_indexing off, which INTO rejects. The flag has to be on for the setting to
        // be accepted at all, which a release build does not do on its own.
        .feature(FeatureFlag.BATCH_INDEXING)
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.security.audit.enabled", "true")
        .setting("indices.batch_indexing", "true")
        .rolesFile(Resource.fromClasspath("roles.yml"))
        .user("test-admin", PASSWORD, "test-admin", true)
        .user("into_writer", PASSWORD, "into_write_only", false)
        .user("into_maintainer", PASSWORD, "into_write_and_maintenance", false)
        .user("into_maintainer_elsewhere", PASSWORD, "into_write_and_maintenance_elsewhere", false)
        .user(READ_ONLY_USER, PASSWORD, "into_read_only", false)
        .user(CREATE_DOC_USER, PASSWORD, "into_create_doc_only", false)
        .user(CREATE_DOC_ELSEWHERE_USER, PASSWORD, "into_create_doc_elsewhere", false)
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
        assertThat(warning(response), containsString(": false to skip it"));

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

    /**
     * A caller who may not write to the destination gets a 403 that is the same for an existing and a missing destination, and is told
     * nothing about the existing one: its ingest pipeline is not reported. That holds for a caller who may only read it, and for one
     * who may write, but to another index.
     */
    public void testDestinationDeniedWithoutWriteAccess() throws IOException {
        createDestinationWithPipeline();

        assertDenied(READ_ONLY_USER, "into_read_only", "", EsqlIntoDestinationAction.NAME);
        assertDenied(CREATE_DOC_ELSEWHERE_USER, "into_create_doc_elsewhere", "", EsqlIntoDestinationAction.NAME);
    }

    /**
     * Replacing documents sends index requests, which {@code create_doc} does not allow, so a caller who may only create documents is
     * denied up front, as for any other write, and not by a 403 on every row. A caller who may index them is not.
     */
    public void testReplaceNeedsMoreThanCreateDoc() throws IOException {
        createDestinationWithPipeline();

        assertDenied(CREATE_DOC_USER, "into_create_doc_only", " WITH {\"collisions\": \"replace\"}", EsqlIntoReplaceDestinationAction.NAME);

        Response response = runInto("into_writer", " WITH {\"collisions\": \"replace\", \"refresh\": false}");
        assertSummary(response);
    }

    private static void assertDenied(String user, String role, String options, String action) throws IOException {
        ResponseException existing = expectThrows(ResponseException.class, () -> intoDestination(user, "ROW x = 1", EXISTING, options));
        ResponseException missing = expectThrows(
            ResponseException.class,
            () -> intoDestination(user, "ROW x = 1", "into-missing", options)
        );
        String existingBody = EntityUtils.toString(existing.getResponse().getEntity());
        String missingBody = EntityUtils.toString(missing.getResponse().getEntity());
        assertThat(existingBody, existing.getResponse().getStatusLine().getStatusCode(), equalTo(HttpStatus.SC_FORBIDDEN));
        assertThat(missingBody, missing.getResponse().getStatusLine().getStatusCode(), equalTo(HttpStatus.SC_FORBIDDEN));
        assertDeniedByTheFilter(existingBody, user, role, EXISTING, action);
        assertDeniedByTheFilter(missingBody, user, role, "into-missing", action);
    }

    /** The denial is the one of the security filter, which audits it, and which names the roles and the privileges that grant it. */
    private static void assertDeniedByTheFilter(String body, String user, String role, String destination, String action) {
        String denial = "action [" + action + "] is unauthorized for user [test-admin] run as [";
        assertThat(body, containsString(denial + user + "] with effective roles [" + role + "]"));
        assertThat(body, containsString("on indices [" + destination + "], this action is granted by the index privileges ["));
    }

    /** A denied destination is audited as the write that it is: the action of the check, the destination, the caller, and who ran it for them. */
    public void testDeniedDestinationIsAudited() throws Exception {
        expectThrows(ResponseException.class, () -> intoDestination(READ_ONLY_USER, "into-audited"));

        assertBusy(() -> {
            try (var auditLog = cluster.getNodeLog(0, LogType.AUDIT)) {
                List<Map<String, Object>> denials = new ArrayList<>();
                for (String line : Streams.readAllLines(auditLog)) {
                    Map<String, Object> event = XContentHelper.convertToMap(XContentType.JSON.xContent(), line, false);
                    if ("access_denied".equals(event.get("event.action"))
                        && EsqlIntoDestinationAction.NAME.equals(event.get("action"))
                        && READ_ONLY_USER.equals(event.get("user.name"))
                    // The cluster is shared with the other tests, which are denied the same action as this user
                        && List.of("into-audited").equals(event.get("indices"))) {
                        denials.add(event);
                    }
                }
                assertThat(denials, hasSize(1));
                assertThat(denials.getFirst().get("user.run_by.name"), equalTo("test-admin"));
            }
        });
    }

    /**
     * A writer passes the authorization without any read access to the destination, and may then be told what a bulk would tell
     * them: that the pipeline of the destination cannot be used. A missing destination is an error for them too, as INTO does not
     * create indices. A writer can write to an existing destination that has no pipeline.
     */
    public void testWriterWithoutReadAccessIsAuthorized() throws IOException {
        createDestinationWithPipeline();

        ResponseException refusal = expectThrows(ResponseException.class, () -> intoDestination(CREATE_DOC_USER, EXISTING));
        String body = EntityUtils.toString(refusal.getResponse().getEntity());
        assertThat(body, refusal.getResponse().getStatusLine().getStatusCode(), equalTo(HttpStatus.SC_BAD_REQUEST));
        assertThat(body, containsString("does not support ingest pipelines"));

        ResponseException missing = expectThrows(ResponseException.class, () -> intoDestination(CREATE_DOC_USER, "into-missing"));
        assertThat(missing.getResponse().getStatusLine().getStatusCode(), equalTo(HttpStatus.SC_NOT_FOUND));
        assertThat(
            EntityUtils.toString(missing.getResponse().getEntity()),
            containsString("INTO destination [into-missing] does not exist")
        );

        createIndex(
            adminClient(),
            "into-plain",
            Settings.builder().put("index.batch_indexing", true).build(),
            "\"properties\": {\"x\": {\"type\": \"long\"}}"
        );
        assertThat(intoDestination(CREATE_DOC_USER, "into-plain").getStatusLine().getStatusCode(), equalTo(HttpStatus.SC_OK));
        assertOK(adminClient().performRequest(new Request("POST", "into-plain/_refresh")));
        Response count = adminClient().performRequest(new Request("GET", "into-plain/_count"));
        assertThat(entityAsMap(count).get("count"), equalTo(1));
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

    /** A data stream is a destination too, and has to get through the filter, which leaves data streams out of a request that says so. */
    public void testWriterCanWriteToADataStream() throws IOException {
        Request template = new Request("PUT", "_index_template/into-ds-template");
        template.setJsonEntity("""
            {
              "index_patterns": ["into-ds"],
              "data_stream": {},
              "template": {
                "settings": {"index.batch_indexing": true},
                "mappings": {"properties": {"@timestamp": {"type": "date"}, "x": {"type": "long"}}}
              }
            }""");
        assertOK(adminClient().performRequest(template));
        assertOK(adminClient().performRequest(new Request("PUT", "_data_stream/into-ds")));

        Response response = intoDestination(CREATE_DOC_USER, "ROW `@timestamp` = TO_DATETIME(\"2026-01-01T00:00:00Z\"), x = 1", "into-ds");
        assertThat(response.getStatusLine().getStatusCode(), equalTo(HttpStatus.SC_OK));
        assertThat(countOf("into-ds"), equalTo(1));
    }

    public void testWriterCanWriteToAnAliasWithAWriteIndex() throws IOException {
        createIndex(
            adminClient(),
            "into-alias-index",
            Settings.builder().put("index.batch_indexing", true).build(),
            "\"properties\": {\"x\": {\"type\": \"long\"}}",
            "\"into-alias\": {\"is_write_index\": true}"
        );

        assertThat(intoDestination(CREATE_DOC_USER, "into-alias").getStatusLine().getStatusCode(), equalTo(HttpStatus.SC_OK));
        assertThat(countOf("into-alias"), equalTo(1));
    }

    /** The alias exists, so the destination check passes. It has nowhere to write, which only the bulk finds out, as it would for a bulk. */
    public void testAliasWithoutAWriteIndexFailsAtTheBulk() throws IOException {
        createIndex(
            adminClient(),
            "into-nowrite-index",
            Settings.builder().put("index.batch_indexing", true).build(),
            "\"properties\": {\"x\": {\"type\": \"long\"}}",
            "\"into-nowrite-alias\": {\"is_write_index\": false}"
        );

        ResponseException e = expectThrows(ResponseException.class, () -> intoDestination(CREATE_DOC_USER, "into-nowrite-alias"));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(HttpStatus.SC_BAD_REQUEST));
        assertThat(EntityUtils.toString(e.getResponse().getEntity()), containsString("no write index is defined for alias"));
    }

    private static Object countOf(String index) throws IOException {
        assertOK(adminClient().performRequest(new Request("POST", index + "/_refresh")));
        return entityAsMap(adminClient().performRequest(new Request("GET", index + "/_count"))).get("count");
    }

    private static void createDestinationWithPipeline() throws IOException {
        Request pipeline = new Request("PUT", "_ingest/pipeline/into-pipeline");
        pipeline.setJsonEntity("{\"processors\": []}");
        assertOK(adminClient().performRequest(pipeline));
        createIndex(adminClient(), EXISTING, Settings.builder().put("index.default_pipeline", "into-pipeline").build());
    }

    /** Writes one row into the given destination as the given user. A user without {@code maintenance} also gets a refresh warning. */
    private static Response intoDestination(String user, String destination) throws IOException {
        return intoDestination(user, "ROW x = 1", destination);
    }

    private static Response intoDestination(String user, String rows, String destination) throws IOException {
        return intoDestination(user, rows, destination, "");
    }

    private static Response intoDestination(String user, String rows, String destination, String options) throws IOException {
        Request request = new Request("POST", "_query");
        String query = rows + " | INTO " + destination + options;
        request.setJsonEntity(Strings.toString(JsonXContent.contentBuilder().startObject().field("query", query).endObject()));
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder().addHeader("es-security-runas-user", user).setWarningsHandler(WarningsHandler.PERMISSIVE)
        );
        return client().performRequest(request);
    }

    private static void assertSummary(Response response) throws IOException {
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        // total, created, updated, version_conflicts, failures, failure_details
        assertThat(entityAsMap(response).get("values"), equalTo(List.of(Arrays.asList(2, 2, 0, 0, 0, null))));
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
