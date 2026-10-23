/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.apache.http.util.EntityUtils;
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
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.hasCapabilities;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * {@code INTO} runs with the caller's privileges: the bulks are authorized as the user who asked, and what the user may
 * learn about the destination is decided by those privileges, not by what the cluster state holds. Each user here has
 * the least the case needs, so a step that asks for more than it should (a destination check that runs before
 * authorization) fails for the user it should not.
 */
public class EsqlSecurityIntoIT extends ESRestTestCase {
    private static final String PASSWORD = "x-pack-test-password";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.enabled", "true")
        .rolesFile(Resource.fromClasspath("into_roles.yml"))
        .user("test-admin", PASSWORD, "test-admin", true)
        .user("writer", PASSWORD, "into_write", false)
        .user("creator", PASSWORD, "into_create_doc", false)
        .user("reader", PASSWORD, "into_read_only", false)
        .user("outsider", PASSWORD, "into_no_access", false)
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
    public void setupIndices() throws Exception {
        assumeTrue("INTO is behind the esql_into feature flag", hasCapabilities(client(), List.of("into_command")));
        createIndex("src", Settings.EMPTY, """
            {"properties": {"host": {"type": "keyword"}, "cpu": {"type": "double"}}}
            """);
        createIndex("dst", Settings.EMPTY, """
            {"properties": {"host": {"type": "keyword"}, "cpu": {"type": "double"}}}
            """);
        createIndex("dst-strict", Settings.EMPTY, """
            {"dynamic": "strict", "properties": {"host": {"type": "keyword"}, "cpu": {"type": "double"}}}
            """);
        Request template = new Request("PUT", "_index_template/tmpl");
        template.setJsonEntity("""
            {
              "index_patterns": ["tmpl-*"],
              "template": {"mappings": {"properties": {"host": {"type": "keyword"}, "cpu": {"type": "double"}}}}
            }
            """);
        assertOK(client().performRequest(template));
        Request bulk = new Request("POST", "src/_bulk");
        bulk.addParameter("refresh", "true");
        bulk.setJsonEntity("""
            {"index": {}}
            {"host": "a", "cpu": 1.0}
            {"index": {}}
            {"host": "b", "cpu": 2.0}
            {"index": {}}
            {"host": "c", "cpu": 3.0}
            """);
        assertOK(client().performRequest(bulk));
    }

    /**
     * A user with only {@code write} may not refresh the destination. The rows are written all the same, so the query
     * succeeds, with a warning that they may not be searchable yet, rather than failing after the write and inviting a
     * retry that writes them again.
     */
    public void testWriteOnlyUserWritesAndIsToldTheRefreshDidNotHappen() throws IOException {
        Request request = intoRequest("writer", "FROM src | KEEP host, cpu | INTO dst");
        request.setOptions(request.getOptions().toBuilder().setWarningsHandler(w -> false));
        Response response = client().performRequest(request);
        // total, created, updated, version_conflicts, batches, failures, failure_messages
        assertThat(singleRow(response).subList(0, 3), equalTo(List.of(3, 3, 0)));
        assertThat(response.getWarnings().toString(), containsString("could not refresh the destination"));
        refresh("dst");
        assertThat(count("dst"), equalTo(3));
    }

    /**
     * A caller with no privilege to write to a name learns nothing about it: the refusal is the same whether the
     * destination has a strict mapping that the query would violate, is only readable by the caller, or does not exist.
     */
    public void testNoPrivilegeToWriteRevealsNothingAboutTheDestination() throws IOException {
        String query = "FROM src | EVAL surprise = 1 | KEEP host, surprise | INTO ";
        for (String user : List.of("outsider", "reader")) {
            for (String destination : List.of("dst-strict", "dst", "no-such-index")) {
                ResponseException e = expectThrows(ResponseException.class, () -> runInto(user, query + destination));
                String reason = errorBody(e);
                assertThat(user + " into " + destination, e.getResponse().getStatusLine().getStatusCode(), equalTo(403));
                assertThat(reason, containsString("unauthorized"));
                assertThat(reason, not(containsString("surprise")));
                assertThat(reason, not(containsString("dynamic")));
                assertThat(reason, not(containsString("does not exist")));
            }
        }
        // Someone who may write to the destination does hear what is wrong with the query.
        ResponseException e = expectThrows(ResponseException.class, () -> runInto("writer", query + "dst-strict"));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(errorBody(e), containsString("surprise"));
        assertThat(count("dst"), equalTo(0));
    }

    /** Replacing needs the {@code index} privilege; a user who may only create documents is refused, and writes nothing. */
    public void testReplaceNeedsMoreThanCreateDoc() throws IOException {
        ResponseException e = expectThrows(
            ResponseException.class,
            () -> runInto("creator", "FROM src | KEEP host, cpu | INTO dst WITH { \"collisions\": \"replace\" }")
        );
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(403));
        assertThat(count("dst"), equalTo(0));

        List<?> stats = singleRow(runInto("creator", "FROM src | KEEP host, cpu | INTO dst"));
        assertThat(stats.subList(0, 3), equalTo(List.of(3, 3, 0)));
        refresh("dst");
        assertThat(count("dst"), equalTo(3));
    }

    /**
     * A destination that has to be created from a template needs a privilege the user lacks, so the bulk is refused per
     * item. That is the caller not being allowed to write, not a row the destination dislikes: {@code keep_going} must
     * not turn it into counted failures behind a 200.
     */
    public void testKeepGoingDoesNotTurnAForbiddenWriteIntoRowFailures() throws IOException {
        ResponseException e = expectThrows(
            ResponseException.class,
            () -> runInto("creator", "FROM src | KEEP host, cpu | INTO tmpl-new WITH { \"collisions\": \"keep\", \"keep_going\": true }")
        );
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(403));
        assertThat(errorBody(e), containsString("unauthorized"));
    }

    private Response runInto(String user, String query) throws IOException {
        Request request = intoRequest(user, query);
        // Whoever may not refresh the destination is warned about it; these tests are not about that.
        request.setOptions(request.getOptions().toBuilder().setWarningsHandler(w -> false));
        return client().performRequest(request);
    }

    private static Request intoRequest(String user, String query) throws IOException {
        XContentBuilder json = JsonXContent.contentBuilder().startObject().field("query", query).endObject();
        Request request = new Request("POST", "_query");
        request.setJsonEntity(Strings.toString(json));
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("es-security-runas-user", user));
        return request;
    }

    private static List<?> singleRow(Response response) throws IOException {
        Map<String, Object> body = entityAsMap(response);
        List<?> rows = (List<?>) body.get("values");
        assertThat(rows.size(), equalTo(1));
        return (List<?>) rows.get(0);
    }

    private int count(String index) throws IOException {
        Request request = new Request("POST", "_query");
        XContentBuilder json = JsonXContent.contentBuilder()
            .startObject()
            .field("query", "FROM " + index + " | STATS c = COUNT(*) | LIMIT 10");
        request.setJsonEntity(Strings.toString(json.endObject()));
        return ((Number) singleRow(client().performRequest(request)).get(0)).intValue();
    }

    private static String errorBody(ResponseException e) throws IOException {
        return EntityUtils.toString(e.getResponse().getEntity());
    }
}
