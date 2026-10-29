/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.audit;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.LogType;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail;
import org.junit.ClassRule;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class AuditIT extends ESRestTestCase {

    private static final String API_USER = "api_user";
    private static final String SECONDARY_USER = "secondary_user";
    private static final String SECONDARY_PASSWORD = "secondary-password";
    private static final DateTimeFormatter TSTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss,SSSZ");

    private static final String ENCRYPTION_PASSWORD_ID = "test";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .nodes(1) // A single node makes it easier to find audit events
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.security.audit.enabled", "true")
        .setting("xpack.security.audit.logfile.events.include", "[ \"_all\" ]")
        .setting("xpack.security.audit.logfile.events.emit_request_body", "true")
        .setting("esql.federation.enabled", "true")
        .keystore("cluster.state.encryption.password." + ENCRYPTION_PASSWORD_ID, "audit-it-encryption-password")
        .keystore("cluster.state.encryption.active_password_id", ENCRYPTION_PASSWORD_ID)
        .user("admin_user", "admin-password")
        .user(API_USER, "api-password", "superuser", false)
        .user(SECONDARY_USER, SECONDARY_PASSWORD, "superuser", false)
        .build();

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue("admin_user", new SecureString("admin-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(API_USER, new SecureString("api-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testAuditAuthenticationSuccess() throws Exception {
        final Request request = new Request("GET", "/_security/_authenticate");
        executeAndVerifyAudit(request, AuditLevel.AUTHENTICATION_SUCCESS, event -> {
            assertThat(event, hasEntry(LoggingAuditTrail.AUTHENTICATION_TYPE_FIELD_NAME, "REALM"));
        });
    }

    public void testAuditAuthenticationFailure() throws Exception {
        final Request request = new Request("GET", "/_security/_authenticate");
        String basicAuth = basicAuthHeaderValue(API_USER, new SecureString(new char[0]));
        request.setOptions(request.getOptions().toBuilder().addHeader("Authorization", basicAuth).addParameter("ignore", "401"));
        executeAndVerifyAudit(request, AuditLevel.AUTHENTICATION_FAILED, event -> {});
    }

    public void testFilteringOfRequestBodies() throws Exception {
        final String username = randomAlphaOfLength(4) + randomIntBetween(100, 999);
        final Request request = new Request(randomFrom("PUT", "POST"), "/_security/user/" + username);
        final String password = randomAlphaOfLength(4) + randomIntBetween(10, 99) + randomAlphaOfLength(4);
        request.setJsonEntity("{ \"password\":\"" + password + "\", \"roles\":[\"superuser\"] }");
        executeAndVerifyAudit(request, AuditLevel.AUTHENTICATION_SUCCESS, event -> {
            assertThat(event, hasEntry(LoggingAuditTrail.REQUEST_BODY_FIELD_NAME, "{\"roles\":[\"superuser\"]}"));
            assertThat(toJson(event), not(containsString(password)));
        });
    }

    public void testFilteringOfDataSourceCredentials() throws Exception {
        final String dataSourceName = randomAlphaOfLength(4).toLowerCase(Locale.ROOT) + randomIntBetween(100, 999);
        final String accessKey = randomAlphaOfLength(20);
        final String secretKey = randomAlphaOfLength(40);
        final Request request = new Request("PUT", "/_query/data_source/" + dataSourceName);
        request.setJsonEntity(
            "{\"type\":\"s3\",\"settings\":{\"access_key\":\""
                + accessKey
                + "\",\"secret_key\":\""
                + secretKey
                + "\",\"endpoint\":\"https://s3.us-east-1.amazonaws.com\"}}"
        );
        executeAndVerifyAudit(request, AuditLevel.AUTHENTICATION_SUCCESS, event -> {
            String body = asInstanceOf(String.class, event.get(LoggingAuditTrail.REQUEST_BODY_FIELD_NAME));
            assertThat(body, containsString("\"type\""));
            assertThat(body, not(containsString(accessKey)));
            assertThat(body, not(containsString(secretKey)));
            assertThat(toJson(event), not(containsString(accessKey)));
            assertThat(toJson(event), not(containsString(secretKey)));
        });
    }

    /**
     * Verifies that a data source PUT via the {@code source} query parameter is rejected with HTTP 400.
     * Using {@code contentParser()} instead of {@code contentOrSourceParamParser()} in the REST handler
     * prevents successful registration from a query string, which is the primary goal: credentials in
     * the query string would otherwise be stored in cluster state and appear in query logs.
     * <p>
     * Note: the {@code url.query} field of an {@code authentication_failed} audit event is still written
     * before the handler runs (SecurityRestFilter writes auth events before dispatching), so the query
     * string is audited regardless of whether the handler accepts the request. This test only asserts
     * that no successful registration occurs.
     */
    public void testDataSourceDefinitionInQueryStringRejected() throws Exception {
        final String dataSourceName = randomAlphaOfLength(4).toLowerCase(Locale.ROOT) + randomIntBetween(100, 999);
        final String accessKey = randomAlphaOfLength(20);
        final String secretKey = randomAlphaOfLength(40);
        final Request request = new Request("PUT", "/_query/data_source/" + dataSourceName);
        request.addParameter(
            "source",
            "{\"type\":\"s3\",\"settings\":{\"access_key\":\""
                + accessKey
                + "\",\"secret_key\":\""
                + secretKey
                + "\",\"endpoint\":\"http://localhost:12345\"}}"
        );
        request.addParameter("source_content_type", "application/json");
        request.addParameter("ignore", "400");
        final Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(400));
    }

    public void testAuditAuthenticationSuccessForStreamingRequest() throws Exception {
        final Request request = new Request("POST", "/testindex/_bulk");
        final String content = """
            {"index":{}}
            {}
            """;
        request.setEntity(new StringEntity(content, ContentType.create("application/x-ndjson", StandardCharsets.UTF_8)));
        executeAndVerifyAudit(
            request,
            AuditLevel.AUTHENTICATION_SUCCESS,
            event -> assertThat(
                event,
                allOf(
                    hasEntry(LoggingAuditTrail.AUTHENTICATION_TYPE_FIELD_NAME, "REALM"),
                    hasEntry(LoggingAuditTrail.REQUEST_BODY_FIELD_NAME, content)
                )
            )
        );
    }

    public void testAuditSecondaryAuthenticationSuccess() throws Exception {
        final Request request = new Request("POST", "/testindex/_bulk");
        final String body = "{\"index\":{}}\n{}\n";
        request.setEntity(new StringEntity(body, ContentType.create("application/x-ndjson", StandardCharsets.UTF_8)));
        final String opaqueId = withSecondaryAuth(request, SECONDARY_PASSWORD);
        final Instant start = auditTimestampNow();
        assertThat(executeRequest(request).getStatusLine().getStatusCode(), equalTo(200));

        assertBusy(() -> {
            final Map<String, Object> primary = findRestEvent(AuditLevel.AUTHENTICATION_SUCCESS, API_USER, request, start);
            final Map<String, Object> secondary = findRestEvent(AuditLevel.AUTHENTICATION_SUCCESS, SECONDARY_USER, request, start);
            assertCorrelated(primary, secondary, opaqueId);
            assertThat(primary, hasEntry(LoggingAuditTrail.REQUEST_BODY_FIELD_NAME, body));
            assertThat(secondary, hasEntry(LoggingAuditTrail.REQUEST_BODY_FIELD_NAME, body));
        }, 5, TimeUnit.SECONDS);
    }

    public void testAuditSecondaryAuthenticationFailure() throws Exception {
        final Request request = new Request("GET", "/_security/_authenticate");
        final String opaqueId = withSecondaryAuth(request, "wrong-" + SECONDARY_PASSWORD);
        request.addParameter("ignore", "401");
        final Instant start = auditTimestampNow();
        assertThat(executeRequest(request).getStatusLine().getStatusCode(), equalTo(401));

        assertBusy(() -> {
            final Map<String, Object> primary = findRestEvent(AuditLevel.AUTHENTICATION_SUCCESS, API_USER, request, start);
            final Map<String, Object> secondary = findRestEvent(AuditLevel.AUTHENTICATION_FAILED, SECONDARY_USER, request, start);
            assertCorrelated(primary, secondary, opaqueId);
        }, 5, TimeUnit.SECONDS);
    }

    /**
     * The failure is audited, but must not turn the routing error into an authentication error.
     */
    public void testAuditSecondaryAuthenticationFailureWithoutHandler() throws Exception {
        final Request request = new Request("DELETE", "/_security/_authenticate");
        withSecondaryAuth(request, "wrong-" + SECONDARY_PASSWORD);
        request.addParameter("ignore", "405");
        final Instant start = auditTimestampNow();
        assertThat(executeRequest(request).getStatusLine().getStatusCode(), equalTo(405));

        assertBusy(() -> {
            findRestEvent(AuditLevel.AUTHENTICATION_FAILED, SECONDARY_USER, request, start);
            assertThat(findRestEvents(AuditLevel.AUTHENTICATION_SUCCESS, API_USER, request, start), hasSize(0));
        }, 5, TimeUnit.SECONDS);
    }

    public void testNoSecondaryAuthenticationWhenPrimaryAuthenticationFails() throws Exception {
        final Request request = new Request("GET", "/_security/_authenticate");
        withSecondaryAuth(request, "wrong-" + SECONDARY_PASSWORD);
        request.setOptions(
            request.getOptions()
                .toBuilder()
                .addHeader("Authorization", basicAuthHeaderValue(API_USER, new SecureString("wrong-password".toCharArray())))
        );
        request.addParameter("ignore", "401");
        final Instant start = auditTimestampNow();
        assertThat(executeRequest(request).getStatusLine().getStatusCode(), equalTo(401));

        assertBusy(() -> {
            findRestEvent(AuditLevel.AUTHENTICATION_FAILED, API_USER, request, start);
            assertThat(findRestEvents(AuditLevel.AUTHENTICATION_FAILED, SECONDARY_USER, request, start), hasSize(0));
        }, 5, TimeUnit.SECONDS);
    }

    private static String withSecondaryAuth(Request request, String secondaryPassword) {
        final String opaqueId = randomAlphaOfLength(12);
        request.setOptions(
            request.getOptions()
                .toBuilder()
                .addHeader(
                    "es-secondary-authorization",
                    basicAuthHeaderValue(SECONDARY_USER, new SecureString(secondaryPassword.toCharArray()))
                )
                .addHeader(Task.X_OPAQUE_ID_HTTP_HEADER, opaqueId)
        );
        return opaqueId;
    }

    /**
     * Primary and secondary authentication have their own request ids, the opaque id links them.
     */
    private static void assertCorrelated(Map<String, Object> primary, Map<String, Object> secondary, String opaqueId) {
        assertThat(primary, hasEntry(LoggingAuditTrail.OPAQUE_ID_FIELD_NAME, opaqueId));
        assertThat(secondary, hasEntry(LoggingAuditTrail.OPAQUE_ID_FIELD_NAME, opaqueId));
        assertThat(
            secondary.get(LoggingAuditTrail.REQUEST_ID_FIELD_NAME),
            allOf(notNullValue(), not(equalTo(primary.get(LoggingAuditTrail.REQUEST_ID_FIELD_NAME))))
        );
    }

    private Map<String, Object> findRestEvent(AuditLevel level, String principal, Request request, Instant start) throws IOException {
        final List<Map<String, Object>> events = findRestEvents(level, principal, request, start);
        assertThat("[" + level + "] events for [" + principal + "]", events, hasSize(1));
        return events.get(0);
    }

    private List<Map<String, Object>> findRestEvents(AuditLevel level, String principal, Request request, Instant start)
        throws IOException {
        try (var auditLog = cluster.getNodeLog(0, LogType.AUDIT)) {
            return findEvents(Streams.readAllLines(auditLog), level, e -> {
                if (principal.equals(e.get(LoggingAuditTrail.PRINCIPAL_FIELD_NAME)) == false
                    || request.getEndpoint().equals(e.get(LoggingAuditTrail.URL_PATH_FIELD_NAME)) == false
                    || request.getMethod().equals(e.get(LoggingAuditTrail.REQUEST_METHOD_FIELD_NAME)) == false) {
                    return false;
                }
                Instant tstamp = ZonedDateTime.parse(String.valueOf(e.get(LoggingAuditTrail.TIMESTAMP)), TSTAMP_FORMATTER).toInstant();
                return tstamp.isBefore(start) == false;
            });
        }
    }

    public void testFilteringOfCloneApiKeyRequestBody() throws Exception {
        final Request createRequest = new Request("POST", "/_security/api_key");
        createRequest.setJsonEntity("{\"name\":\"source-key\"}");
        final Response createResponse = client().performRequest(createRequest);
        final Map<String, Object> createResponseMap = responseAsMap(createResponse);
        final String encodedCredential = (String) createResponseMap.get("encoded");
        assertThat(encodedCredential, notNullValue());

        final Request cloneRequest = new Request("POST", "/_security/api_key/clone");
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject().field("api_key", encodedCredential).field("name", "cloned-key").endObject();
            cloneRequest.setJsonEntity(Strings.toString(builder));
        }
        executeAndVerifyAudit(cloneRequest, AuditLevel.AUTHENTICATION_SUCCESS, event -> {
            String body = asInstanceOf(String.class, event.get("request.body"));
            assertThat(body, equalTo("{\"name\":\"cloned-key\"}"));
            final String eventJson = toJson(event);
            assertThat(eventJson, not(containsString(encodedCredential)));
        });
    }

    private void executeAndVerifyAudit(Request request, AuditLevel eventType, CheckedConsumer<Map<String, Object>, Exception> assertions)
        throws Exception {
        Instant start = auditTimestampNow();
        executeRequest(request);
        assertBusy(() -> {
            try (var auditLog = cluster.getNodeLog(0, LogType.AUDIT)) {
                final List<String> lines = Streams.readAllLines(auditLog);
                final List<Map<String, Object>> events = findEvents(lines, eventType, e -> {
                    if (API_USER.equals(e.get(LoggingAuditTrail.PRINCIPAL_FIELD_NAME)) == false) {
                        return false;
                    }
                    Instant tstamp = ZonedDateTime.parse(String.valueOf(e.get(LoggingAuditTrail.TIMESTAMP)), TSTAMP_FORMATTER).toInstant();
                    if (tstamp.isBefore(start)) {
                        return false;
                    }
                    return true;
                });
                if (events.isEmpty()) {
                    fail("Could not find any [" + eventType + "] events for [" + API_USER + "] in [" + String.join("\n", lines) + "]");
                }
                assertThat(events, hasSize(1));
                final Map<String, Object> event = events.get(0);
                assertThat(event, hasEntry("type", "audit"));
                assertThat(event, hasKey(LoggingAuditTrail.NODE_ID_FIELD_NAME));
                assertThat(event, hasKey(LoggingAuditTrail.REQUEST_ID_FIELD_NAME));
                assertThat(event, hasEntry(LoggingAuditTrail.EVENT_TYPE_FIELD_NAME, "rest"));
                assertThat(event, hasEntry(LoggingAuditTrail.ORIGIN_TYPE_FIELD_NAME, "rest"));
                assertThat(event, hasEntry(LoggingAuditTrail.URL_PATH_FIELD_NAME, request.getEndpoint()));
                assertThat(event, hasEntry(LoggingAuditTrail.REQUEST_METHOD_FIELD_NAME, request.getMethod()));
                assertions.accept(event);

            }
        }, 5, TimeUnit.SECONDS);
    }

    /**
     * Audit timestamps have millisecond precision, so an event in the same millisecond would otherwise appear to precede the start.
     */
    private static Instant auditTimestampNow() {
        return Instant.now().truncatedTo(ChronoUnit.MILLIS);
    }

    private static Response executeRequest(Request request) throws IOException {
        return client().performRequest(request);
    }

    private List<Map<String, Object>> findEvents(List<String> lines, AuditLevel level, Predicate<Map<String, Object>> filter) {
        final String eventType = level.name().toLowerCase(Locale.ROOT);
        final List<Map<String, Object>> events = new ArrayList<>();
        for (var line : lines) {
            if (line.contains(eventType)) {
                Map<String, Object> event = XContentHelper.convertToMap(XContentType.JSON.xContent(), line, true);
                if (event.get(LoggingAuditTrail.EVENT_ACTION_FIELD_NAME).equals(eventType) && filter.test(event)) {
                    events.add(event);
                }
            }
        }
        return events;
    }

    private static String toJson(Map<String, ? extends Object> map) throws IOException {
        final XContentBuilder builder = XContentFactory.jsonBuilder().map(map);
        final BytesReference bytes = BytesReference.bytes(builder);
        return bytes.utf8ToString();
    }

}
