/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.audit;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.CheckedConsumer;
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
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

public class AuditIT extends ESRestTestCase {

    private static final String API_USER = "api_user";
    private static final DateTimeFormatter TSTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss,SSSZ");

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .nodes(1) // A single node makes it easier to find audit events
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.security.audit.enabled", "true")
        .setting("xpack.security.audit.logfile.events.include", "[ \"_all\" ]")
        .setting("xpack.security.audit.logfile.events.emit_request_body", "true")
        .user("admin_user", "admin-password")
        .user(API_USER, "api-password", "superuser", false)
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

    private void executeAndVerifyAudit(Request request, AuditLevel eventType, CheckedConsumer<Map<String, Object>, Exception> assertions)
        throws Exception {
        Instant start = Instant.now();
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
