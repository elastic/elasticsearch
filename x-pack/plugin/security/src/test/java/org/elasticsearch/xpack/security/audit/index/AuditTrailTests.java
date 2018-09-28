/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.audit.index;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.xpack.core.security.ScrollHelper;
import org.elasticsearch.xpack.core.security.authc.AuthenticationServiceField;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.index.IndexAuditTrailField;
import org.elasticsearch.xpack.security.audit.AuditTrail;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.nullValue;

public class AuditTrailTests extends SecurityIntegTestCase {

    private static final String AUTHENTICATE_USER = "http_user";
    private static final String EXECUTE_USER = "exec_user";
    private static final String ROLE_CAN_RUN_AS = "can_run_as";
    private static final String ROLES = ROLE_CAN_RUN_AS + ":\n" + "  run_as: [ '" + EXECUTE_USER + "' ]\n";

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    public Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("xpack.security.audit.enabled", true)
                .put("xpack.security.audit.outputs", "index")
                .putList("xpack.security.audit.index.events.include", "access_denied", "authentication_failed", "run_as_denied")
                .build();
    }

    @Override
    public String configRoles() {
        return ROLES + super.configRoles();
    }

    @Override
    public String configUsers() {
        return super.configUsers()
                + AUTHENTICATE_USER + ":" + SecuritySettingsSource.TEST_PASSWORD_HASHED + "\n"
                + EXECUTE_USER + ":xx_no_password_xx\n";
    }

    @Override
    public String configUsersRoles() {
        return super.configUsersRoles()
                + ROLE_CAN_RUN_AS + ":" + AUTHENTICATE_USER + "\n"
                + "monitoring_user:" + EXECUTE_USER;
    }

    @Override
    public boolean transportSSLEnabled() {
        return true;
    }

    public void testAuditAccessDeniedWithRunAsUser() throws Exception {
        try {
            Request request = new Request("GET", "/.security/_search");
            RequestOptions.Builder options = request.getOptions().toBuilder();
            options.addHeader("Authorization", UsernamePasswordToken.basicAuthHeaderValue(AUTHENTICATE_USER, TEST_PASSWORD_SECURE_STRING));
            options.addHeader(AuthenticationServiceField.RUN_AS_USER_HEADER, EXECUTE_USER);
            request.setOptions(options);
            getRestClient().performRequest(request);
            fail("request should have failed");
        } catch (final ResponseException e) {
            assertThat(e.getResponse().getStatusLine().getStatusCode(), is(403));
        }

        final Collection<Map<String, Object>> events = waitForAuditEvents();

        assertThat(events, iterableWithSize(1));
        final Map<String, Object> event = events.iterator().next();
        assertThat(event.get(IndexAuditTrail.Field.TYPE), equalTo("access_denied"));
        assertThat((List<?>) event.get(IndexAuditTrail.Field.INDICES), containsInAnyOrder(".security"));
        assertThat(event.get(IndexAuditTrail.Field.PRINCIPAL), equalTo(EXECUTE_USER));
        assertThat(event.get(IndexAuditTrail.Field.RUN_BY_PRINCIPAL), equalTo(AUTHENTICATE_USER));
    }


    public void testAuditRunAsDeniedEmptyUser() throws Exception {
        try {
            Request request = new Request("GET", "/.security/_search");
            RequestOptions.Builder options = request.getOptions().toBuilder();
            options.addHeader("Authorization", UsernamePasswordToken.basicAuthHeaderValue(AUTHENTICATE_USER, TEST_PASSWORD_SECURE_STRING));
            options.addHeader(AuthenticationServiceField.RUN_AS_USER_HEADER, "");
            request.setOptions(options);
            getRestClient().performRequest(request);
            fail("request should have failed");
        } catch (final ResponseException e) {
            assertThat(e.getResponse().getStatusLine().getStatusCode(), is(401));
        }

        final Collection<Map<String, Object>> events = waitForAuditEvents();

        assertThat(events, iterableWithSize(1));
        final Map<String, Object> event = events.iterator().next();
        assertThat(event.get(IndexAuditTrail.Field.TYPE), equalTo("run_as_denied"));
        assertThat(event.get(IndexAuditTrail.Field.PRINCIPAL), equalTo(AUTHENTICATE_USER));
        assertThat(event.get(IndexAuditTrail.Field.RUN_AS_PRINCIPAL), equalTo(""));
        assertThat(event.get(IndexAuditTrail.Field.REALM), equalTo("file"));
        assertThat(event.get(IndexAuditTrail.Field.RUN_AS_REALM), nullValue());
    }

    private Collection<Map<String, Object>> waitForAuditEvents() throws InterruptedException {
        waitForAuditTrailToBeWritten();
        final AtomicReference<Collection<Map<String, Object>>> eventsRef = new AtomicReference<>();
        awaitBusy(() -> {
            try {
                final Collection<Map<String, Object>> events = getAuditEvents();
                eventsRef.set(events);
                return events.size() > 0;
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        });

        return eventsRef.get();
    }
    private Collection<Map<String, Object>> getAuditEvents() throws Exception {
        final Client client = client();
        final DateTime now = new DateTime(DateTimeZone.UTC);
        final String indexName = IndexNameResolver.resolve(IndexAuditTrailField.INDEX_NAME_PREFIX, now, IndexNameResolver.Rollover.DAILY);

        assertTrue(awaitBusy(() -> indexExists(client, indexName), 5, TimeUnit.SECONDS));

        client.admin().indices().refresh(Requests.refreshRequest(indexName)).get();

        final SearchRequest request = client.prepareSearch(indexName)
                .setScroll(TimeValue.timeValueMinutes(10L))
                .setTypes(IndexAuditTrail.DOC_TYPE)
                .setQuery(QueryBuilders.matchAllQuery())
                .setSize(1000)
                .setFetchSource(true)
                .request();
        request.indicesOptions().ignoreUnavailable();

        final PlainActionFuture<Collection<Map<String, Object>>> listener = new PlainActionFuture<>();
        ScrollHelper.fetchAllByEntity(client, request, listener, SearchHit::getSourceAsMap);

        return listener.get();
    }

    private boolean indexExists(Client client, String indexName) {
        try {
            final ActionFuture<IndicesExistsResponse> future = client.admin().indices().exists(Requests.indicesExistsRequest(indexName));
            return future.get().isExists();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Failed to check if " + indexName + " exists", e);
        }
    }

    private void waitForAuditTrailToBeWritten() throws InterruptedException {
        final AuditTrailService auditTrailService = (AuditTrailService) internalCluster().getInstance(AuditTrail.class);
        assertThat(auditTrailService.getAuditTrails(), iterableWithSize(1));

        final IndexAuditTrail indexAuditTrail = (IndexAuditTrail) auditTrailService.getAuditTrails().get(0);
        assertTrue(awaitBusy(() -> indexAuditTrail.peek() == null, 5, TimeUnit.SECONDS));
    }
}
