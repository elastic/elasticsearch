/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.monitoring.rest.action;

import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.monitoring.action.MonitoringMigrateAlertsResponse;
import org.elasticsearch.xpack.core.monitoring.action.MonitoringMigrateAlertsResponse.ExporterMigrationResult;
import org.elasticsearch.xpack.monitoring.exporter.http.HttpExporter;
import org.elasticsearch.xpack.monitoring.exporter.local.LocalExporter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RestMonitoringMigrateAlertsActionTests extends ESTestCase {

    private final RestMonitoringMigrateAlertsAction action = new RestMonitoringMigrateAlertsAction();

    public void testGetName() {
        assertThat(action.getName(), is("monitoring_migrate_alerts"));
    }

    public void testSupportsAllContentTypes() {
        final var route = action.routes().get(0);
        for (var xContentType : XContentType.values()) {
            final var request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(route.getMethod())
                .withPath(route.getPath())
                .withHeaders(Map.of("Content-Type", List.of(xContentType.mediaType())))
                .build();
            assertTrue(xContentType.toString(), action.mediaTypesValid(request));
        }
    }

    public void testRestActionCompletion() throws Exception {
        List<ExporterMigrationResult> migrationResults = new ArrayList<>();
        for (int i = 0; i < randomInt(5); i++) {
            boolean success = randomBoolean();
            migrationResults.add(
                new ExporterMigrationResult(
                    randomAlphaOfLength(10),
                    randomFrom(LocalExporter.TYPE, HttpExporter.TYPE),
                    success,
                    success ? null : new IOException("mock failure")
                )
            );
        }
        MonitoringMigrateAlertsResponse restResponse = new MonitoringMigrateAlertsResponse(migrationResults);

        final RestChannel channel = mock(RestChannel.class);
        when(channel.newBuilder()).thenReturn(JsonXContent.contentBuilder());
        RestResponse response = RestMonitoringMigrateAlertsAction.getRestBuilderListener(channel).buildResponse(restResponse);

        assertThat(response.status(), is(RestStatus.OK));
        assertThat(response.content().utf8ToString(), startsWith("{\"exporters\":["));
    }
}
