/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.settings;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.reservedstate.action.ReservedClusterSettingsAction;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockUtils;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Mockito.mock;

public class ClusterUpdateSettingsRequestTests extends ESTestCase {

    public void testFromXContent() throws IOException {
        doFromXContentTestWithRandomFields(false);
    }

    public void testFromXContentWithRandomFields() throws IOException {
        doFromXContentTestWithRandomFields(true);
    }

    private void doFromXContentTestWithRandomFields(boolean addRandomFields) throws IOException {
        final ClusterUpdateSettingsRequest request = createTestItem();
        boolean humanReadable = randomBoolean();
        final XContentType xContentType = XContentType.JSON;
        BytesReference originalBytes = toShuffledXContent(request, xContentType, ToXContent.EMPTY_PARAMS, humanReadable);

        if (addRandomFields) {
            String unsupportedField = "unsupported_field";
            BytesReference mutated = BytesReference.bytes(
                XContentTestUtils.insertIntoXContent(
                    xContentType.xContent(),
                    originalBytes,
                    Collections.singletonList(""),
                    () -> unsupportedField,
                    () -> randomAlphaOfLengthBetween(3, 10)
                )
            );
            XContentParseException iae = expectThrows(XContentParseException.class, () -> {
                try (var parser = createParser(xContentType.xContent(), mutated)) {
                    ClusterUpdateSettingsRequest.fromXContent(ClusterUpdateSettingsRequestTests::newTestRequest, parser);
                }
            });
            assertThat(iae.getMessage(), containsString("[cluster_update_settings_request] unknown field [" + unsupportedField + "]"));
        } else {
            try (XContentParser parser = createParser(xContentType.xContent(), originalBytes)) {
                var parsedRequest = ClusterUpdateSettingsRequest.fromXContent(ClusterUpdateSettingsRequestTests::newTestRequest, parser);

                assertNull(parser.nextToken());
                assertThat(parsedRequest.transientSettings(), equalTo(request.transientSettings()));
                assertThat(parsedRequest.persistentSettings(), equalTo(request.persistentSettings()));
            }
        }
    }

    private static ClusterUpdateSettingsRequest createTestItem() {
        ClusterUpdateSettingsRequest request = new ClusterUpdateSettingsRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT);
        request.persistentSettings(ClusterUpdateSettingsResponseTests.randomClusterSettings(0, 2));
        request.transientSettings(ClusterUpdateSettingsResponseTests.randomClusterSettings(0, 2));
        return request;
    }

    public void testOperatorHandler() throws IOException {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        final ThreadPool threadPool = mock(ThreadPool.class);
        TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor(threadPool);
        TransportClusterUpdateSettingsAction action = new TransportClusterUpdateSettingsAction(
            transportService,
            mock(ClusterService.class),
            mock(RerouteService.class),
            threadPool,
            mock(ActionFilters.class),
            clusterSettings
        );

        assertEquals(ReservedClusterSettingsAction.NAME, action.reservedStateHandlerName().get());

        String oneSettingJSON = """
            {
                "persistent": {
                    "indices.recovery.max_bytes_per_sec": "25mb",
                    "cluster": {
                         "remote": {
                             "cluster_one": {
                                 "seeds": [
                                     "127.0.0.1:9300"
                                 ]
                             }
                         }
                    }
                }
            }""";

        try (XContentParser parser = createParser(XContentType.JSON.xContent(), oneSettingJSON)) {
            var parsedRequest = ClusterUpdateSettingsRequest.fromXContent(ClusterUpdateSettingsRequestTests::newTestRequest, parser);
            assertThat(
                action.modifiedKeys(parsedRequest),
                containsInAnyOrder("indices.recovery.max_bytes_per_sec", "cluster.remote.cluster_one.seeds")
            );
        }
    }

    public static ClusterUpdateSettingsRequest newTestRequest() {
        return new ClusterUpdateSettingsRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT);
    }
}
