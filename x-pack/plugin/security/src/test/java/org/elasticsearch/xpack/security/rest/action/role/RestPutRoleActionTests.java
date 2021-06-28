/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.role;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.elasticsearch.xpack.core.security.action.role.PutRoleRequest;
import org.elasticsearch.xpack.core.security.action.role.PutRoleResponse;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class RestPutRoleActionTests extends RestActionTestCase {

    final List<String> contentTypeHeader = Collections.singletonList(randomCompatibleMediaType(RestApiVersion.V_7));
    final List<String> jsonContentType = Collections.singletonList(XContentType.JSON.toParsedMediaType().responseContentTypeHeader());

    @Before
    public void setUpAction() {
        controller().registerHandler(new RestPutRoleAction(Settings.EMPTY, new XPackLicenseState(Settings.EMPTY, () -> 0)));
        verifyingClient.setExecuteVerifier((actionType, request) -> {
            assertThat(request, instanceOf(PutRoleRequest.class));
            return Mockito.mock(PutRoleResponse.class);
        });
    }


    public void testCreationOfRoleWithMalformedQueryJsonFails() {

        String[] malformedQueries = new String[]{"{ \"match_all\": { \"unknown_field\": \"\" } }",
            "{ malformed JSON }",
            "{ \"unknown\": {\"\"} }",
            "{}"};

        String queryPattern = "{" +
            "\"indices\": [\n" +
            "    {\n" +
            "      \"names\": \"index\",\n" +
            "      \"privileges\": [\n" +
            "        \"all\"\n" +
            "      ],\n" +
            "      \"query\": %s" +
            "      }\n" +
            "    }\n" +
            "  ]" +
            "}";
        for (String malformedQuery : malformedQueries) {

            final String query = String.format(Locale.ROOT, queryPattern, malformedQuery);
            FakeRestRequest.Builder deprecatedRequest = new FakeRestRequest.Builder(xContentRegistry())
                .withHeaders(Map.of("Content-Type", jsonContentType, "Accept", jsonContentType))
                .withPath("_security/role/rolename")
                .withMethod(RestRequest.Method.POST)
                .withContent(new BytesArray(query), null);

            RestRequest request = deprecatedRequest.build();
            FakeRestChannel channel = new FakeRestChannel(request, true, 1);

            dispatchRequest(request, channel);

            assertThat(channel.errors().get(), equalTo(1));
            assertThat(channel.responses().get(), equalTo(0));
            assertThat(channel.capturedResponse().status(), equalTo(RestStatus.BAD_REQUEST));
        }
    }
}
