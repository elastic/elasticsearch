/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.client.security.support.ServiceAccountInfo;
import org.elasticsearch.client.security.user.privileges.IndicesPrivileges;
import org.elasticsearch.client.security.user.privileges.Role;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class GetServiceAccountsResponseTests extends ESTestCase {

    public void testFromXContent() throws IOException {
        final String json =
            "{\n" +
                "  \"elastic/fleet-server\": {\n" +
                "    \"role_descriptor\": {\n" +
                "      \"cluster\": [\n" +
                "        \"monitor\",\n" +
                "        \"manage_own_api_key\"\n" +
                "      ],\n" +
                "      \"indices\": [\n" +
                "        {\n" +
                "          \"names\": [\n" +
                "            \"logs-*\",\n" +
                "            \"metrics-*\",\n" +
                "            \"traces-*\",\n" +
                "            \"synthetics-*\",\n" +
                "            \".logs-endpoint.diagnostic.collection-*\"\n" +
                "          ],\n" +
                "          \"privileges\": [\n" +
                "            \"write\",\n" +
                "            \"create_index\",\n" +
                "            \"auto_configure\"\n" +
                "          ],\n" +
                "          \"allow_restricted_indices\": false\n" +
                "        },\n" +
                "        {\n" +
                "          \"names\": [\n" +
                "            \".fleet-*\"\n" +
                "          ],\n" +
                "          \"privileges\": [\n" +
                "            \"read\",\n" +
                "            \"write\",\n" +
                "            \"monitor\",\n" +
                "            \"create_index\",\n" +
                "            \"auto_configure\"\n" +
                "          ],\n" +
                "          \"allow_restricted_indices\": false\n" +
                "        }\n" +
                "      ],\n" +
                "      \"applications\": [],\n" +
                "      \"run_as\": [],\n" +
                "      \"metadata\": {},\n" +
                "      \"transient_metadata\": {\n" +
                "        \"enabled\": true\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}\n";

        final GetServiceAccountsResponse getServiceAccountsResponse = GetServiceAccountsResponse.fromXContent(
            XContentType.JSON.xContent().createParser(
                NamedXContentRegistry.EMPTY, DeprecationHandler.IGNORE_DEPRECATIONS, json));

        assertThat(getServiceAccountsResponse.getServiceAccountInfos().size(), equalTo(1));
        final ServiceAccountInfo serviceAccountInfo = getServiceAccountsResponse.getServiceAccountInfos().get(0);
        assertThat(serviceAccountInfo.getPrincipal(), equalTo("elastic/fleet-server"));
        assertThat(serviceAccountInfo.getRole(), equalTo(
            Role.builder()
                .name("role_descriptor")
                .clusterPrivileges("monitor", "manage_own_api_key")
                .indicesPrivileges(
                    IndicesPrivileges.builder()
                        .indices("logs-*", "metrics-*", "traces-*", "synthetics-*", ".logs-endpoint.diagnostic.collection-*")
                        .privileges("write", "create_index", "auto_configure")
                        .allowRestrictedIndices(false)
                        .build(),
                    IndicesPrivileges.builder()
                        .indices(".fleet-*")
                        .privileges("read", "write", "monitor", "create_index", "auto_configure")
                        .allowRestrictedIndices(false)
                        .build()
                )
                .build()
        ));
    }
}
