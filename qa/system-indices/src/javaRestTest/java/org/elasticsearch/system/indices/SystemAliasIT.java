/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.system.indices;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.junit.After;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class SystemAliasIT extends AbstractSystemIndicesIT {

    @After
    public void resetFeatures() throws Exception {
        client().performRequest(new Request("POST", "/_features/_reset"));
    }

    public void testCreatingSystemIndexWithAlias() throws Exception {
        {
            Request request = new Request("PUT", "/.internal-unmanaged-index-8");
            request.setJsonEntity("{\"aliases\": {\".internal-unmanaged-alias\": {}}}");
            request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("X-elastic-product-origin", "elastic"));
            Response response = client().performRequest(request);
            assertThat(response.getStatusLine().getStatusCode(), is(200));
        }

        assertAliasIsHiddenInIndexResponse(".internal-unmanaged-index-8", ".internal-unmanaged-alias", true);
        assertAliasIsHiddenInAliasesEndpoint(".internal-unmanaged-index-8", ".internal-unmanaged-alias", true);
    }

    public void testCreatingSystemIndexWithLegacyAlias() throws Exception {
        {
            Request request = new Request("PUT", "/_template/system_template");
            request.setJsonEntity(
                "{"
                    + "  \"index_patterns\": [\".internal-unmanaged-*\"],"
                    + "  \"aliases\": {"
                    + "    \".internal-unmanaged-alias\": {}"
                    + "  }"
                    + "}"
            );
            request.setOptions(expectWarnings("Legacy index templates are deprecated in favor of composable templates."));
            Response response = client().performRequest(request);
            assertThat(response.getStatusLine().getStatusCode(), is(200));
        }

        {
            Request request = new Request("PUT", "/.internal-unmanaged-index-8");
            request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("X-elastic-product-origin", "elastic"));
            Response response = client().performRequest(request);
            assertThat(response.getStatusLine().getStatusCode(), is(200));
        }

        assertAliasIsHiddenInIndexResponse(".internal-unmanaged-index-8", ".internal-unmanaged-alias", false);
        assertAliasIsHiddenInAliasesEndpoint(".internal-unmanaged-index-8", ".internal-unmanaged-alias", false);
    }

    public void testCreatingSystemIndexWithIndexAliasEndpoint() throws Exception {
        {
            Request request = new Request("PUT", "/.internal-unmanaged-index-8");
            request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("X-elastic-product-origin", "elastic"));
            Response response = client().performRequest(request);
            assertThat(response.getStatusLine().getStatusCode(), is(200));
        }

        {
            Request request = new Request("PUT", "/.internal-unmanaged-index-8/_alias/.internal-unmanaged-alias");
            request.setOptions(
                expectWarnings(
                    "this request accesses system indices: [.internal-unmanaged-index-8], "
                        + "but in a future major version, direct access to system indices will be prevented by default"
                )
            );
            Response response = client().performRequest(request);
            assertThat(response.getStatusLine().getStatusCode(), is(200));
        }

        assertAliasIsHiddenInIndexResponse(".internal-unmanaged-index-8", ".internal-unmanaged-alias", true);
        assertAliasIsHiddenInAliasesEndpoint(".internal-unmanaged-index-8", ".internal-unmanaged-alias", true);
    }

    public void testCreatingSystemIndexWithAliasEndpoint() throws Exception {
        {
            Request request = new Request("PUT", "/.internal-unmanaged-index-8");
            request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("X-elastic-product-origin", "elastic"));
            Response response = client().performRequest(request);
            assertThat(response.getStatusLine().getStatusCode(), is(200));
        }

        {
            Request request = new Request("PUT", "/_alias/.internal-unmanaged-alias");
            request.setJsonEntity("{\"index\": \".internal-unmanaged-index-8\"}");
            request.setOptions(
                expectWarnings(
                    "this request accesses system indices: [.internal-unmanaged-index-8], "
                        + "but in a future major version, direct access to system indices will be prevented by default"
                )
            );
            Response response = client().performRequest(request);
            assertThat(response.getStatusLine().getStatusCode(), is(200));
        }

        assertAliasIsHiddenInIndexResponse(".internal-unmanaged-index-8", ".internal-unmanaged-alias", true);
        assertAliasIsHiddenInAliasesEndpoint(".internal-unmanaged-index-8", ".internal-unmanaged-alias", true);
    }

    public void testCreatingSystemIndexWithAliasesEndpoint() throws Exception {
        {
            Request request = new Request("PUT", "/.internal-unmanaged-index-8");
            request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("X-elastic-product-origin", "elastic"));
            Response response = client().performRequest(request);
            assertThat(response.getStatusLine().getStatusCode(), is(200));
        }

        {
            Request request = new Request("POST", "/_aliases");
            request.setJsonEntity(
                "{"
                    + "  \"actions\": ["
                    + "    {"
                    + "      \"add\": {"
                    + "        \"index\": \".internal-unmanaged-index-8\","
                    + "        \"alias\": \".internal-unmanaged-alias\""
                    + "      }"
                    + "    }"
                    + "  ]"
                    + "}"
            );

            request.setOptions(
                expectWarnings(
                    "this request accesses system indices: [.internal-unmanaged-index-8], "
                        + "but in a future major version, direct access to system indices will be prevented by default"
                )
            );
            Response response = client().performRequest(request);
            assertThat(response.getStatusLine().getStatusCode(), is(200));
        }

        assertAliasIsHiddenInIndexResponse(".internal-unmanaged-index-8", ".internal-unmanaged-alias", true);
        assertAliasIsHiddenInAliasesEndpoint(".internal-unmanaged-index-8", ".internal-unmanaged-alias", true);
    }

    @SuppressWarnings("unchecked")
    private void assertAliasIsHiddenInIndexResponse(String indexName, String aliasName, boolean expectMatch) throws IOException {
        Request request = new Request("GET", "/" + indexName);
        request.setOptions(
            expectWarnings(
                "this request accesses system indices: ["
                    + indexName
                    + "], "
                    + "but in a future major version, direct access to system indices will be prevented by default"
            )
        );
        Response response = client().performRequest(request);
        Map<String, Object> responseMap = responseAsMap(response);
        Map<String, Object> indexMap = (Map<String, Object>) responseMap.get(indexName);
        Map<String, Object> settingsMap = (Map<String, Object>) indexMap.get("settings");
        Map<String, Object> indexSettingsMap = (Map<String, Object>) settingsMap.get("index");
        assertThat(indexSettingsMap.get("hidden"), equalTo("true"));

        Map<String, Object> aliasesMap = (Map<String, Object>) indexMap.get("aliases");
        if (expectMatch == false) {
            assertTrue(aliasesMap.keySet().isEmpty());
        } else {
            assertThat(aliasesMap.keySet(), equalTo(Set.of(aliasName)));
            Map<String, Object> aliasMap = (Map<String, Object>) aliasesMap.get(aliasName);
            assertThat(aliasMap.get("is_hidden"), notNullValue());
            assertThat(aliasMap.get("is_hidden"), equalTo(true));
        }
    }

    @SuppressWarnings("unchecked")
    private void assertAliasIsHiddenInAliasesEndpoint(String indexName, String aliasName, boolean expectMatch) throws IOException {
        Request request = new Request("GET", "/_aliases");
        request.setOptions(
            expectWarnings(
                "this request accesses system indices: ["
                    + indexName
                    + "], "
                    + "but in a future major version, direct access to system indices will be prevented by default"
            )
        );
        Response response = client().performRequest(request);
        Map<String, Object> responseMap = responseAsMap(response);
        Map<String, Object> indexAliasMap = (Map<String, Object>) responseMap.get(indexName);
        Map<String, Object> aliasesMap = (Map<String, Object>) indexAliasMap.get("aliases");
        Map<String, Object> aliasMap = (Map<String, Object>) aliasesMap.get(aliasName);
        if (expectMatch == false) {
            assertNull(aliasMap);
        } else {
            assertThat(aliasMap.get("is_hidden"), notNullValue());
            assertThat(aliasMap.get("is_hidden"), equalTo(true));
        }
    }
}
