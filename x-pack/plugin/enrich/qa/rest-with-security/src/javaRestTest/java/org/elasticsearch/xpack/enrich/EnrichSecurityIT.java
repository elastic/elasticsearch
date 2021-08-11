/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.enrich;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.enrich.CommonEnrichRestTestCase;

import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;

public class EnrichSecurityIT extends CommonEnrichRestTestCase {

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("test_enrich", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue("test_admin", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    public void testInsufficientPermissionsOnNonExistentIndex() throws Exception {
        // This test is here because it requires a valid user that has permission to execute policy PUTs but should fail if the user
        // does not have access to read the backing indices used to enrich the data.
        Request request = new Request("PUT", "/some-other-index");
        request.setJsonEntity("{\n \"mappings\" : {" + createSourceIndexMapping() + "} }");
        adminClient().performRequest(request);
        Request putPolicyRequest = new Request("PUT", "/_enrich/policy/my_policy");
        putPolicyRequest.setJsonEntity(generatePolicySource("some-other-index"));
        ResponseException exc = expectThrows(ResponseException.class, () -> client().performRequest(putPolicyRequest));
        assertThat(
                exc.getMessage(),
                containsString("unable to store policy because no indices match with the specified index patterns [some-other-index]")
        );
    }

    public void testEnrichRoleCannotAccessEnrichIndices() throws Exception {
        testBasicFlow();
        // enrich_user cannot read the .enrich* indices
        {
            Request request = new Request("GET", "/.enrich*/_search?allow_no_indices=false&expand_wildcards=all");
            if (randomBoolean()) {
                RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder()
                        .addHeader("X-elastic-product-origin", "dummy");
                request.setOptions(builder.build());
            }
            ResponseException exc = expectThrows(ResponseException.class, () -> client().performRequest(request));
            assertThat(
                    exc.getMessage(),
                    containsString("no such index [.enrich*]")
            );
        }

        // admin does see them
        {
            Request request = new Request("GET", "/.enrich*/_search?allow_no_indices=false&expand_wildcards=all");
            RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
            builder.setWarningsHandler(WarningsHandler.PERMISSIVE);
            request.setOptions(builder.build());
            Map<String, Object> response = entityAsMap(adminClient().performRequest(request));
            Map<?, ?> hits = (Map<?, ?>) response.get("hits");
            assertThat((Integer) ((Map<?, ?>) hits.get("total")).get("value"), greaterThanOrEqualTo(1));
        }

        // enrich_user cannot see the .enrich* indices
        {
            Request request = new Request("GET", "/_resolve/index/.enrich-my_policy*?expand_wildcards=all");
            if (randomBoolean()) {
                RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder()
                        .addHeader("X-elastic-product-origin", "dummy");
                request.setOptions(builder.build());
            }
            Map<String, Object> response = toMap(client().performRequest(request));
            assertThat(((List<?>) response.get("indices")).isEmpty(), is(true));
            assertThat(((List<?>) response.get("aliases")).isEmpty(), is(true));
            assertThat(((List<?>) response.get("data_streams")).isEmpty(), is(true));
        }
    }
}
