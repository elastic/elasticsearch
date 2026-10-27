/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;

import java.io.IOException;
import java.util.Locale;

import static org.hamcrest.Matchers.equalTo;

/**
 * Streamed bulk requests with secondary credentials. A new native user isn't cached, so secondary authentication completes asynchronously.
 */
public class SecondaryAuthIncrementalBulkIT extends SecurityInBasicRestTestCase {

    public void testBulkWithNativeSecondaryAuthUser() throws IOException {
        final String username = randomUsername();
        final Request putUser = new Request("PUT", "/_security/user/" + username);
        putUser.setJsonEntity("{\"password\":\"secondary-password\",\"roles\":[\"superuser\"]}");
        assertOK(adminClient().performRequest(putUser));

        final var response = responseAsMap(adminClient().performRequest(bulkRequest(username, "secondary-password")));
        assertThat(response.toString(), response.get("errors"), equalTo(false));
    }

    public void testBulkWithInvalidSecondaryAuthIsRejected() {
        final Request bulk = bulkRequest(randomUsername(), "secondary-password");
        final ResponseException e = expectThrows(ResponseException.class, () -> adminClient().performRequest(bulk));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(401));
    }

    private static String randomUsername() {
        return "secondary_" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
    }

    private static Request bulkRequest(String username, String password) {
        final Request bulk = new Request("POST", "/_bulk");
        bulk.setOptions(
            RequestOptions.DEFAULT.toBuilder()
                .addHeader("es-secondary-authorization", basicAuthHeaderValue(username, new SecureString(password.toCharArray())))
        );
        final StringBuilder body = new StringBuilder();
        final int docs = randomIntBetween(1, 50);
        for (int i = 0; i < docs; i++) {
            body.append(Strings.format("{\"index\":{\"_index\":\"secondary_auth_bulk\"}}\n{\"value\":%d}\n", i));
        }
        bulk.setJsonEntity(body.toString());
        return bulk;
    }
}
