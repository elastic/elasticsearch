/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Strings;
import org.elasticsearch.xpack.security.SecurityOnTrialLicenseRestTestCase;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

/**
 * Verifies that the Rollover API authorizes the aliases and the explicit new index name carried in its request,
 * so that a caller scoped to their own namespace cannot attach an out-of-namespace alias or create an out-of-namespace
 * index by rolling over an alias they control.
 */
public class RolloverAliasSecurityRestIT extends SecurityOnTrialLicenseRestTestCase {

    private static final SecureString PASSWORD = new SecureString("rollover-test-password".toCharArray());

    public void testRolloverCannotAttachOutOfNamespaceAlias() throws IOException {
        createNamespaceScopedUser();
        createWriteAliasIndex("my-index-000001", "my-alias");

        // Denied: the caller has no privilege on the out-of-namespace alias it tries to attach via the rollover body.
        final Request injectAlias = rolloverRequest("my-alias", null, """
            {"aliases":{"other-alias":{"is_write_index":true}}}""");
        final ResponseException e = expectThrows(ResponseException.class, () -> performRequestAsUser("my_user", injectAlias));
        assertEquals(403, e.getResponse().getStatusLine().getStatusCode());
        assertThat(e.getMessage(), containsString("action [indices:admin/aliases] is unauthorized"));
        // the denial names the offending alias, not the rollover target
        assertThat(e.getMessage(), containsString("on indices [other-alias]"));

        // Allowed: an in-namespace body alias still works, so the check does not over-restrict.
        final Request inNamespaceAlias = rolloverRequest("my-alias", null, """
            {"aliases":{"my-second-alias":{}}}""");
        assertOK(performRequestAsUser("my_user", inNamespaceAlias));
    }

    public void testRolloverCannotCreateOutOfNamespaceIndex() throws IOException {
        createNamespaceScopedUser();
        createWriteAliasIndex("my-index-000001", "my-alias");

        // Denied: an explicit new index name outside the caller's namespace, even with no body aliases.
        // The new index is being created, so the denial comes from the create check, not the aliases check.
        final Request outOfNamespaceTarget = rolloverRequest("my-alias", "other-index-000002", null);
        final ResponseException e = expectThrows(ResponseException.class, () -> performRequestAsUser("my_user", outOfNamespaceTarget));
        assertEquals(403, e.getResponse().getStatusLine().getStatusCode());
        assertThat(e.getMessage(), containsString("action [indices:admin/create] is unauthorized"));
        // the denial names the offending new index, not the rollover target
        assertThat(e.getMessage(), containsString("on indices [other-index-000002]"));

        // Allowed: an explicit in-namespace new index name still works.
        final Request inNamespaceTarget = rolloverRequest("my-alias", "my-index-000002", null);
        assertOK(performRequestAsUser("my_user", inNamespaceTarget));
    }

    private void createNamespaceScopedUser() throws IOException {
        upsertRole("""
            {"indices":[{"names":["my-*"],"privileges":["manage"]}]}""", "my_role");
        createUser("my_user", PASSWORD, List.of("my_role"));
    }

    private void createWriteAliasIndex(String index, String alias) throws IOException {
        final Request request = new Request("PUT", "/" + index);
        request.setJsonEntity(Strings.format("""
            {"aliases":{"%s":{"is_write_index":true}}}""", alias));
        assertOK(adminClient().performRequest(request));
    }

    private static Request rolloverRequest(String rolloverTarget, String newIndexName, String body) {
        final String endpoint = "/" + rolloverTarget + "/_rollover" + (newIndexName == null ? "" : "/" + newIndexName);
        final Request request = new Request("POST", endpoint);
        if (body != null) {
            request.setJsonEntity(body);
        }
        return request;
    }

    private Response performRequestAsUser(String user, Request request) throws IOException {
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", basicAuthHeaderValue(user, PASSWORD)).build());
        return client().performRequest(request);
    }
}
