/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.namedcredentials;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;

/**
 * End-to-end integration tests for the named credentials store. Exercises all five REST endpoints
 * against a real Elasticsearch cluster with security and encryption enabled.
 *
 * <p>The cluster uses PEK keystore entries so that the encryption service is fully initialised.
 * The first PUT in each test waits up to 60 s for the PEK to be generated (the coordinator fires
 * every 1 s, but cluster start-up may race it).
 *
 * <p>{@link #preserveClusterUponCompletion()} returns {@code true} for the same reason as
 * {@code EncryptedDataHandlerProviderSpiIT}: the {@code KeyRotationCoordinator} submits a
 * cluster-state task roughly every 1 s, and {@code ESRestTestCase#waitForClusterStateUpdatesToFinish}
 * cannot consistently catch the short quiet windows between tasks, causing spurious teardown
 * failures. Tests here create credentials/roles/users but the cluster is ephemeral, so skipping
 * the index-wipe is safe.
 */
public class NamedCredentialsRestIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .name("named-credentials-cluster")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.named_credentials.enabled", "true")
        .setting("xpack.encryption.key_rotation.check_interval", "1s")
        .keystore("cluster.state.encryption.active_password_id", "v1")
        .keystore("cluster.state.encryption.password.v1", "named-credentials-test-password")
        .user("test-admin", "x-pack-test-password")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        return Settings.builder()
            .put(
                ThreadContext.PREFIX + ".Authorization",
                basicAuthHeaderValue("test-admin", new SecureString("x-pack-test-password".toCharArray()))
            )
            .build();
    }

    /**
     * The KeyRotationCoordinator submits a begin-project-encryption-key-rotation cluster-state task every ~1 s while the cluster is
     * alive. ESRestTestCase#waitForClusterStateUpdatesToFinish uses assertBusy with exponential-backoff polling that consistently misses
     * the ~200 ms clean windows between successive tasks, causing spurious teardown failures. The cluster is ephemeral, so skipping the
     * wipe is safe.
     */
    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    private static Request putRequest(String name, String body) {
        Request request = new Request("PUT", "/_security/named_credentials/" + name);
        request.setJsonEntity(body);
        return request;
    }

    private static Request patchRequest(String name, String body) {
        Request request = new Request("PATCH", "/_security/named_credentials/" + name);
        request.setJsonEntity(body);
        return request;
    }

    /**
     * The first PUT in each test may race PEK generation (503 until the key exists); retries until the key is available.
     * Returns the parsed response body so callers can assert fields like {@code created}.
     */
    private org.elasticsearch.test.rest.ObjectPath putCredentialWaitingForKey(String name, String body) throws Exception {
        final Response[] holder = new Response[1];
        assertBusy(() -> {
            var response = client().performRequest(putRequest(name, body));
            assertOK(response);
            holder[0] = response;
        }, 60, TimeUnit.SECONDS);
        return assertOKAndCreateObjectPath(holder[0]);
    }

    public void testCrudLifecycle() throws Exception {
        var created = putCredentialWaitingForKey("it-servicenow", """
            {
              "auth_type": "oauth_client_credentials",
              "url": "https://instance.service-now.com",
              "config": { "token_url": "https://instance.service-now.com/oauth_token.do", "scope": "read write" },
              "auth": { "client_id": "my-client-id", "client_secret": "super-secret" }
            }""");
        assertThat(created.evaluate("created"), equalTo(true));

        // Update via PUT (full-replace) returns created=false
        var updated = assertOKAndCreateObjectPath(client().performRequest(putRequest("it-servicenow", """
            {
              "auth_type": "oauth_client_credentials",
              "url": "https://instance.service-now.com/v2",
              "auth": { "client_id": "my-client-id", "client_secret": "super-secret" }
            }""")));
        assertThat(updated.evaluate("created"), equalTo(false));

        // GET redacts
        var get = assertOKAndCreateObjectPath(client().performRequest(new Request("GET", "/_security/named_credentials/it-servicenow")));
        assertThat(get.evaluate("name"), equalTo("it-servicenow"));
        assertThat(get.evaluate("auth_type"), equalTo("oauth_client_credentials"));
        assertThat(get.evaluate("auth"), equalTo("::es_redacted::"));

        // LIST redacts
        var list = assertOKAndCreateObjectPath(client().performRequest(new Request("GET", "/_security/named_credentials")));
        assertThat(list.evaluate("named_credentials.0.auth"), equalTo("::es_redacted::"));

        // _decrypt returns plaintext
        var decrypted = assertOKAndCreateObjectPath(
            client().performRequest(new Request("GET", "/_security/named_credentials/it-servicenow/_decrypt"))
        );
        assertThat(decrypted.evaluate("auth.client_id"), equalTo("my-client-id"));
        assertThat(decrypted.evaluate("auth.client_secret"), equalTo("super-secret"));

        // DELETE, then GET is 404
        assertOK(client().performRequest(new Request("DELETE", "/_security/named_credentials/it-servicenow")));
        var e = expectThrows(
            ResponseException.class,
            () -> client().performRequest(new Request("GET", "/_security/named_credentials/it-servicenow"))
        );
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(404));
    }

    public void testPatchCarryForward() throws Exception {
        putCredentialWaitingForKey("it-patch", """
            { "auth_type": "basic", "url": "https://one.example.com", "auth": { "username": "u1", "password": "p1" } }""");

        // PATCH to update only url — auth is preserved
        assertOK(client().performRequest(patchRequest("it-patch", """
            { "url": "https://two.example.com" }""")));

        var decrypted = assertOKAndCreateObjectPath(
            client().performRequest(new Request("GET", "/_security/named_credentials/it-patch/_decrypt"))
        );
        assertThat(decrypted.evaluate("url"), equalTo("https://two.example.com"));
        assertThat(decrypted.evaluate("auth.username"), equalTo("u1"));
        assertThat(decrypted.evaluate("auth.password"), equalTo("p1"));

        // PATCH to update only auth — url is preserved
        assertOK(client().performRequest(patchRequest("it-patch", """
            { "auth": { "username": "u2", "password": "p2" } }""")));
        decrypted = assertOKAndCreateObjectPath(
            client().performRequest(new Request("GET", "/_security/named_credentials/it-patch/_decrypt"))
        );
        assertThat(decrypted.evaluate("url"), equalTo("https://two.example.com"));
        assertThat(decrypted.evaluate("auth.username"), equalTo("u2"));
        assertThat(decrypted.evaluate("auth.password"), equalTo("p2"));
    }

    public void testValidationErrors() throws Exception {
        // Unknown auth_type
        var e = expectThrows(ResponseException.class, () -> client().performRequest(putRequest("it-bad1", """
            { "auth_type": "carrier_pigeon", "auth": { "x": "y" } }""")));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));

        // Unknown config field
        e = expectThrows(ResponseException.class, () -> client().performRequest(putRequest("it-bad2", """
            { "auth_type": "basic", "config": { "bogus": "x" }, "auth": { "username": "u", "password": "p" } }""")));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));

        // Missing auth on PUT (auth required) — must wait for the key first via a successful control PUT
        putCredentialWaitingForKey("it-control", """
            { "auth_type": "basic", "auth": { "username": "u", "password": "p" } }""");
        e = expectThrows(ResponseException.class, () -> client().performRequest(putRequest("it-bad3", """
            { "auth_type": "basic" }""")));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));

        // Bad name
        e = expectThrows(ResponseException.class, () -> client().performRequest(putRequest("Uppercase-Name", """
            { "auth_type": "basic", "auth": { "username": "u", "password": "p" } }""")));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));

        // PATCH on non-existent credential returns 404
        e = expectThrows(ResponseException.class, () -> client().performRequest(patchRequest("it-does-not-exist", """
            { "url": "https://new.example.com" }""")));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(404));

        // PATCH with empty body (no fields) returns 400
        e = expectThrows(ResponseException.class, () -> client().performRequest(patchRequest("it-control", "{}")));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
    }

    public void testPrivilegeEnforcement() throws Exception {
        putCredentialWaitingForKey("it-privs", """
            { "auth_type": "bearer", "auth": { "token": "secret-token" } }""");

        // Role with read_named_credentials only
        Request putRole = new Request("PUT", "/_security/role/nc_reader");
        putRole.setJsonEntity("""
            { "cluster": ["read_named_credentials"] }""");
        assertOK(client().performRequest(putRole));
        Request putUser = new Request("PUT", "/_security/user/nc_reader_user");
        putUser.setJsonEntity("""
            { "password": "nc-reader-password", "roles": ["nc_reader"] }""");
        assertOK(client().performRequest(putUser));

        RequestOptions readerAuth = RequestOptions.DEFAULT.toBuilder()
            .addHeader("Authorization", basicAuthHeaderValue("nc_reader_user", new SecureString("nc-reader-password".toCharArray())))
            .build();

        // Reader can GET (redacted)
        Request readerGet = new Request("GET", "/_security/named_credentials/it-privs");
        readerGet.setOptions(readerAuth);
        var got = assertOKAndCreateObjectPath(client().performRequest(readerGet));
        assertThat(got.evaluate("auth"), equalTo("::es_redacted::"));

        // Reader cannot decrypt
        Request readerDecrypt = new Request("GET", "/_security/named_credentials/it-privs/_decrypt");
        readerDecrypt.setOptions(readerAuth);
        var e = expectThrows(ResponseException.class, () -> client().performRequest(readerDecrypt));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(403));

        // Reader cannot PUT
        Request readerPut = putRequest("it-privs2", """
            { "auth_type": "bearer", "auth": { "token": "t" } }""");
        readerPut.setOptions(readerAuth);
        e = expectThrows(ResponseException.class, () -> client().performRequest(readerPut));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(403));

        // Reader cannot DELETE
        Request readerDelete = new Request("DELETE", "/_security/named_credentials/it-privs");
        readerDelete.setOptions(readerAuth);
        e = expectThrows(ResponseException.class, () -> client().performRequest(readerDelete));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(403));
    }
}
