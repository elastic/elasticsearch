/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.example.implicit;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.junit.ClassRule;

import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.example.implicit.HelicarrierImplicitPrivilegesProvider.AGENT_PRIV;
import static org.elasticsearch.example.implicit.HelicarrierImplicitPrivilegesProvider.HELICARRIER_INDEX_PATTERN;
import static org.elasticsearch.example.implicit.HelicarrierImplicitPrivilegesProvider.SHIELD_APP;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * End-to-end HTTP coverage for the {@link HelicarrierImplicitPrivilegesProvider} SPI extension
 * point. The test plugin in this module registers the provider via {@code SecurityExtension}
 * SPI so each Security REST API can be exercised against a real cluster, with implicit
 * privileges contributed by a real provider.
 */
public class ImplicitPrivilegesIT extends ESRestTestCase {

    private static final String ADMIN_USER = "test-admin";
    private static final String ADMIN_PASSWORD = "x-pack-test-password";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .name("implicit-privileges-extension-cluster")
        .plugin("implicit-privileges-extension-test")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.license.self_generated.type", "basic")
        .setting("xpack.ml.enabled", "false")
        .user(ADMIN_USER, ADMIN_PASSWORD)
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
                UsernamePasswordToken.basicAuthHeaderValue(ADMIN_USER, new SecureString(ADMIN_PASSWORD.toCharArray()))
            )
            .build();
    }

    public void testIncludeImplicitTrueAddsMarkerForRoleWithQualifyingApplicationPrivilege() throws Exception {
        putShieldAgentApplicationPrivilege();
        final String roleName = "director";
        putRoleWithApplicationPrivilege(roleName);

        // Default and explicit-false: marker must not be present anywhere.
        getRole(roleName, null).assertNoImplicitlyGrantedEntries();
        getRole(roleName, false).assertNoImplicitlyGrantedEntries();

        // include_implicit=true: provider contributes a single implicit entry on top of the
        // explicit one. The relative ordering of explicit vs implicit entries is not part of
        // the response contract, so partition by the marker rather than by position.
        final RoleResponse response = getRole(roleName, true);
        assertThat(response.indices(), hasSize(2));

        final IndicesEntry explicit = response.findExplicit();
        assertThat(explicit.names(), equalTo(List.of("logs-*")));
        assertThat(explicit.privileges(), equalTo(List.of("read")));

        final IndicesEntry implicit = response.findImplicit();
        assertThat(implicit.names(), equalTo(List.of(HELICARRIER_INDEX_PATTERN)));
        assertThat(implicit.privileges(), equalTo(List.of("read")));
        // DLS query and FLS grant survive xcontent rendering and reach the response unchanged.
        assertThat(implicit.query(), notNullValue());
        assertThat(implicit.query(), containsString("clearance"));
    }

    public void testIncludeImplicitTrueWithoutQualifyingApplicationPrivilegeLeavesResponseUnchanged() throws Exception {
        // No application privilege referenced, so the provider returns nothing. The response
        // shape must be identical to include_implicit=false: no extra entries, no marker.
        final String roleName = "ops-engineer";
        putRoleWithoutApplicationPrivilege(roleName);

        final RoleResponse response = getRole(roleName, true);
        assertThat(response.indices(), hasSize(1));
        assertThat(response.indices().get(0).implicitlyGranted(), is(false));
        assertThat(response.indices().get(0).names(), equalTo(List.of("logs-*")));
    }

    /**
     * The role holds an application privilege, but not the one the provider keys off of.
     * This exercises the full resolution path that the previous "no application privileges"
     * test short-circuits past.
     */
    public void testIncludeImplicitTrueWithNonQualifyingApplicationPrivilegeLeavesResponseUnchanged() throws Exception {
        // Action-based provider: "non-qualifying" means the privilege's actions do not accept AGENT_ACTION.
        // Use a disjoint action namespace ("data:write/*") so the resolved descriptor's actions cannot match.
        final String nonQualifyingPriv = "director";
        putShieldApplicationPrivilege(nonQualifyingPriv, "data:write/*");
        final String roleName = "field-agent";
        putRoleWithApplicationPrivilege(roleName, SHIELD_APP, nonQualifyingPriv);

        final RoleResponse response = getRole(roleName, true);
        assertThat(response.indices(), hasSize(1));
        response.assertNoImplicitlyGrantedEntries();
        assertThat(response.indices().get(0).names(), equalTo(List.of("logs-*")));
    }

    public void testPutRoleRejectsImplicitlyGrantedField() throws Exception {
        // PUT bodies must never contain the response-only marker. Without this rejection a client
        // could accidentally persist the implicit grant as if it had been explicitly configured.
        final Request put = new Request("PUT", "/_security/role/commander");
        put.setJsonEntity("""
            {
              "cluster": ["monitor"],
              "indices": [
                {
                  "names": ["logs-*"],
                  "privileges": ["read"],
                  "implicitly_granted": true
                }
              ]
            }
            """);
        final ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(put));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), is(400));
        assertThat(EntityUtils.toString(e.getResponse().getEntity()), containsString("unexpected field [implicitly_granted]"));
    }

    public void testWildcardApplicationPrivilegeWithConcretePrivilegeNameResolvesStoredDescriptor() throws Exception {
        putShieldAgentApplicationPrivilege();
        final String roleName = "wildcard-shield-agent";
        final Request put = new Request("PUT", "/_security/role/" + roleName);
        put.setJsonEntity("""
            {
              "cluster": ["monitor"],
              "indices": [
                { "names": ["logs-*"], "privileges": ["read"] }
              ],
              "applications": [
                { "application": "shield*", "privileges": ["agent"], "resources": ["*"] }
              ]
            }
            """);
        assertOK(client().performRequest(put));

        final RoleResponse response = getRole(roleName, true);
        assertThat(response.indices(), hasSize(2));

        final IndicesEntry explicit = response.findExplicit();
        assertThat(explicit.names(), equalTo(List.of("logs-*")));

        final IndicesEntry implicit = response.findImplicit();
        assertThat(implicit.names(), equalTo(List.of(HELICARRIER_INDEX_PATTERN)));
        assertThat(implicit.privileges(), equalTo(List.of("read")));
        assertThat(implicit.query(), notNullValue());
        assertThat(implicit.query(), containsString("clearance"));
    }

    public void testWildcardApplicationPrivilegeStillTriggersImplicitGrantViaRawRolePatterns() throws Exception {
        putShieldAgentApplicationPrivilege();
        final String roleName = "wildcard-app-role";
        final Request put = new Request("PUT", "/_security/role/" + roleName);
        put.setJsonEntity("""
            {
              "cluster": ["monitor"],
              "indices": [
                { "names": ["logs-*"], "privileges": ["read"] }
              ],
              "applications": [
                { "application": "*", "privileges": ["*"], "resources": ["*"] }
              ]
            }
            """);
        assertOK(client().performRequest(put));

        final RoleResponse response = getRole(roleName, true);
        assertThat(response.indices(), hasSize(2));

        final IndicesEntry explicit = response.findExplicit();
        assertThat(explicit.names(), equalTo(List.of("logs-*")));

        final IndicesEntry implicit = response.findImplicit();
        assertThat(implicit.names(), equalTo(List.of(HELICARRIER_INDEX_PATTERN)));
        assertThat(implicit.privileges(), equalTo(List.of("read")));
        assertThat(implicit.query(), notNullValue());
        assertThat(implicit.query(), containsString("clearance"));
    }

    private void putShieldAgentApplicationPrivilege() throws Exception {
        putShieldApplicationPrivilege(AGENT_PRIV);
    }

    private void putShieldApplicationPrivilege(String privilegeName) throws Exception {
        putShieldApplicationPrivilege(privilegeName, "data:read/*");
    }

    private void putShieldApplicationPrivilege(String privilegeName, String actionPattern) throws Exception {
        final Request request = new Request("PUT", "/_security/privilege");
        request.setJsonEntity(String.format(Locale.ROOT, """
            {
              "%s": {
                "%s": {
                  "actions": ["%s"]
                }
              }
            }
            """, SHIELD_APP, privilegeName, actionPattern));
        assertOK(client().performRequest(request));
    }

    private void putRoleWithApplicationPrivilege(String roleName) throws Exception {
        putRoleWithApplicationPrivilege(roleName, SHIELD_APP, AGENT_PRIV);
    }

    private void putRoleWithApplicationPrivilege(String roleName, String application, String privilege) throws Exception {
        final Request request = new Request("PUT", "/_security/role/" + roleName);
        request.setJsonEntity(String.format(Locale.ROOT, """
            {
              "cluster": ["monitor"],
              "indices": [
                { "names": ["logs-*"], "privileges": ["read"] }
              ],
              "applications": [
                { "application": "%s", "privileges": ["%s"], "resources": ["*"] }
              ]
            }
            """, application, privilege));
        assertOK(client().performRequest(request));
    }

    private void putRoleWithoutApplicationPrivilege(String roleName) throws Exception {
        final Request request = new Request("PUT", "/_security/role/" + roleName);
        request.setJsonEntity("""
            {
              "cluster": ["monitor"],
              "indices": [
                { "names": ["logs-*"], "privileges": ["read"] }
              ]
            }
            """);
        assertOK(client().performRequest(request));
    }

    private RoleResponse getRole(String roleName, Boolean includeImplicit) throws Exception {
        final Request request = new Request("GET", "/_security/role/" + roleName);
        if (includeImplicit != null) {
            request.addParameter("include_implicit", Boolean.toString(includeImplicit));
        }
        final Response response = client().performRequest(request);
        assertOK(response);
        return RoleResponse.from(entityAsMap(response), roleName);
    }

    /**
     * Typed view over a single role's {@code indices} list in a {@code GET _security/role} response.
     */
    private record RoleResponse(List<IndicesEntry> indices) {

        @SuppressWarnings("unchecked")
        static RoleResponse from(Map<String, Object> body, String roleName) {
            final Map<String, Object> role = (Map<String, Object>) body.get(roleName);
            final List<Map<String, Object>> raw = (List<Map<String, Object>>) role.get("indices");
            return new RoleResponse(raw.stream().map(IndicesEntry::from).toList());
        }

        IndicesEntry findExplicit() {
            return indices.stream()
                .filter(e -> !e.implicitlyGranted())
                .findFirst()
                .orElseThrow(() -> new AssertionError("expected an entry without implicitly_granted, got " + indices));
        }

        IndicesEntry findImplicit() {
            return indices.stream()
                .filter(IndicesEntry::implicitlyGranted)
                .findFirst()
                .orElseThrow(() -> new AssertionError("expected an entry with implicitly_granted=true, got " + indices));
        }

        void assertNoImplicitlyGrantedEntries() {
            for (IndicesEntry entry : indices) {
                assertThat("entry " + entry + " must not be marked implicit", entry.implicitlyGranted(), is(false));
                assertThat("raw entry must not contain the marker key", entry.raw().get("implicitly_granted"), nullValue());
            }
        }
    }

    /**
     * Typed view of one entry in the {@code "indices"} array. Captures only the fields the
     * tests assert on; the original map is retained for diagnostics.
     */
    private record IndicesEntry(
        List<String> names,
        List<String> privileges,
        String query,
        boolean implicitlyGranted,
        Map<String, Object> raw
    ) {

        @SuppressWarnings("unchecked")
        static IndicesEntry from(Map<String, Object> raw) {
            return new IndicesEntry(
                (List<String>) raw.get("names"),
                (List<String>) raw.get("privileges"),
                (String) raw.get("query"),
                Boolean.TRUE.equals(raw.get("implicitly_granted")),
                raw
            );
        }
    }
}
