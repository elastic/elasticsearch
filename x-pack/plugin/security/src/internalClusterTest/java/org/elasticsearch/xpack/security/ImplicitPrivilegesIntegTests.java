/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.GetFeatureUsageRequest;
import org.elasticsearch.license.GetFeatureUsageResponse;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.license.TransportGetFeatureUsageAction;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.xpack.core.security.SecurityExtension;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.privilege.PutPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.privilege.PutPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.role.PutRoleRequestBuilder;
import org.elasticsearch.xpack.core.security.action.user.GetUserPrivilegesRequestBuilder;
import org.elasticsearch.xpack.core.security.action.user.GetUserPrivilegesResponse;
import org.elasticsearch.xpack.core.security.action.user.PutUserRequestBuilder;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsDefinition;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ImplicitPrivilegesProvider;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.test.SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xpack.core.security.SecurityField.DOCUMENT_LEVEL_SECURITY_FEATURE;
import static org.elasticsearch.xpack.core.security.SecurityField.FIELD_LEVEL_SECURITY_FEATURE;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

/**
 * Integration tests covering the {@link ImplicitPrivilegesProvider} SPI extension point on
 * {@link SecurityExtension}.
 *
 * <p>The whole suite runs under a {@code basic} license. Implicit DLS/FLS is intentionally
 * exempt from the DLS/FLS license check, so exercising the SPI at the most restrictive tier
 * is the strongest demonstration that nothing in the resolution, search, or feature-tracking
 * paths secretly requires a higher license.
 */
public class ImplicitPrivilegesIntegTests extends SecurityIntegTestCase {

    private static final String SHIELD_APP = "shield";
    private static final String AGENT_PRIV = "agent";
    private static final String HELICARRIER_INDEX_PATTERN = "helicarrier-*";
    private static final String HELICARRIER_DLS_QUERY = "{\"term\":{\"clearance\":\"public\"}}";

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "basic")
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(LocalStateSecurity.class);
        plugins.add(LocalStateWithImplicitPrivileges.class);
        return List.copyOf(plugins);
    }

    @Override
    protected Class<?> xpackPluginClass() {
        return LocalStateWithImplicitPrivileges.class;
    }

    @Before
    public void registerApplicationPrivilege() {
        final var putPrivilegesRequest = new PutPrivilegesRequest();
        putPrivilegesRequest.setPrivileges(
            List.of(new ApplicationPrivilegeDescriptor(SHIELD_APP, AGENT_PRIV, Set.of("action:read"), emptyMap()))
        );
        client().execute(PutPrivilegesAction.INSTANCE, putPrivilegesRequest).actionGet();
    }

    public void testImplicitPrivilegesAppearInGetUserPrivilegesResponse() {
        createUserWithRole("fury", createRoleWithApplicationPrivilege("director"));

        final GetUserPrivilegesResponse response = getUserPrivileges("fury");

        final GetUserPrivilegesResponse.Indices implicitEntry = findImplicitIndexPrivilege(response);
        assertThat(implicitEntry.getPrivileges(), containsInAnyOrder("read"));
        assertThat(implicitEntry.getQueries().stream().map(BytesReference::utf8ToString).toList(), contains(HELICARRIER_DLS_QUERY));
        assertThat(
            implicitEntry.getFieldSecurity(),
            contains(new FieldPermissionsDefinition.FieldGrantExcludeGroup(new String[] { "clearance" }, null))
        );

        assertThat(
            response.getApplicationPrivileges().stream().map(RoleDescriptor.ApplicationResourcePrivileges::getApplication).toList(),
            hasItem(SHIELD_APP)
        );
    }

    public void testImplicitPrivilegesAreAbsentForUsersWithoutQualifyingApplicationPrivilege() {
        new PutRoleRequestBuilder(client()).name("villain").cluster("monitor").get();
        createUserWithRole("loki", "villain");

        final GetUserPrivilegesResponse response = getUserPrivileges("loki");

        final List<String> indexPatterns = response.getIndexPrivileges().stream().flatMap(idx -> idx.getIndices().stream()).toList();
        assertThat(indexPatterns, not(hasItem(HELICARRIER_INDEX_PATTERN)));
    }

    public void testImplicitPrivilegesGrantSearchAccessAndApplyDlsFls() {
        createUserWithRole("coulson", createRoleWithApplicationPrivilege("field_agent"));

        assertAcked(indicesAdmin().prepareCreate("helicarrier-1").setMapping("clearance", "type=keyword", "codename", "type=keyword"));
        assertAcked(indicesAdmin().prepareCreate("stark-tower-1").setMapping("clearance", "type=keyword"));
        prepareIndex("helicarrier-1").setId("1").setSource("clearance", "public", "codename", "fury").setRefreshPolicy(IMMEDIATE).get();
        prepareIndex("helicarrier-1").setId("2").setSource("clearance", "classified", "codename", "loki").setRefreshPolicy(IMMEDIATE).get();
        prepareIndex("stark-tower-1").setId("1").setSource("clearance", "public").setRefreshPolicy(IMMEDIATE).get();

        final Client userClient = clientFor("coulson");

        // DLS hides "classified" docs; FLS strips the "codename" field from the source.
        assertResponse(userClient.prepareSearch("helicarrier-1"), response -> {
            assertHitCount(response, 1);
            assertThat(response.getHits().getAt(0).getSourceAsMap(), is(Map.of("clearance", "public")));
        });

        // Indices not covered by the implicit grant are denied
        final ElasticsearchSecurityException e = expectThrows(
            ElasticsearchSecurityException.class,
            () -> userClient.prepareSearch("stark-tower-1").get()
        );
        assertThat(e.status(), is(RestStatus.FORBIDDEN));
    }

    public void testImplicitDlsAndFlsBypassLicenseEnforcement() {
        createUserWithRole("hill", createRoleWithApplicationPrivilege("deputy_director"));

        assertAcked(indicesAdmin().prepareCreate("helicarrier-omega").setMapping("clearance", "type=keyword"));
        prepareIndex("helicarrier-omega").setId("1").setSource("clearance", "public").setRefreshPolicy(IMMEDIATE).get();
        prepareIndex("helicarrier-omega").setId("2").setSource("clearance", "classified").setRefreshPolicy(IMMEDIATE).get();

        final Client userClient = clientFor("hill");
        assertHitCount(userClient.prepareSearch("helicarrier-omega"), 1);

        // Implicit grants must not contribute to DLS/FLS feature-usage tracking.
        assertThat(fetchTrackedFeatureNames(), not(hasItem(DOCUMENT_LEVEL_SECURITY_FEATURE.getName())));
        assertThat(fetchTrackedFeatureNames(), not(hasItem(FIELD_LEVEL_SECURITY_FEATURE.getName())));
    }

    /**
     * Exercises the {@code LimitedRole} composition path used by API keys: when both the owner role
     * and the API key role declare the qualifying application privilege, the implicit DLS/FLS
     * derived by the provider on each side composes via
     * with both sides reporting {@code isDlsFlsImplicit() == true}, so the AND preserves the flag
     * and the basic-license bypass holds end-to-end.
     */
    public void testApiKeyWithSameApplicationPrivilegePreservesImplicitGrant() throws Exception {
        createUserWithRole("romanoff", createRoleWithApplicationPrivilege("agent"));

        assertAcked(indicesAdmin().prepareCreate("helicarrier-bridge").setMapping("clearance", "type=keyword", "codename", "type=keyword"));
        prepareIndex("helicarrier-bridge").setId("1")
            .setSource("clearance", "public", "codename", "widow")
            .setRefreshPolicy(IMMEDIATE)
            .get();
        prepareIndex("helicarrier-bridge").setId("2")
            .setSource("clearance", "classified", "codename", "hawkeye")
            .setRefreshPolicy(IMMEDIATE)
            .get();

        // API key role mirrors the owner: only the qualifying application privilege, no explicit
        // index privileges. The provider yields the same implicit DLS+FLS on the key side.
        final RoleDescriptor apiKeyRole = new RoleDescriptor(
            "api-key-mirror",
            null,
            null,
            new RoleDescriptor.ApplicationResourcePrivileges[] {
                RoleDescriptor.ApplicationResourcePrivileges.builder()
                    .application(SHIELD_APP)
                    .privileges(AGENT_PRIV)
                    .resources("*")
                    .build() },
            null,
            null,
            null,
            null
        );
        final Client apiKeyClient = clientForApiKey(createApiKey("romanoff", List.of(apiKeyRole)));

        // DLS hides the "classified" doc and FLS strips "codename" — same observable behavior as
        // the owner would see directly, demonstrating the implicit flag survived composition.
        assertResponse(apiKeyClient.prepareSearch("helicarrier-bridge"), response -> {
            assertHitCount(response, 1);
            assertThat(response.getHits().getAt(0).getSourceAsMap(), is(Map.of("clearance", "public")));
        });

        // Implicit DLS/FLS via an API key must not register feature usage either.
        assertThat(fetchTrackedFeatureNames(), not(hasItem(DOCUMENT_LEVEL_SECURITY_FEATURE.getName())));
        assertThat(fetchTrackedFeatureNames(), not(hasItem(FIELD_LEVEL_SECURITY_FEATURE.getName())));
    }

    /**
     * Owner role grants raw {@code read} on {@link #HELICARRIER_INDEX_PATTERN} with no application
     * privilege, so the provider does not fire on the owner side. The API key declares only the
     * qualifying application privilege, so the provider attaches implicit DLS/FLS on the key side.
     * At auth time the two IACs compose: owner contributes no DLS/FLS (neutral), key contributes
     * implicit DLS/FLS. Under the current composition rule the flag is dropped, license enforcement
     * kicks in, and the basic-license bypass is lost — which this test is written to catch.
     */
    public void testApiKeyWithImplicitGrantAndOwnerWithRawAccessPreservesImplicitGrant() throws Exception {
        createUserWithRole("banner", createRoleWithRawReadOnHelicarrier("raw_reader"));

        assertAcked(indicesAdmin().prepareCreate("helicarrier-raw-owner").setMapping("clearance", "type=keyword", "codename", "type=keyword"));
        prepareIndex("helicarrier-raw-owner").setId("1")
            .setSource("clearance", "public", "codename", "hulk")
            .setRefreshPolicy(IMMEDIATE)
            .get();
        prepareIndex("helicarrier-raw-owner").setId("2")
            .setSource("clearance", "classified", "codename", "thanos")
            .setRefreshPolicy(IMMEDIATE)
            .get();

        final RoleDescriptor apiKeyRole = new RoleDescriptor(
            "api-key-app-priv-only",
            null,
            null,
            new RoleDescriptor.ApplicationResourcePrivileges[] {
                RoleDescriptor.ApplicationResourcePrivileges.builder()
                    .application(SHIELD_APP)
                    .privileges(AGENT_PRIV)
                    .resources("*")
                    .build() },
            null,
            null,
            null,
            null
        );
        final Client apiKeyClient = clientForApiKey(createApiKey("banner", List.of(apiKeyRole)));

        assertResponse(apiKeyClient.prepareSearch("helicarrier-raw-owner"), response -> {
            assertHitCount(response, 1);
            assertThat(response.getHits().getAt(0).getSourceAsMap(), is(Map.of("clearance", "public")));
        });

        assertThat(fetchTrackedFeatureNames(), not(hasItem(DOCUMENT_LEVEL_SECURITY_FEATURE.getName())));
        assertThat(fetchTrackedFeatureNames(), not(hasItem(FIELD_LEVEL_SECURITY_FEATURE.getName())));
    }

    /**
     * Mirror of {@link #testApiKeyWithImplicitGrantAndOwnerWithRawAccessPreservesImplicitGrant} with
     * the sides swapped: the owner holds the application privilege (implicit DLS/FLS from the
     * provider), while the API key declares raw {@code read} on the same index pattern. The composed
     * IAC has implicit DLS/FLS from the owner side and nothing from the key side — same "asymmetric"
     * shape, opposite direction.
     */
    public void testApiKeyWithRawAccessAndOwnerWithImplicitGrantPreservesImplicitGrant() throws Exception {
        createUserWithRole("rogers", createRoleWithApplicationPrivilege("captain"));

        assertAcked(indicesAdmin().prepareCreate("helicarrier-implicit-owner").setMapping("clearance", "type=keyword", "codename", "type=keyword"));
        prepareIndex("helicarrier-implicit-owner").setId("1")
            .setSource("clearance", "public", "codename", "cap")
            .setRefreshPolicy(IMMEDIATE)
            .get();
        prepareIndex("helicarrier-implicit-owner").setId("2")
            .setSource("clearance", "classified", "codename", "bucky")
            .setRefreshPolicy(IMMEDIATE)
            .get();

        final RoleDescriptor apiKeyRole = new RoleDescriptor(
            "api-key-raw-only",
            null,
            new RoleDescriptor.IndicesPrivileges[] {
                RoleDescriptor.IndicesPrivileges.builder().indices(HELICARRIER_INDEX_PATTERN).privileges("read").build() },
            null,
            null,
            null,
            null,
            null
        );
        final Client apiKeyClient = clientForApiKey(createApiKey("rogers", List.of(apiKeyRole)));

        assertResponse(apiKeyClient.prepareSearch("helicarrier-implicit-owner"), response -> {
            assertHitCount(response, 1);
            assertThat(response.getHits().getAt(0).getSourceAsMap(), is(Map.of("clearance", "public")));
        });

        assertThat(fetchTrackedFeatureNames(), not(hasItem(DOCUMENT_LEVEL_SECURITY_FEATURE.getName())));
        assertThat(fetchTrackedFeatureNames(), not(hasItem(FIELD_LEVEL_SECURITY_FEATURE.getName())));
    }

    private String createRoleWithApplicationPrivilege(String roleName) {
        final var putRole = new PutRoleRequestBuilder(client()).name(roleName).cluster("manage_own_api_key");
        putRole.request()
            .addApplicationPrivileges(
                RoleDescriptor.ApplicationResourcePrivileges.builder().application(SHIELD_APP).privileges(AGENT_PRIV).resources("*").build()
            );
        putRole.get();
        return roleName;
    }

    private String createRoleWithRawReadOnHelicarrier(String roleName) {
        new PutRoleRequestBuilder(client()).name(roleName)
            .cluster("manage_own_api_key")
            .addIndices(new String[] { HELICARRIER_INDEX_PATTERN }, new String[] { "read" }, null, null, null, false)
            .get();
        return roleName;
    }

    private void createUserWithRole(String username, String role) {
        new PutUserRequestBuilder(client()).username(username)
            .password(TEST_PASSWORD_SECURE_STRING, getFastStoredHashAlgoForTests())
            .roles(role)
            .get();
    }

    private GetUserPrivilegesResponse getUserPrivileges(String username) {
        return new GetUserPrivilegesRequestBuilder(clientFor(username)).username(username).get();
    }

    private Client clientFor(String username) {
        return client().filterWithHeader(Map.of("Authorization", basicAuthHeaderValue(username, TEST_PASSWORD_SECURE_STRING)));
    }

    private CreateApiKeyResponse createApiKey(String username, List<RoleDescriptor> roleDescriptors) throws Exception {
        final CreateApiKeyRequest request = new CreateApiKeyRequest(randomAlphaOfLengthBetween(4, 12), roleDescriptors, null);
        request.setRefreshPolicy(IMMEDIATE);
        return clientFor(username).execute(CreateApiKeyAction.INSTANCE, request).get();
    }

    private Client clientForApiKey(CreateApiKeyResponse apiKey) {
        final String credentials = apiKey.getId() + ":" + apiKey.getKey();
        final String header = "ApiKey " + Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
        return client().filterWithHeader(Map.of("Authorization", header));
    }

    private GetUserPrivilegesResponse.Indices findImplicitIndexPrivilege(GetUserPrivilegesResponse response) {
        return response.getIndexPrivileges()
            .stream()
            .filter(idx -> idx.getIndices().contains(HELICARRIER_INDEX_PATTERN))
            .findFirst()
            .orElseThrow(
                () -> new AssertionError(
                    "expected implicit index privilege for [" + HELICARRIER_INDEX_PATTERN + "], got " + response.getIndexPrivileges()
                )
            );
    }

    private Set<String> fetchTrackedFeatureNames() {
        final Set<String> features = new HashSet<>();
        for (String node : internalCluster().getNodeNames()) {
            final PlainActionFuture<GetFeatureUsageResponse> listener = new PlainActionFuture<>();
            client(node).execute(TransportGetFeatureUsageAction.TYPE, new GetFeatureUsageRequest(), listener);
            for (var feature : listener.actionGet().getFeatures()) {
                features.add(feature.getName());
            }
        }
        return features;
    }

    public static class LocalStateWithImplicitPrivileges extends LocalStateSecurity {

        public LocalStateWithImplicitPrivileges(Settings settings, Path configPath) throws Exception {
            super(settings, configPath);
        }

        @Override
        protected List<SecurityExtension> securityExtensions() {
            return List.of(new TestImplicitPrivilegesExtension());
        }
    }

    /**
     * A {@link SecurityExtension} that grants {@code read} on {@link #HELICARRIER_INDEX_PATTERN}
     * with an implicit DLS filter (only {@code clearance: public} docs) and an implicit FLS
     * grant (only the {@code clearance} field is exposed) for any role that holds the
     * {@code shield/agent} application privilege.
     */
    static class TestImplicitPrivilegesExtension implements SecurityExtension {

        @Override
        public String extensionName() {
            return "test-implicit-privileges-extension";
        }

        @Override
        public List<ImplicitPrivilegesProvider> getImplicitPrivilegesProviders(SecurityComponents components) {
            return List.of(new TestImplicitPrivilegesProvider());
        }
    }

    static class TestImplicitPrivilegesProvider implements ImplicitPrivilegesProvider {

        @Override
        public Collection<RoleDescriptor.IndicesPrivileges> getImplicitIndicesPrivileges(
            RoleDescriptor roleDescriptor,
            Collection<ApplicationPrivilegeDescriptor> storedApplicationPrivileges
        ) {
            final boolean hasQualifyingPrivilege = storedApplicationPrivileges.stream()
                .anyMatch(apd -> SHIELD_APP.equals(apd.getApplication()) && AGENT_PRIV.equals(apd.getName()));
            if (hasQualifyingPrivilege == false) {
                return List.of();
            }
            return List.of(
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices(HELICARRIER_INDEX_PATTERN)
                    .privileges("read")
                    .query(new BytesArray(HELICARRIER_DLS_QUERY))
                    .grantedFields("clearance")
                    .build()
            );
        }
    }
}
