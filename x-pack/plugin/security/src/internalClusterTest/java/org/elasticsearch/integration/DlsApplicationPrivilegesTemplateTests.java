/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.integration;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.cache.request.RequestCacheStats;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.mustache.MustachePlugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.SecuritySingleNodeTestCase;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.action.privilege.PutPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.privilege.PutPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.role.PutRoleRequestBuilder;
import org.elasticsearch.xpack.core.security.action.user.PutUserRequestBuilder;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.test.SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.equalTo;

/**
 * Integration tests for the {@code _user.application_resources} DLS query template field.
 * <p>
 * Scenario (mirrors the PR description example): a shared "reader" role whose DLS query template
 * filters documents by the application resources (spaces) the authenticated user holds, so that
 * the same role definition enforces per-user document visibility without any per-request query
 * wrapper. Each user is assigned the shared reader role plus a per-space "resource role" that
 * grants the application privilege on a specific space resource; the DLS template renders each
 * user's resource list at query time.
 * <p>
 * A second test validates that the request cache is partitioned by user: different application
 * resources → different cache keys, so cached results are never shared across users.
 */
public class DlsApplicationPrivilegesTemplateTests extends SecuritySingleNodeTestCase {

    private static final String APP_NAME = "myapp";
    private static final String SPACE_ACCESS_PRIV = "space_access";
    private static final String DATA_INDEX = "app-docs";

    private static final String READER_ROLE = "app_reader";
    private static final String MARKETING_ROLE = "space_marketing";
    private static final String FINANCE_ROLE = "space_finance";

    private static final String ALICE = "alice";  // marketing only
    private static final String BOB = "bob";      // finance only
    private static final String CAROL = "carol";  // marketing + finance

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            .put(super.nodeSettings())
            .put(XPackSettings.DLS_FLS_ENABLED.getKey(), true)
            .put(XPackSettings.API_KEY_SERVICE_ENABLED_SETTING.getKey(), false)
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        final ArrayList<Class<? extends Plugin>> plugins = new ArrayList<>(super.getPlugins());
        plugins.add(MustachePlugin.class);
        return List.copyOf(plugins);
    }

    @Before
    public void setup() {
        // 1. Register the application privilege so the security layer recognises it.
        final PutPrivilegesRequest putPriv = new PutPrivilegesRequest();
        putPriv.setPrivileges(List.of(new ApplicationPrivilegeDescriptor(APP_NAME, SPACE_ACCESS_PRIV, Set.of("space:access"), emptyMap())));
        putPriv.setRefreshPolicy(IMMEDIATE);
        client().execute(PutPrivilegesAction.INSTANCE, putPriv).actionGet();

        // 2. Shared reader role: DLS template filters by _user.application_resources.
        // At render time {{#toJson}}_user.application_resources{{/toJson}} produces e.g.
        // ["space:marketing"] for Alice and ["space:finance"] for Bob.
        new PutRoleRequestBuilder(client()).name(READER_ROLE)
            .addIndices(new String[] { DATA_INDEX }, new String[] { "read" }, null, null, new BytesArray("""
                {"template":{"source":"{\\"terms\\":{\\"spaces\\":{{#toJson}}_user.application_resources{{/toJson}}}}"}}"""), false)
            .get();

        // 3. Per-space resource roles grant the application privilege on one space resource each.
        PutRoleRequestBuilder marketingRoleBuilder = new PutRoleRequestBuilder(client()).name(MARKETING_ROLE);
        marketingRoleBuilder.request()
            .addApplicationPrivileges(
                RoleDescriptor.ApplicationResourcePrivileges.builder()
                    .application(APP_NAME)
                    .privileges(SPACE_ACCESS_PRIV)
                    .resources("space:marketing")
                    .build()
            );
        marketingRoleBuilder.get();

        PutRoleRequestBuilder financeRoleBuilder = new PutRoleRequestBuilder(client()).name(FINANCE_ROLE);
        financeRoleBuilder.request()
            .addApplicationPrivileges(
                RoleDescriptor.ApplicationResourcePrivileges.builder()
                    .application(APP_NAME)
                    .privileges(SPACE_ACCESS_PRIV)
                    .resources("space:finance")
                    .build()
            );
        financeRoleBuilder.get();

        // 4. Create users.
        createUser(ALICE, READER_ROLE, MARKETING_ROLE);
        createUser(BOB, READER_ROLE, FINANCE_ROLE);
        createUser(CAROL, READER_ROLE, MARKETING_ROLE, FINANCE_ROLE);

        // 5. Index documents tagged with the spaces that may see them.
        assertAcked(indicesAdmin().prepareCreate(DATA_INDEX).setMapping("spaces", "type=keyword", "title", "type=keyword"));
        client().prepareIndex(DATA_INDEX)
            .setId("marketing")
            .setSource("title", "Marketing doc", "spaces", List.of("space:marketing"))
            .setRefreshPolicy(IMMEDIATE)
            .get();
        client().prepareIndex(DATA_INDEX)
            .setId("finance")
            .setSource("title", "Finance doc", "spaces", List.of("space:finance"))
            .setRefreshPolicy(IMMEDIATE)
            .get();
        client().prepareIndex(DATA_INDEX)
            .setId("shared")
            .setSource("title", "Shared doc", "spaces", List.of("space:marketing", "space:finance"))
            .setRefreshPolicy(IMMEDIATE)
            .get();

        ensureGreen(DATA_INDEX);
        // Force merge to prevent background merges from invalidating the request cache during tests.
        indicesAdmin().prepareForceMerge(DATA_INDEX).setFlush(true).get();
        indicesAdmin().prepareRefresh(DATA_INDEX).get();
    }

    /**
     * Each user sees exactly the documents tagged with their granted application resources.
     * Alice (marketing) → marketing + shared; Bob (finance) → finance + shared;
     * Carol (marketing + finance) → all three.
     */
    public void testDlsTemplateFiltersByApplicationResources() {
        assertDocIds(clientFor(ALICE), "marketing", "shared");
        assertDocIds(clientFor(BOB), "finance", "shared");
        assertDocIds(clientFor(CAROL), "marketing", "finance", "shared");
    }

    /**
     * The request cache must be partitioned by application resources so that a cached result for
     * Alice is never served to Bob, whose application resources (and therefore DLS query rendering)
     * differ.
     */
    public void testRequestCacheIsPartitionedByApplicationResources() {
        final Client alice = clientFor(ALICE);
        final Client bob = clientFor(BOB);

        // Alice searches — cache miss.
        assertDocIds(alice, "marketing", "shared");
        final long missesAfterAlice = cacheStats().getMissCount();

        // Bob searches — must produce a separate cache entry (different application resources
        // → different rendered DLS query → different cache key).
        assertDocIds(bob, "finance", "shared");
        assertThat(
            "Bob's search must be a cache miss, not served from Alice's entry",
            cacheStats().getMissCount(),
            equalTo(missesAfterAlice + 1)
        );

        // Alice searches again — must hit her own cache entry.
        assertDocIds(alice, "marketing", "shared");
        assertThat("Alice's repeat search must hit the cache", cacheStats().getHitCount(), equalTo(1L));
    }

    private void createUser(String username, String... roles) {
        new PutUserRequestBuilder(client()).username(username)
            .password(TEST_PASSWORD_SECURE_STRING, getFastStoredHashAlgoForTests())
            .roles(roles)
            .get();
    }

    private void assertDocIds(Client userClient, String... expectedIds) {
        final var response = userClient.prepareSearch(DATA_INDEX).setRequestCache(true).get();
        try {
            assertThat(response.getFailedShards(), equalTo(0));
            final Set<String> actualIds = Arrays.stream(response.getHits().getHits()).map(SearchHit::getId).collect(Collectors.toSet());
            assertThat(actualIds, equalTo(Set.of(expectedIds)));
        } finally {
            response.decRef();
        }
    }

    private Client clientFor(String username) {
        return client().filterWithHeader(Map.of("Authorization", basicAuthHeaderValue(username, TEST_PASSWORD_SECURE_STRING)));
    }

    private RequestCacheStats cacheStats() {
        return indicesAdmin().prepareStats(DATA_INDEX).setRequestCache(true).get().getTotal().getRequestCache();
    }
}
