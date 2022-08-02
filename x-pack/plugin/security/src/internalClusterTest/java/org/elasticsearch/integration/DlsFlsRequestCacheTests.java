/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.integration;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.cache.request.RequestCacheStats;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.mustache.MustachePlugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.SecuritySingleNodeTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyResponse;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.elasticsearch.test.SecuritySettingsSource.TEST_PASSWORD_HASHED;
import static org.elasticsearch.test.SecuritySettingsSourceField.TEST_PASSWORD;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.equalTo;

public class DlsFlsRequestCacheTests extends SecuritySingleNodeTestCase {

    private static final String DLS_FLS_USER = "dls_fls_user";
    private static final String DLS_INDEX = "dls-index";
    private static final String DLS_ALIAS = "dls-alias";
    private static final String FLS_INDEX = "fls-index";
    private static final String FLS_ALIAS = "fls-alias";
    private static final String INDEX = "index";
    private static final String ALIAS1 = "alias1";
    private static final String ALIAS2 = "alias2";
    private static final String ALL_ALIAS = "all-alias";
    private static final String DLS_TEMPLATE_ROLE_QUERY_USER_1 = "dls_template_role_query_user_1";
    private static final String DLS_TEMPLATE_ROLE_QUERY_USER_2 = "dls_template_role_query_user_2";
    private static final String DLS_TEMPLATE_ROLE_QUERY_ROLE = "dls_template_role_query_role";
    private static final String DLS_TEMPLATE_ROLE_QUERY_INDEX = "dls-template-role-query-index";
    private static final String DLS_TEMPLATE_ROLE_QUERY_ALIAS = "dls-template-role-query-alias";

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(super.nodeSettings()).put(XPackSettings.API_KEY_SERVICE_ENABLED_SETTING.getKey(), true).build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        final ArrayList<Class<? extends Plugin>> plugins = new ArrayList<>(super.getPlugins());
        plugins.add(MustachePlugin.class);
        return List.copyOf(plugins);
    }

    @Override
    protected String configUsers() {
        return super.configUsers()
            + DLS_FLS_USER
            + ":"
            + TEST_PASSWORD_HASHED
            + "\n"
            + DLS_TEMPLATE_ROLE_QUERY_USER_2
            + ":"
            + TEST_PASSWORD_HASHED
            + "\n"
            + DLS_TEMPLATE_ROLE_QUERY_USER_1
            + ":"
            + TEST_PASSWORD_HASHED
            + "\n";
    }

    @Override
    protected String configRoles() {
        return """
            %s%s:
              cluster: [ "manage_own_api_key" ]
              indices:
              - names:
                - "dls-index"
                privileges:
                - "read"
                query: "{\\"match\\": {\\"number\\": 101}}"
              - names:
                - "dls-alias"
                privileges:
                - "read"
                query: "{\\"match\\": {\\"number\\": 102}}"
              - names:
                - "fls-index"
                privileges:
                - "read"
                field_security:
                  grant:
                  - "public"
              - names:
                - "fls-alias"
                privileges:
                - "read"
                field_security:
                  grant:
                  - "private"
              - names:
                - "alias1"
                privileges:
                - "read"
                query: "{\\"match\\": {\\"number\\": 1}}"
                field_security:
                  grant:
                  - "*"
                  except:
                  - "private"
              - names:
                - "alias2"
                privileges:
                - "read"
                query: "{\\"match\\": {\\"number\\": 2}}"
                field_security:
                  grant:
                  - "*"
                  except:
                  - "public"
              - names:
                - "all-alias"
                privileges:
                - "read"
            %s:
              indices:
              - names:
                - "dls-template-role-query-index"
                privileges:
                - "read"
                query: {"template":{"source":{"match":{"username":"{{_user.username}}"}}}}
              - names:
                - "dls-template-role-query-alias"
                privileges:
                - "read"
                query: {"template":{"id":"my-script"}}
            """.formatted(super.configRoles(), DLS_FLS_USER, DLS_TEMPLATE_ROLE_QUERY_ROLE);
    }

    @Override
    protected String configUsersRoles() {
        return super.configUsersRoles()
            + DLS_FLS_USER
            + ":"
            + DLS_FLS_USER
            + "\n"
            + DLS_TEMPLATE_ROLE_QUERY_ROLE
            + ":"
            + DLS_TEMPLATE_ROLE_QUERY_USER_1
            + ","
            + DLS_TEMPLATE_ROLE_QUERY_USER_2
            + "\n";
    }

    @Before
    public void init() {
        prepareIndices();
    }

    public void testRequestCacheForDLS() {
        final Client powerClient = client();
        final Client limitedClient = limitedClient();

        // Search first with power client, it should see all docs
        assertSearchResponse(powerClient.prepareSearch(DLS_INDEX).setRequestCache(true).get(), Set.of("101", "102"));
        assertCacheState(DLS_INDEX, 0, 1);

        // Search with the limited client and it should see only one doc (i.e. it won't use cache entry for power client)
        assertSearchResponse(limitedClient.prepareSearch(DLS_INDEX).setRequestCache(true).get(), Set.of("101"));
        assertCacheState(DLS_INDEX, 0, 2);

        // Execute the above search again and it should use the cache entry for limited client
        assertSearchResponse(limitedClient.prepareSearch(DLS_INDEX).setRequestCache(true).get(), Set.of("101"));
        assertCacheState(DLS_INDEX, 1, 2);

        // Execute the search with power client again and it should still see all docs
        assertSearchResponse(powerClient.prepareSearch(DLS_INDEX).setRequestCache(true).get(), Set.of("101", "102"));
        assertCacheState(DLS_INDEX, 2, 2);

        // The limited client has a different DLS query for dls-alias compared to the underlying dls-index
        assertSearchResponse(limitedClient.prepareSearch(DLS_ALIAS).setRequestCache(true).get(), Set.of("102"));
        assertCacheState(DLS_INDEX, 2, 3);
        assertSearchResponse(limitedClient.prepareSearch(DLS_ALIAS).setRequestCache(true).get(), Set.of("102"));
        assertCacheState(DLS_INDEX, 3, 3);

        // Search with limited client for dls-alias and dls-index returns all docs. The cache entry is however different
        // from the power client, i.e. still no sharing even if the end results are the same. This is because the
        // search with limited client still have DLS queries attached to it.
        assertSearchResponse(limitedClient.prepareSearch(DLS_ALIAS, DLS_INDEX).setRequestCache(true).get(), Set.of("101", "102"));
        assertCacheState(DLS_INDEX, 3, 4);
    }

    public void testRequestCacheForFLS() {
        final Client powerClient = client();
        final Client limitedClient = limitedClient();

        // Search first with power client, it should see all fields
        assertSearchResponse(
            powerClient.prepareSearch(FLS_INDEX).setRequestCache(true).get(),
            Set.of("201", "202"),
            Set.of("public", "private")
        );
        assertCacheState(FLS_INDEX, 0, 1);

        // Search with limited client and it should see only public field
        assertSearchResponse(limitedClient.prepareSearch(FLS_INDEX).setRequestCache(true).get(), Set.of("201", "202"), Set.of("public"));
        assertCacheState(FLS_INDEX, 0, 2);

        // Search with limited client again and it should use the cache
        assertSearchResponse(limitedClient.prepareSearch(FLS_INDEX).setRequestCache(true).get(), Set.of("201", "202"), Set.of("public"));
        assertCacheState(FLS_INDEX, 1, 2);

        // Search again with power client, it should use its own cache entry
        assertSearchResponse(
            powerClient.prepareSearch(FLS_INDEX).setRequestCache(true).get(),
            Set.of("201", "202"),
            Set.of("public", "private")
        );
        assertCacheState(FLS_INDEX, 2, 2);

        // The fls-alias has a different FLS definition compared to its underlying fls-index.
        assertSearchResponse(limitedClient.prepareSearch(FLS_ALIAS).setRequestCache(true).get(), Set.of("201", "202"), Set.of("private"));
        assertCacheState(FLS_INDEX, 2, 3);

        // Search with the limited client for both fls-alias and fls-index and all docs and fields are also returned.
        // But request cache is not shared with the power client because it still has a different indexAccessControl
        assertSearchResponse(
            limitedClient.prepareSearch(FLS_ALIAS, FLS_INDEX).setRequestCache(true).get(),
            Set.of("201", "202"),
            Set.of("public", "private")
        );
        assertCacheState(FLS_INDEX, 2, 4);
    }

    public void testRequestCacheForBothDLSandFLS() throws ExecutionException, InterruptedException {
        final Client powerClient = client();
        final Client limitedClient = limitedClient();

        // Search first with power client, it should see all fields
        assertSearchResponse(
            powerClient.prepareSearch(INDEX).setRequestCache(true).get(),
            Set.of("1", "2"),
            Set.of("number", "letter", "public", "private")
        );
        assertCacheState(INDEX, 0, 1);

        // The limited client does not have access to the underlying index
        // Search with the limited client results in error
        expectThrows(ElasticsearchSecurityException.class, () -> limitedClient.prepareSearch(INDEX).setRequestCache(true).get());

        // Search for alias1 that points to index and has DLS/FLS
        assertSearchResponse(
            limitedClient.prepareSearch(ALIAS1).setRequestCache(true).get(),
            Set.of("1"),
            Set.of("number", "letter", "public")
        );
        assertCacheState(INDEX, 0, 2);

        // Search for alias2 that also points to index but has a different set of DLS/FLS
        assertSearchResponse(
            limitedClient.prepareSearch(ALIAS2).setRequestCache(true).get(),
            Set.of("2"),
            Set.of("number", "letter", "private")
        );
        assertCacheState(INDEX, 0, 3);

        // Search for all-alias that has full read access to the underlying index
        // This makes it share the cache entry of the power client
        assertSearchResponse(
            limitedClient.prepareSearch(ALL_ALIAS).setRequestCache(true).get(),
            Set.of("1", "2"),
            Set.of("number", "letter", "public", "private")
        );
        assertCacheState(INDEX, 1, 3);

        // Similarly, search for alias1 and all-alias results in full read access to the index
        // and again reuse the cache entry of the power client
        assertSearchResponse(
            limitedClient.prepareSearch(ALIAS1, ALL_ALIAS).setRequestCache(true).get(),
            Set.of("1", "2"),
            Set.of("number", "letter", "public", "private")
        );
        assertCacheState(INDEX, 2, 3);

        // Though search for both alias1 and alias2 is effectively full read access to index,
        // it does not share the cache entry of the power client because role queries still exist.
        assertSearchResponse(
            limitedClient.prepareSearch(ALIAS1, ALIAS2).setRequestCache(true).get(),
            Set.of("1", "2"),
            Set.of("number", "letter", "public", "private")
        );
        assertCacheState(INDEX, 2, 4);

        // Test with an API Key that has different DLS/FLS on all-alias
        final Client limitedClientApiKey = limitedClientApiKey();

        // It should not reuse any entries from the cache
        assertSearchResponse(
            limitedClientApiKey.prepareSearch(ALL_ALIAS).setRequestCache(true).get(),
            Set.of("1"),
            Set.of("letter", "public", "private")
        );
        assertCacheState(INDEX, 2, 5);
    }

    public void testRequestCacheWithTemplateRoleQuery() {
        final Client client1 = client().filterWithHeader(
            Map.of("Authorization", basicAuthHeaderValue(DLS_TEMPLATE_ROLE_QUERY_USER_1, new SecureString(TEST_PASSWORD.toCharArray())))
        );
        final Client client2 = client().filterWithHeader(
            Map.of("Authorization", basicAuthHeaderValue(DLS_TEMPLATE_ROLE_QUERY_USER_2, new SecureString(TEST_PASSWORD.toCharArray())))
        );

        // Search first with user1 and only one document will be return with the corresponding username
        assertSearchResponse(
            client1.prepareSearch(DLS_TEMPLATE_ROLE_QUERY_INDEX).setRequestCache(true).get(),
            Set.of("1"),
            Set.of("username")
        );
        assertCacheState(DLS_TEMPLATE_ROLE_QUERY_INDEX, 0, 1);

        // Search with user2 will not use user1's cache because template query is resolved differently for them
        assertSearchResponse(
            client2.prepareSearch(DLS_TEMPLATE_ROLE_QUERY_INDEX).setRequestCache(true).get(),
            Set.of("2"),
            Set.of("username")
        );
        assertCacheState(DLS_TEMPLATE_ROLE_QUERY_INDEX, 0, 2);

        // Search with user1 again will use user1's cache
        assertSearchResponse(
            client1.prepareSearch(DLS_TEMPLATE_ROLE_QUERY_INDEX).setRequestCache(true).get(),
            Set.of("1"),
            Set.of("username")
        );
        assertCacheState(DLS_TEMPLATE_ROLE_QUERY_INDEX, 1, 2);

        // Search with user2 again will use user2's cache
        assertSearchResponse(
            client2.prepareSearch(DLS_TEMPLATE_ROLE_QUERY_INDEX).setRequestCache(true).get(),
            Set.of("2"),
            Set.of("username")
        );
        assertCacheState(DLS_TEMPLATE_ROLE_QUERY_INDEX, 2, 2);

        // Since the DLS for the alias uses a stored script, this should cause the request cached to be disabled
        assertSearchResponse(
            client1.prepareSearch(DLS_TEMPLATE_ROLE_QUERY_ALIAS).setRequestCache(true).get(),
            Set.of("1"),
            Set.of("username")
        );
        // No cache should be used
        assertCacheState(DLS_TEMPLATE_ROLE_QUERY_INDEX, 2, 2);
    }

    private void prepareIndices() {
        final Client client = client();

        assertAcked(
            client.admin()
                .cluster()
                .preparePutStoredScript()
                .setId("my-script")
                .setContent(
                    new BytesArray("""
                        {"script":{"source":"{\\"match\\":{\\"username\\":\\"{{_user.username}}\\"}}","lang":"mustache"}}"""),
                    XContentType.JSON
                )
                .get()
        );

        assertAcked(client.admin().indices().prepareCreate(DLS_INDEX).addAlias(new Alias("dls-alias")).get());
        client.prepareIndex(DLS_INDEX).setId("101").setSource("number", 101, "letter", "A").get();
        client.prepareIndex(DLS_INDEX).setId("102").setSource("number", 102, "letter", "B").get();

        assertAcked(client.admin().indices().prepareCreate(FLS_INDEX).addAlias(new Alias("fls-alias")).get());
        client.prepareIndex(FLS_INDEX).setId("201").setSource("public", "X", "private", "x").get();
        client.prepareIndex(FLS_INDEX).setId("202").setSource("public", "Y", "private", "y").get();

        assertAcked(
            client.admin()
                .indices()
                .prepareCreate(INDEX)
                .addAlias(new Alias(ALIAS1))
                .addAlias(new Alias(ALIAS2))
                .addAlias(new Alias(ALL_ALIAS))
                .get()
        );
        client.prepareIndex(INDEX).setId("1").setSource("number", 1, "letter", "a", "private", "sesame_1", "public", "door_1").get();
        client.prepareIndex(INDEX).setId("2").setSource("number", 2, "letter", "b", "private", "sesame_2", "public", "door_2").get();

        assertAcked(
            client.admin().indices().prepareCreate(DLS_TEMPLATE_ROLE_QUERY_INDEX).addAlias(new Alias(DLS_TEMPLATE_ROLE_QUERY_ALIAS)).get()
        );
        client.prepareIndex(DLS_TEMPLATE_ROLE_QUERY_INDEX).setId("1").setSource("username", DLS_TEMPLATE_ROLE_QUERY_USER_1).get();
        client.prepareIndex(DLS_TEMPLATE_ROLE_QUERY_INDEX).setId("2").setSource("username", DLS_TEMPLATE_ROLE_QUERY_USER_2).get();

        ensureGreen(DLS_INDEX, FLS_INDEX, INDEX, DLS_TEMPLATE_ROLE_QUERY_INDEX);
        assertCacheState(DLS_INDEX, 0, 0);
        assertCacheState(FLS_INDEX, 0, 0);
        assertCacheState(INDEX, 0, 0);
        assertCacheState(DLS_TEMPLATE_ROLE_QUERY_INDEX, 0, 0);

        // Force merge the index to ensure there can be no background merges during the subsequent searches that would invalidate the cache
        final ForceMergeResponse forceMergeResponse = client.admin()
            .indices()
            .prepareForceMerge(DLS_INDEX, FLS_INDEX, INDEX, DLS_TEMPLATE_ROLE_QUERY_INDEX)
            .setFlush(true)
            .get();
        ElasticsearchAssertions.assertAllSuccessful(forceMergeResponse);
        final RefreshResponse refreshResponse = client.admin()
            .indices()
            .prepareRefresh(DLS_INDEX, FLS_INDEX, INDEX, DLS_TEMPLATE_ROLE_QUERY_INDEX)
            .get();
        assertThat(refreshResponse.getFailedShards(), equalTo(0));
        ensureGreen(DLS_INDEX, FLS_INDEX, INDEX, DLS_TEMPLATE_ROLE_QUERY_INDEX);
    }

    private Client limitedClient() {
        return client().filterWithHeader(
            Map.of("Authorization", basicAuthHeaderValue(DLS_FLS_USER, new SecureString(TEST_PASSWORD.toCharArray())))
        );
    }

    private Client limitedClientApiKey() throws ExecutionException, InterruptedException {
        final CreateApiKeyRequest createApiKeyRequest = new CreateApiKeyRequest(
            randomAlphaOfLengthBetween(3, 8),
            List.of(
                new RoleDescriptor(
                    randomAlphaOfLengthBetween(3, 8),
                    null,
                    new RoleDescriptor.IndicesPrivileges[] {
                        RoleDescriptor.IndicesPrivileges.builder().indices(ALL_ALIAS).privileges("read").query("""
                            {"term":{"letter":"a"}}""").grantedFields("*").deniedFields("number").build() },
                    null
                )
            ),
            null
        );
        final CreateApiKeyResponse createApiKeyResponse = limitedClient().execute(CreateApiKeyAction.INSTANCE, createApiKeyRequest).get();

        final String base64ApiKey = Base64.getEncoder()
            .encodeToString((createApiKeyResponse.getId() + ":" + createApiKeyResponse.getKey()).getBytes(StandardCharsets.UTF_8));
        return client().filterWithHeader(Map.of("Authorization", "ApiKey " + base64ApiKey));
    }

    private void assertSearchResponse(SearchResponse searchResponse, Set<String> docIds) {
        assertSearchResponse(searchResponse, docIds, null);
    }

    private void assertSearchResponse(SearchResponse searchResponse, Set<String> docIds, Set<String> fieldNames) {
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo((long) docIds.size()));
        final SearchHit[] hits = searchResponse.getHits().getHits();
        assertThat(Arrays.stream(hits).map(SearchHit::getId).collect(Collectors.toUnmodifiableSet()), equalTo(docIds));
        if (fieldNames != null) {
            for (SearchHit hit : hits) {
                assertThat(hit.getSourceAsMap().keySet(), equalTo(fieldNames));
            }
        }
    }

    private void assertCacheState(String index, long expectedHits, long expectedMisses) {
        RequestCacheStats requestCacheStats = client().admin()
            .indices()
            .prepareStats(index)
            .setRequestCache(true)
            .get()
            .getTotal()
            .getRequestCache();
        // Check the hit count and miss count together so if they are not
        // correct we can see both values
        assertEquals(
            Arrays.asList(expectedHits, expectedMisses, 0L),
            Arrays.asList(requestCacheStats.getHitCount(), requestCacheStats.getMissCount(), requestCacheStats.getEvictions())
        );
    }

    private void clearCache() {
        assertNoFailures(client().admin().indices().prepareClearCache(DLS_INDEX, FLS_INDEX, INDEX).setRequestCache(true).get());
    }
}
