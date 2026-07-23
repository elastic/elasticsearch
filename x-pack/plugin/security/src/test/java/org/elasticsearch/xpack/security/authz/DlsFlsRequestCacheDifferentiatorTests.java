/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.mustache.MustacheScriptEngine;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.AuthorizationInfo;
import org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.permission.DocumentPermissions;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissions;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsDefinition;
import org.elasticsearch.xpack.core.security.user.User;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.core.security.SecurityField.DOCUMENT_LEVEL_SECURITY_FEATURE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DlsFlsRequestCacheDifferentiatorTests extends ESTestCase {

    private MockLicenseState licenseState;
    private ThreadContext threadContext;
    private StreamOutput out;
    private DlsFlsRequestCacheDifferentiator differentiator;
    private ShardSearchRequest shardSearchRequest;
    private String indexName;
    private String dlsIndexName;
    private String flsIndexName;
    private String dlsFlsIndexName;

    @Before
    public void init() throws IOException {
        licenseState = mock(MockLicenseState.class);
        when(licenseState.isAllowed(DOCUMENT_LEVEL_SECURITY_FEATURE)).thenReturn(true);
        threadContext = new ThreadContext(Settings.EMPTY);
        out = new BytesStreamOutput();
        final SecurityContext securityContext = new SecurityContext(Settings.EMPTY, threadContext);
        differentiator = new DlsFlsRequestCacheDifferentiator(
            licenseState,
            new SetOnce<>(securityContext),
            new SetOnce<>(mock(ScriptService.class))
        );
        shardSearchRequest = mock(ShardSearchRequest.class);
        indexName = randomAlphaOfLengthBetween(3, 8);
        dlsIndexName = "dls-" + randomAlphaOfLengthBetween(3, 8);
        flsIndexName = "fls-" + randomAlphaOfLengthBetween(3, 8);
        dlsFlsIndexName = "dls-fls-" + randomAlphaOfLengthBetween(3, 8);

        final DocumentPermissions documentPermissions1 = DocumentPermissions.filteredBy(Set.of(new BytesArray("""
            {"term":{"number":1}}""")));

        securityContext.putIndicesAccessControl(
            new IndicesAccessControl(
                true,
                Map.of(
                    flsIndexName,
                    new IndicesAccessControl.IndexAccessControl(
                        new FieldPermissions(new FieldPermissionsDefinition(new String[] { "*" }, new String[] { "private" })),
                        DocumentPermissions.allowAll()
                    ),
                    dlsIndexName,
                    new IndicesAccessControl.IndexAccessControl(FieldPermissions.DEFAULT, documentPermissions1),
                    dlsFlsIndexName,
                    new IndicesAccessControl.IndexAccessControl(
                        new FieldPermissions(new FieldPermissionsDefinition(new String[] { "*" }, new String[] { "private" })),
                        documentPermissions1
                    )
                )
            )
        );
    }

    public void testWillWriteCacheKeyForAnyDlsOrFls() throws IOException {
        when(shardSearchRequest.shardId()).thenReturn(
            new ShardId(randomFrom(dlsIndexName, flsIndexName, dlsFlsIndexName), randomAlphaOfLength(10), randomIntBetween(0, 3))
        );
        differentiator.accept(shardSearchRequest, out);
        assertThat(out.position(), greaterThan(0L));
    }

    public void testWillDoNothingIfNoDlsFls() throws IOException {
        when(shardSearchRequest.shardId()).thenReturn(new ShardId(indexName, randomAlphaOfLength(10), randomIntBetween(0, 3)));
        differentiator.accept(shardSearchRequest, out);
        assertThat(out.position(), equalTo(0L));
    }

    /**
     * When a DLS query is templated over the user's application resources ({@code _user.application_resources}),
     * the request-cache key must incorporate those resources — otherwise two users with different Kibana spaces
     * could receive each other's cached results. Verifies same resources → same key, different resources → different key.
     */
    public void testCacheKeyIncorporatesTemplatedApplicationResources() throws IOException {
        final BytesReference marketing = buildCacheKeyForResources("space:marketing");
        final BytesReference marketingAgain = buildCacheKeyForResources("space:marketing");
        final BytesReference finance = buildCacheKeyForResources("space:finance");

        assertThat(marketing, equalTo(marketingAgain));
        assertThat(marketing, not(equalTo(finance)));
    }

    /**
     * When a DLS query is templated over the user's resource-scoped application privileges
     * ({@code _user.application_privileges}), the request-cache key must incorporate those privileges —
     * otherwise two users with the same role template but different space/action grants could read each
     * other's cached results. Verifies same privileges → same key, different privileges → different key.
     */
    public void testCacheKeyIncorporatesTemplatedApplicationPrivileges() throws IOException {
        final BytesReference marketing = buildCacheKeyForPrivileges("space:marketing|saved_object:dashboard/get");
        final BytesReference marketingAgain = buildCacheKeyForPrivileges("space:marketing|saved_object:dashboard/get");
        final BytesReference finance = buildCacheKeyForPrivileges("space:finance|saved_object:dashboard/get");

        assertThat(marketing, equalTo(marketingAgain));
        assertThat(marketing, not(equalTo(finance)));
    }

    /**
     * When both {@code _user.application_resources} and {@code _user.application_privileges} appear in the
     * DLS query template, the cache key must reflect both — a change in either alone must produce a distinct key.
     */
    public void testCacheKeyIncorporatesBothApplicationResourcesAndPrivileges() throws IOException {
        final Map<String, List<String>> resourcesA = Map.of("myapp", List.of("space:marketing"));
        final Map<String, List<String>> resourcesB = Map.of("myapp", List.of("space:finance"));
        final Map<String, List<String>> privilegesA = Map.of("myapp", List.of("space:marketing|data:read/*"));
        final Map<String, List<String>> privilegesB = Map.of("myapp", List.of("space:finance|data:read/*"));

        final BytesReference aaKey = buildCacheKeyForBoth(resourcesA, privilegesA);
        final BytesReference aaKeyAgain = buildCacheKeyForBoth(resourcesA, privilegesA);
        final BytesReference abKey = buildCacheKeyForBoth(resourcesA, privilegesB);
        final BytesReference baKey = buildCacheKeyForBoth(resourcesB, privilegesA);
        final BytesReference bbKey = buildCacheKeyForBoth(resourcesB, privilegesB);

        assertThat(aaKey, equalTo(aaKeyAgain));
        assertThat(aaKey, not(equalTo(abKey)));  // same resources, different privileges → different key
        assertThat(aaKey, not(equalTo(baKey)));  // different resources, same privileges → different key
        assertThat(aaKey, not(equalTo(bbKey)));
    }

    private BytesReference buildCacheKeyForResources(String... resources) throws IOException {
        final DocumentPermissions templatedDls = DocumentPermissions.filteredBy(Set.of(new BytesArray("""
            {"template":{"source":"{\\"terms\\":{\\"space_resources\\":{{#toJson}}_user.application_resources{{/toJson}}}}"}}""")));
        final Map<String, List<String>> appResources = Map.of("myapp", List.of(resources));
        return buildCacheKeyWithInfo(templatedDls, new StubAuthorizationInfo(appResources, Map.of()));
    }

    private BytesReference buildCacheKeyForPrivileges(String... privileges) throws IOException {
        final DocumentPermissions templatedDls = DocumentPermissions.filteredBy(Set.of(new BytesArray("""
            {"template":{"source":"{\\"terms\\":{\\"space_perms\\":{{#toJson}}_user.application_privileges{{/toJson}}}}"}}""")));
        final Map<String, List<String>> appPrivileges = Map.of("myapp", List.of(privileges));
        return buildCacheKeyWithInfo(templatedDls, new StubAuthorizationInfo(Map.of(), appPrivileges));
    }

    private BytesReference buildCacheKeyForBoth(
        Map<String, List<String>> applicationResources,
        Map<String, List<String>> applicationPrivileges
    ) throws IOException {
        final DocumentPermissions templatedDls = DocumentPermissions.filteredBy(Set.of(new BytesArray("""
            {"template":{"source":"{\\"bool\\":{\\"must\\":[{\\"terms\\":{\\"space_resources\\":{{#toJson}}_user.application_resources{{/toJson}}}},\
{\\"terms\\":{\\"space_perms\\":{{#toJson}}_user.application_privileges{{/toJson}}}}]}}"}}""")));
        return buildCacheKeyWithInfo(templatedDls, new StubAuthorizationInfo(applicationResources, applicationPrivileges));
    }

    /**
     * Builds the DLS/FLS request-cache key for an index with the given DLS query and authorization info.
     * Uses a fresh {@link DocumentPermissions} per call because it memoizes its evaluated queries — a
     * shared instance would render only once and mask per-user differences.
     */
    private BytesReference buildCacheKeyWithInfo(DocumentPermissions templatedDls, StubAuthorizationInfo authInfo) throws IOException {
        final ThreadContext localThreadContext = new ThreadContext(Settings.EMPTY);
        final SecurityContext securityContext = new SecurityContext(Settings.EMPTY, localThreadContext);
        // The DLS template render reads the authenticated user (e.g. for _user.username).
        final Authentication authentication = AuthenticationTestHelper.builder().user(new User("test-user")).build(false);
        authentication.writeToContext(localThreadContext);
        // A real mustache-backed ScriptService so the template actually renders (the base test class mocks it,
        // which is fine for the non-templated cases but cannot render {{#toJson}}).
        final ScriptService realScriptService = new ScriptService(
            Settings.EMPTY,
            Collections.singletonMap(MustacheScriptEngine.NAME, new MustacheScriptEngine(Settings.EMPTY)),
            ScriptModule.CORE_CONTEXTS,
            () -> 1L,
            TestProjectResolvers.singleProject(randomProjectIdOrDefault())
        );
        final DlsFlsRequestCacheDifferentiator localDifferentiator = new DlsFlsRequestCacheDifferentiator(
            licenseState,
            new SetOnce<>(securityContext),
            new SetOnce<>(realScriptService)
        );

        final String templatedIndexName = "tmpl-" + randomAlphaOfLengthBetween(3, 8);
        securityContext.putIndicesAccessControl(
            new IndicesAccessControl(
                true,
                Map.of(templatedIndexName, new IndicesAccessControl.IndexAccessControl(FieldPermissions.DEFAULT, templatedDls))
            )
        );
        AuthorizationServiceField.AUTHORIZATION_INFO_VALUE.set(localThreadContext, authInfo);

        final ShardSearchRequest request = mock(ShardSearchRequest.class);
        when(request.shardId()).thenReturn(new ShardId(templatedIndexName, randomAlphaOfLength(10), randomIntBetween(0, 3)));
        final BytesStreamOutput keyOut = new BytesStreamOutput();
        localDifferentiator.accept(request, keyOut);
        assertThat(keyOut.position(), greaterThan(0L));
        return keyOut.bytes();
    }

    /**
     * Minimal {@link AuthorizationInfo} that carries application resources and privileges so the
     * differentiator can read them from the thread context exactly as it reads the RBAC engine's info
     * at runtime.
     */
    private record StubAuthorizationInfo(
        Map<String, List<String>> applicationResources,
        Map<String, List<String>> applicationPrivileges
    ) implements AuthorizationInfo {
        @Override
        public Map<String, Object> asMap() {
            return Map.of();
        }

        @Override
        public Map<String, List<String>> getApplicationResources() {
            return applicationResources;
        }

        @Override
        public Map<String, List<String>> getApplicationPrivileges() {
            return applicationPrivileges;
        }
    }

}
