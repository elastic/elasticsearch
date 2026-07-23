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
     * When a DLS query is templated over the user's application privileges, the request-cache key must
     * incorporate those privileges — otherwise two users who share the same role/template but hold
     * different application privileges (e.g. different Kibana spaces) could read each other's cached
     * results. Verifies that identical privileges yield an identical cache key while differing privileges
     * yield a different one.
     */
    public void testCacheKeyIncorporatesTemplatedApplicationPrivileges() throws IOException {
        final BytesReference marketing = buildCacheKey("space:marketing|saved_object:dashboard/get");
        final BytesReference marketingAgain = buildCacheKey("space:marketing|saved_object:dashboard/get");
        final BytesReference finance = buildCacheKey("space:finance|saved_object:dashboard/get");

        assertThat(marketing, equalTo(marketingAgain));
        assertThat(marketing, not(equalTo(finance)));
    }

    /**
     * Builds the DLS/FLS request-cache key for an index whose DLS query is templated over the user's
     * resource-scoped application privileges (the shape the Agent Builder ai-index DLS role uses), with the
     * given {@code applicationPrivileges} tokens exposed on the thread-context authorization info (as the
     * RBAC engine would at runtime). Returns the written key bytes.
     */
    private BytesReference buildCacheKey(String... applicationPrivileges) throws IOException {
        // A fresh DocumentPermissions per call: it memoizes its evaluated queries, so a shared instance would
        // render only once and mask the per-user difference we are asserting.
        final DocumentPermissions templatedDls = DocumentPermissions.filteredBy(Set.of(new BytesArray("""
            {"template":{"source":"{\\"terms\\":{\\"space_perms\\":{{#toJson}}_user.application_privileges{{/toJson}}}}"}}""")));
        final ThreadContext localThreadContext = new ThreadContext(Settings.EMPTY);
        final SecurityContext securityContext = new SecurityContext(Settings.EMPTY, localThreadContext);
        // The DLS template render reads the authenticated user (e.g. for _user.username); a templated query
        // needs one present in the context.
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
        // The resolved authorization info the render sites read from the thread context.
        AuthorizationServiceField.AUTHORIZATION_INFO_VALUE.set(
            localThreadContext,
            new StubAuthorizationInfo(Map.of("kibana-.kibana", List.of(applicationPrivileges)))
        );

        final ShardSearchRequest request = mock(ShardSearchRequest.class);
        when(request.shardId()).thenReturn(new ShardId(templatedIndexName, randomAlphaOfLength(10), randomIntBetween(0, 3)));
        final BytesStreamOutput keyOut = new BytesStreamOutput();
        localDifferentiator.accept(request, keyOut);
        assertThat(keyOut.position(), greaterThan(0L));
        return keyOut.bytes();
    }

    /**
     * Minimal {@link AuthorizationInfo} that only carries application privileges, so the differentiator can
     * read them from the thread context exactly as it reads the RBAC engine's info at runtime.
     */
    private record StubAuthorizationInfo(Map<String, List<String>> applicationPrivileges) implements AuthorizationInfo {
        @Override
        public Map<String, Object> asMap() {
            return Map.of();
        }

        @Override
        public Map<String, List<String>> getApplicationPrivileges() {
            return applicationPrivileges;
        }
    }

}
