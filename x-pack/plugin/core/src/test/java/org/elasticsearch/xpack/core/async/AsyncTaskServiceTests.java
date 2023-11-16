/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.async;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.user.User;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.is;

// TODO: test CRUD operations
public class AsyncTaskServiceTests extends ESSingleNodeTestCase {
    private AsyncTaskIndexService<AsyncSearchResponse> indexService;

    public String index = ".async-search";

    @Before
    public void setup() {
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        TransportService transportService = getInstanceFromNode(TransportService.class);
        BigArrays bigArrays = getInstanceFromNode(BigArrays.class);
        indexService = new AsyncTaskIndexService<>(
            index,
            clusterService,
            transportService.getThreadPool().getThreadContext(),
            client(),
            "test_origin",
            AsyncSearchResponse::new,
            writableRegistry(),
            bigArrays
        );
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.getPlugins());
        plugins.add(TestPlugin.class);
        return plugins;
    }

    /**
     * This class exists because AsyncResultsIndexPlugin exists in a different x-pack module.
     */
    public static class TestPlugin extends Plugin implements SystemIndexPlugin {
        @Override
        public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
            return List.of(AsyncTaskIndexService.getSystemIndexDescriptor());
        }

        @Override
        public String getFeatureName() {
            return this.getClass().getSimpleName();
        }

        @Override
        public String getFeatureDescription() {
            return this.getClass().getCanonicalName();
        }
    }

    public void testEnsuredAuthenticatedUserIsSame() throws IOException {
        Authentication original = AuthenticationTestHelper.builder()
            .user(new User("test", "role"))
            .realmRef(new Authentication.RealmRef("realm", "file", "node"))
            .build(false);
        Authentication current = randomBoolean()
            ? original
            : AuthenticationTestHelper.builder()
                .user(new User("test", "role"))
                .realmRef(new Authentication.RealmRef("realm", "file", "node"))
                .build(false);
        current.writeToContext(indexService.getSecurityContext().getThreadContext());
        assertThat(indexService.getSecurityContext().canIAccessResourcesCreatedWithHeaders(getAuthenticationAsHeaders(original)), is(true));

        // original is not authenticated
        assertThat(indexService.getSecurityContext().canIAccessResourcesCreatedWithHeaders(Collections.emptyMap()), is(true));
        // current is not authenticated
        try (ThreadContext.StoredContext ignore = indexService.getSecurityContext().getThreadContext().stashContext()) {
            assertThat(
                indexService.getSecurityContext().canIAccessResourcesCreatedWithHeaders(getAuthenticationAsHeaders(original)),
                is(false)
            );
            assertThat(indexService.getSecurityContext().canIAccessResourcesCreatedWithHeaders(Map.of()), is(true));
        }

        // original user being run as
        final User authenticatingUser = new User("authenticated", "runas");
        final User effectiveUser = new User("test", "role");
        assertThat(
            indexService.getSecurityContext()
                .canIAccessResourcesCreatedWithHeaders(
                    getAuthenticationAsHeaders(
                        AuthenticationTestHelper.builder()
                            .user(authenticatingUser)
                            .realmRef(new Authentication.RealmRef(randomAlphaOfLengthBetween(1, 16), "file", "node"))
                            .runAs()
                            .user(effectiveUser)
                            .realmRef(new Authentication.RealmRef("realm", "file", "node"))
                            .build()
                    )
                ),
            is(true)
        );

        try (ThreadContext.StoredContext ignore = indexService.getSecurityContext().getThreadContext().stashContext()) {
            // current user being run as
            current = AuthenticationTestHelper.builder()
                .user(authenticatingUser)
                .realmRef(new Authentication.RealmRef(randomAlphaOfLengthBetween(1, 16), "file", "node"))
                .runAs()
                .user(effectiveUser)
                .realmRef(new Authentication.RealmRef("realm", "file", "node"))
                .build();
            current.writeToContext(indexService.getSecurityContext().getThreadContext());
            assertThat(
                indexService.getSecurityContext().canIAccessResourcesCreatedWithHeaders(getAuthenticationAsHeaders(original)),
                is(true)
            );

            // both users are run as
            assertThat(
                indexService.getSecurityContext()
                    .canIAccessResourcesCreatedWithHeaders(
                        getAuthenticationAsHeaders(
                            AuthenticationTestHelper.builder()
                                .user(authenticatingUser)
                                .realmRef(new Authentication.RealmRef(randomAlphaOfLengthBetween(1, 16), "file", "node"))
                                .runAs()
                                .user(effectiveUser)
                                .realmRef(new Authentication.RealmRef("realm", "file", "node"))
                                .build()
                        )
                    ),
                is(true)
            );
        }

        try (ThreadContext.StoredContext ignore = indexService.getSecurityContext().getThreadContext().stashContext()) {
            // different authenticated by type
            final Authentication differentRealmType = AuthenticationTestHelper.builder()
                .user(new User("test", "role"))
                .realmRef(new Authentication.RealmRef("realm", randomAlphaOfLength(10), "node"))
                .build(false);
            differentRealmType.writeToContext(indexService.getSecurityContext().getThreadContext());
            assertFalse(indexService.getSecurityContext().canIAccessResourcesCreatedWithHeaders(getAuthenticationAsHeaders(original)));
        }

        // different user
        try (ThreadContext.StoredContext ignore = indexService.getSecurityContext().getThreadContext().stashContext()) {
            final Authentication differentUser = AuthenticationTestHelper.builder()
                .user(new User("test2", "role"))
                .realmRef(new Authentication.RealmRef("realm", "file", "node"))
                .build(false);
            differentUser.writeToContext(indexService.getSecurityContext().getThreadContext());
            assertFalse(indexService.getSecurityContext().canIAccessResourcesCreatedWithHeaders(getAuthenticationAsHeaders(original)));
        }

        // run as different user
        try (ThreadContext.StoredContext ignore = indexService.getSecurityContext().getThreadContext().stashContext()) {
            final Authentication differentRunAs = AuthenticationTestHelper.builder()
                .user(new User("authenticated", "runas"))
                .realmRef(new Authentication.RealmRef("realm_runas", "file", "node1"))
                .runAs()
                .user(new User("test2", "role"))
                .realmRef(new Authentication.RealmRef("realm", "file", "node1"))
                .build();
            differentRunAs.writeToContext(indexService.getSecurityContext().getThreadContext());
            assertFalse(indexService.getSecurityContext().canIAccessResourcesCreatedWithHeaders(getAuthenticationAsHeaders(original)));
        }

        // run as different looked up by type
        try (ThreadContext.StoredContext ignore = indexService.getSecurityContext().getThreadContext().stashContext()) {
            final Authentication runAsDiffType = AuthenticationTestHelper.builder()
                .user(authenticatingUser)
                .realmRef(new Authentication.RealmRef("realm", "file", "node"))
                .runAs()
                .user(effectiveUser)
                .realmRef(new Authentication.RealmRef(randomAlphaOfLengthBetween(1, 16), randomAlphaOfLengthBetween(5, 12), "node"))
                .build();
            runAsDiffType.writeToContext(indexService.getSecurityContext().getThreadContext());
            assertFalse(indexService.getSecurityContext().canIAccessResourcesCreatedWithHeaders(getAuthenticationAsHeaders(original)));
        }
    }

    public void testAutoCreateIndex() throws Exception {
        // To begin with, the results index should be auto-created.
        AsyncExecutionId id = new AsyncExecutionId("0", new TaskId("N/A", 0));
        AsyncSearchResponse resp = new AsyncSearchResponse(id.getEncoded(), true, true, 0L, 0L);
        {
            PlainActionFuture<DocWriteResponse> future = new PlainActionFuture<>();
            indexService.createResponse(id.getDocId(), Collections.emptyMap(), resp, future);
            future.get();
            assertSettings();
        }

        // Delete the index, so we can test subsequent auto-create behaviour
        assertAcked(client().admin().indices().prepareDelete(index));

        // Subsequent response deletes throw a (wrapped) index not found exception
        {
            PlainActionFuture<DeleteResponse> future = new PlainActionFuture<>();
            indexService.deleteResponse(id, future);
            expectThrows(Exception.class, future::get);
        }

        // So do updates
        {
            PlainActionFuture<UpdateResponse> future = new PlainActionFuture<>();
            indexService.updateResponse(id.getDocId(), Collections.emptyMap(), resp, future);
            expectThrows(Exception.class, future::get);
            assertSettings();
        }

        // And so does updating the expiration time
        {
            PlainActionFuture<UpdateResponse> future = new PlainActionFuture<>();
            indexService.updateExpirationTime("0", 10L, future);
            expectThrows(Exception.class, future::get);
            assertSettings();
        }

        // But the index is still auto-created
        {
            PlainActionFuture<DocWriteResponse> future = new PlainActionFuture<>();
            indexService.createResponse(id.getDocId(), Collections.emptyMap(), resp, future);
            future.get();
            assertSettings();
        }
    }

    private void assertSettings() {
        GetIndexResponse getIndexResponse = client().admin().indices().getIndex(new GetIndexRequest().indices(index)).actionGet();
        Settings settings = getIndexResponse.getSettings().get(index);
        Settings expected = AsyncTaskIndexService.settings();
        assertThat(expected, is(settings.filter(expected::hasValue)));
    }

    private Map<String, String> getAuthenticationAsHeaders(Authentication authentication) throws IOException {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        authentication.writeToContext(threadContext);
        return threadContext.getHeaders();
    }
}
