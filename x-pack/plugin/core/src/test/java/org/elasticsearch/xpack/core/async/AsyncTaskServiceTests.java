/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.async;

import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.user.User;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;

// TODO: test CRUD operations
public class AsyncTaskServiceTests extends ESSingleNodeTestCase {
    private AsyncTaskIndexService<AsyncSearchResponse> indexService;

    public String index = ".async-search";

    @Before
    public void setup() {
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        TransportService transportService = getInstanceFromNode(TransportService.class);
        indexService = new AsyncTaskIndexService<>(index, clusterService,
            transportService.getThreadPool().getThreadContext(),
            client(), "test_origin", AsyncSearchResponse::new, writableRegistry());
    }

    public void testEnsuredAuthenticatedUserIsSame() throws IOException {
        Authentication original =
            new Authentication(new User("test", "role"), new Authentication.RealmRef("realm", "file", "node"), null);
        Authentication current = randomBoolean() ? original :
            new Authentication(new User("test", "role"), new Authentication.RealmRef("realm", "file", "node"), null);
        assertTrue(indexService.ensureAuthenticatedUserIsSame(original, current));
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        original.writeToContext(threadContext);
        assertTrue(indexService.ensureAuthenticatedUserIsSame(threadContext.getHeaders(), current));

        // original is not set
        assertTrue(indexService.ensureAuthenticatedUserIsSame(Collections.emptyMap(), current));
        // current is not set
        assertFalse(indexService.ensureAuthenticatedUserIsSame(threadContext.getHeaders(), null));

        // original user being run as
        User user = new User(new User("test", "role"), new User("authenticated", "runas"));
        current = new Authentication(user, new Authentication.RealmRef("realm", "file", "node"),
            new Authentication.RealmRef(randomAlphaOfLengthBetween(1, 16), "file", "node"));
        assertTrue(indexService.ensureAuthenticatedUserIsSame(original, current));
        assertTrue(indexService.ensureAuthenticatedUserIsSame(threadContext.getHeaders(), current));

        // both user are run as
        current = new Authentication(user, new Authentication.RealmRef("realm", "file", "node"),
            new Authentication.RealmRef(randomAlphaOfLengthBetween(1, 16), "file", "node"));
        Authentication runAs = current;
        assertTrue(indexService.ensureAuthenticatedUserIsSame(runAs, current));
        threadContext = new ThreadContext(Settings.EMPTY);
        original.writeToContext(threadContext);
        assertTrue(indexService.ensureAuthenticatedUserIsSame(threadContext.getHeaders(), current));

        // different authenticated by type
        Authentication differentRealmType =
            new Authentication(new User("test", "role"), new Authentication.RealmRef("realm", randomAlphaOfLength(5), "node"), null);
        threadContext = new ThreadContext(Settings.EMPTY);
        original.writeToContext(threadContext);
        assertFalse(indexService.ensureAuthenticatedUserIsSame(original, differentRealmType));
        assertFalse(indexService.ensureAuthenticatedUserIsSame(threadContext.getHeaders(), differentRealmType));

        // wrong user
        Authentication differentUser =
            new Authentication(new User("test2", "role"), new Authentication.RealmRef("realm", "realm", "node"), null);
        assertFalse(indexService.ensureAuthenticatedUserIsSame(original, differentUser));

        // run as different user
        Authentication diffRunAs = new Authentication(new User(new User("test2", "role"), new User("authenticated", "runas")),
            new Authentication.RealmRef("realm", "file", "node1"), new Authentication.RealmRef("realm", "file", "node1"));
        assertFalse(indexService.ensureAuthenticatedUserIsSame(original, diffRunAs));
        assertFalse(indexService.ensureAuthenticatedUserIsSame(threadContext.getHeaders(), diffRunAs));

        // run as different looked up by type
        Authentication runAsDiffType = new Authentication(user, new Authentication.RealmRef("realm", "file", "node"),
            new Authentication.RealmRef(randomAlphaOfLengthBetween(1, 16), randomAlphaOfLengthBetween(5, 12), "node"));
        assertFalse(indexService.ensureAuthenticatedUserIsSame(original, runAsDiffType));
        assertFalse(indexService.ensureAuthenticatedUserIsSame(threadContext.getHeaders(), runAsDiffType));
    }

    public void testAutoCreateIndex() throws Exception {
        {
            PlainActionFuture<Void> future = PlainActionFuture.newFuture();
            indexService.createIndexIfNecessary(future);
            future.get();
            assertSettings();
        }
        AcknowledgedResponse ack = client().admin().indices().prepareDelete(index).get();
        assertTrue(ack.isAcknowledged());

        AsyncExecutionId id = new AsyncExecutionId("0", new TaskId("N/A", 0));
        AsyncSearchResponse resp = new AsyncSearchResponse(id.getEncoded(), true, true, 0L, 0L);
        {
            PlainActionFuture<IndexResponse> future = PlainActionFuture.newFuture();
            indexService.createResponse(id.getDocId(), Collections.emptyMap(), resp, future);
            future.get();
            assertSettings();
        }
        ack = client().admin().indices().prepareDelete(index).get();
        assertTrue(ack.isAcknowledged());
        {
            PlainActionFuture<DeleteResponse> future = PlainActionFuture.newFuture();
            indexService.deleteResponse(id, future);
            future.get();
            assertSettings();
        }
        ack = client().admin().indices().prepareDelete(index).get();
        assertTrue(ack.isAcknowledged());
        {
            PlainActionFuture<UpdateResponse> future = PlainActionFuture.newFuture();
            indexService.updateResponse(id.getDocId(), Collections.emptyMap(), resp, future);
            expectThrows(Exception.class, () -> future.get());
            assertSettings();
        }
        ack = client().admin().indices().prepareDelete(index).get();
        assertTrue(ack.isAcknowledged());
        {
            PlainActionFuture<UpdateResponse> future = PlainActionFuture.newFuture();
            indexService.updateExpirationTime("0", 10L, future);
            expectThrows(Exception.class, () -> future.get());
            assertSettings();
        }
    }

    private void assertSettings() throws IOException {
        GetIndexResponse getIndexResponse = client().admin().indices().getIndex(
            new GetIndexRequest().indices(index)).actionGet();
        Settings settings = getIndexResponse.getSettings().get(index);
        Settings expected = AsyncTaskIndexService.settings();
        assertEquals(expected, settings.filter(key -> expected.hasValue(key)));
    }
}
