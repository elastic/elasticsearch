/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.search;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.user.User;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;

import static org.elasticsearch.xpack.search.AsyncSearchResponseTests.assertEqualResponses;
import static org.elasticsearch.xpack.search.AsyncSearchResponseTests.randomAsyncSearchResponse;
import static org.elasticsearch.xpack.search.AsyncSearchResponseTests.randomSearchResponse;
import static org.elasticsearch.xpack.search.GetAsyncSearchRequestTests.randomSearchId;

// TODO: test CRUD operations
public class AsyncSearchIndexServiceTests extends ESSingleNodeTestCase {
    private AsyncSearchIndexService indexService;

    @Before
    public void setup() {
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        TransportService transportService = getInstanceFromNode(TransportService.class);
        indexService = new AsyncSearchIndexService(clusterService, transportService.getThreadPool().getThreadContext(),
            client(), writableRegistry());
    }

    public void testEncodeSearchResponse() throws IOException {
        for (int i = 0; i < 10; i++) {
            AsyncSearchResponse response = randomAsyncSearchResponse(randomSearchId(), randomSearchResponse());
            String encoded = indexService.encodeResponse(response);
            AsyncSearchResponse same = indexService.decodeResponse(encoded);
            assertEqualResponses(response, same);
        }
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
}
