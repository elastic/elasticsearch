/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.user;

import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsAction;
import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.index.RestrictedIndicesNames;
import org.elasticsearch.xpack.core.security.user.AsyncEqlSearchUser;
import org.elasticsearch.xpack.core.security.user.AsyncEqlSearchUser;
import org.elasticsearch.xpack.core.watcher.transport.actions.get.GetWatchAction;
import org.hamcrest.Matchers;

import java.util.Arrays;
import java.util.function.Predicate;

import static org.mockito.Mockito.mock;

public class AsyncEqlSearchUserTests extends ESTestCase {

    public void testAsyncEqlSearchUserCannotAccessNonRestrictedIndices() {
        for (String action : Arrays.asList(GetAction.NAME, DeleteAction.NAME, SearchAction.NAME, IndexAction.NAME)) {
            Predicate<String> predicate = AsyncEqlSearchUser.ROLE.indices().allowedIndicesMatcher(action);
            String index = randomAlphaOfLengthBetween(3, 12);
            if (false == RestrictedIndicesNames.isRestricted(index)) {
                assertThat(predicate.test(index), Matchers.is(false));
            }
            index = "." + randomAlphaOfLengthBetween(3, 12);
            if (false == RestrictedIndicesNames.isRestricted(index)) {
                assertThat(predicate.test(index), Matchers.is(false));
            }
        }
    }

    public void testAsyncEqlSearchUserCanAccessOnlyAsyncEqlSearchRestrictedIndices() {
        for (String action : Arrays.asList(GetAction.NAME, DeleteAction.NAME, SearchAction.NAME, IndexAction.NAME)) {
            final Predicate<String> predicate = AsyncEqlSearchUser.ROLE.indices().allowedIndicesMatcher(action);
            for (String index : RestrictedIndicesNames.RESTRICTED_NAMES) {
                assertThat(predicate.test(index), Matchers.is(false));
            }
            assertThat(predicate.test(RestrictedIndicesNames.ASYNC_EQL_SEARCH_PREFIX + randomAlphaOfLengthBetween(0, 3)),
                Matchers.is(true));
            assertThat(predicate.test(RestrictedIndicesNames.ASYNC_SEARCH_PREFIX + randomAlphaOfLengthBetween(0, 3)),
                Matchers.is(false));
        }
    }

    public void testAsyncEqlSearchUserHasNoClusterPrivileges() {
        for (String action : Arrays.asList(ClusterStateAction.NAME, GetWatchAction.NAME, ClusterStatsAction.NAME, NodesStatsAction.NAME)) {
            assertThat(AsyncEqlSearchUser.ROLE.cluster().check(action, mock(TransportRequest.class), mock(Authentication.class)),
                    Matchers.is(false));
        }
    }
}
