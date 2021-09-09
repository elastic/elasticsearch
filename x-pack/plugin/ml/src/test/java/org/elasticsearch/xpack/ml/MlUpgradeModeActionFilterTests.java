/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.action.CloseJobAction;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.action.SetUpgradeModeAction;
import org.elasticsearch.xpack.security.action.filter.SecurityActionFilter;
import org.junit.After;
import org.junit.Before;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class MlUpgradeModeActionFilterTests extends ESTestCase {

    private static final String DISALLOWED_ACTION = PutJobAction.NAME;
    private static final String ALLOWED_ACTION = SetUpgradeModeAction.NAME;
    private static final String DISALLOWED_ACTION_WITH_RESET_MODE_EXEMPTION = CloseJobAction.NAME;

    private ClusterService clusterService;
    private Task task;
    private ActionRequest request;
    private ActionListener<ActionResponse> listener;
    private ActionFilterChain<ActionRequest, ActionResponse> chain;

    @Before
    @SuppressWarnings("unchecked")
    public void setUpMocks() {
        clusterService = mock(ClusterService.class);
        task = mock(Task.class);
        request = mock(ActionRequest.class);
        listener = mock(ActionListener.class);
        chain = mock(ActionFilterChain.class);
    }

    @After
    public void assertNoMoreInteractions() {
        verifyNoMoreInteractions(task, request, listener, chain);
    }

    public void testApply_ActionDisallowedInUpgradeMode() {
        String action = randomFrom(DISALLOWED_ACTION, DISALLOWED_ACTION_WITH_RESET_MODE_EXEMPTION);
        MlUpgradeModeActionFilter filter = new MlUpgradeModeActionFilter(clusterService);
        filter.apply(task, action, request, listener, chain);

        filter.setUpgradeResetFlags(createClusterChangedEvent(createClusterState(true, false)));
        ElasticsearchStatusException e =
            expectThrows(
                ElasticsearchStatusException.class,
                () -> filter.apply(task, action, request, listener, chain));

        filter.setUpgradeResetFlags(createClusterChangedEvent(createClusterState(false, false)));
        filter.apply(task, action, request, listener, chain);

        assertThat(e.getMessage(), is(equalTo("Cannot perform " + action + " action while upgrade mode is enabled")));
        assertThat(e.status(), is(equalTo(RestStatus.TOO_MANY_REQUESTS)));

        verify(chain, times(2)).proceed(task, action, request, listener);
    }

    public void testApply_ActionAllowedInUpgradeMode() {
        MlUpgradeModeActionFilter filter = new MlUpgradeModeActionFilter(clusterService);
        filter.apply(task, ALLOWED_ACTION, request, listener, chain);

        filter.setUpgradeResetFlags(createClusterChangedEvent(createClusterState(true, false)));
        filter.apply(task, ALLOWED_ACTION, request, listener, chain);

        filter.setUpgradeResetFlags(createClusterChangedEvent(createClusterState(false, false)));
        filter.apply(task, ALLOWED_ACTION, request, listener, chain);

        verify(chain, times(3)).proceed(task, ALLOWED_ACTION, request, listener);
    }

    public void testApply_ActionDisallowedInUpgradeModeWithResetModeExemption() {
        MlUpgradeModeActionFilter filter = new MlUpgradeModeActionFilter(clusterService);
        filter.apply(task, DISALLOWED_ACTION_WITH_RESET_MODE_EXEMPTION, request, listener, chain);

        filter.setUpgradeResetFlags(createClusterChangedEvent(createClusterState(true, true)));
        filter.apply(task, DISALLOWED_ACTION_WITH_RESET_MODE_EXEMPTION, request, listener, chain);

        filter.setUpgradeResetFlags(createClusterChangedEvent(createClusterState(false, true)));
        filter.apply(task, DISALLOWED_ACTION_WITH_RESET_MODE_EXEMPTION, request, listener, chain);

        verify(chain, times(3)).proceed(task, DISALLOWED_ACTION_WITH_RESET_MODE_EXEMPTION, request, listener);
    }

    public void testOrder_UpgradeFilterIsExecutedAfterSecurityFilter() {
        MlUpgradeModeActionFilter upgradeModeFilter = new MlUpgradeModeActionFilter(clusterService);
        SecurityActionFilter securityFilter = new SecurityActionFilter(null, null, null, null, mock(ThreadPool.class), null, null);

        ActionFilter[] actionFiltersInOrderOfExecution = new ActionFilters(Sets.newHashSet(upgradeModeFilter, securityFilter)).filters();
        assertThat(actionFiltersInOrderOfExecution, is(arrayContaining(securityFilter, upgradeModeFilter)));
    }

    private static ClusterChangedEvent createClusterChangedEvent(ClusterState clusterState) {
        return new ClusterChangedEvent("created-from-test", clusterState, clusterState);
    }

    private static ClusterState createClusterState(boolean isUpgradeMode, boolean isResetMode) {
        return ClusterState.builder(new ClusterName("MlUpgradeModeActionFilterTests"))
            .metadata(Metadata.builder().putCustom(MlMetadata.TYPE,
                new MlMetadata.Builder().isUpgradeMode(isUpgradeMode).isResetMode(isResetMode).build()))
            .build();
    }
}
