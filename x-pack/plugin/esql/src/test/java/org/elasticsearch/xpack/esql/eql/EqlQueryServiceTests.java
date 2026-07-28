/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.eql;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.eql.action.EqlSearchAction;
import org.elasticsearch.xpack.eql.action.EqlSearchRequest;
import org.mockito.ArgumentCaptor;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link EqlQueryService}: verify the (index, query) pair plus the pushed-down {@code size} and the ES|QL
 * parent task are stamped onto the {@link EqlSearchRequest} before it is dispatched to the EQL search transport action.
 */
public class EqlQueryServiceTests extends ESTestCase {

    private EqlSearchRequest captureRequest(Integer size, CancellableTask parentTask) {
        EqlSearchRequest request = captureRequestFor("idx", size, parentTask);
        assertThat(request.indices(), equalTo(new String[] { "idx" }));
        assertThat(request.query(), equalTo("any where true"));
        return request;
    }

    private EqlSearchRequest captureRequestFor(String index, Integer size, CancellableTask parentTask) {
        Client client = mock(Client.class);
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.localNode()).thenReturn(DiscoveryNodeUtils.create("local-node"));

        EqlQueryService service = new EqlQueryService(client, clusterService);
        service.query(index, "any where true", size, parentTask, ActionListener.noop());

        ArgumentCaptor<EqlSearchRequest> captor = ArgumentCaptor.forClass(EqlSearchRequest.class);
        verify(client).execute(eq(EqlSearchAction.INSTANCE), captor.capture(), any());
        return captor.getValue();
    }

    public void testSizeAndParentTaskAreForwarded() {
        CancellableTask task = new CancellableTask(42L, "type", "action", "desc", TaskId.EMPTY_TASK_ID, Map.of());
        EqlSearchRequest request = captureRequest(7, task);
        assertThat(request.size(), equalTo(7));
        assertThat(request.getParentTask(), equalTo(new TaskId("local-node", 42L)));
    }

    public void testNoSizeKeepsEqlDefaultAndNoParentTask() {
        EqlSearchRequest request = captureRequest(null, null);
        // No pushed limit -> EQL keeps its own default size (10); no parent task set.
        assertThat(request.size(), equalTo(10));
        assertThat(request.getParentTask().isSet(), is(false));
    }

    public void testCommaSeparatedIndexPatternIsSplit() {
        // The parser hands a comma-joined pattern; the service must split it so each index reaches EQL separately.
        EqlSearchRequest request = captureRequestFor("idx1,idx2,logs-*", null, null);
        assertThat(request.indices(), equalTo(new String[] { "idx1", "idx2", "logs-*" }));
    }

    public void testSingleIndexPatternIsForwardedVerbatim() {
        EqlSearchRequest request = captureRequestFor("cluster_a:logs-*", null, null);
        assertThat(request.indices(), equalTo(new String[] { "cluster_a:logs-*" }));
    }
}
