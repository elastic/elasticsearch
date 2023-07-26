/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.utils;

import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksAction;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequestBuilder;
import org.elasticsearch.client.internal.AdminClient;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ClusterAdminClient;
import org.elasticsearch.test.ESTestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TaskRetrieverTests extends ESTestCase {
    /**
     * A helper method for setting up the mock cluster client so that it will return a valid {@link ListTasksRequestBuilder}.
     *
     * @return a mocked Client
     */
    public static Client mockListTasksClient() {
        var client = mock(Client.class);
        var cluster = mock(ClusterAdminClient.class);
        var admin = mock(AdminClient.class);

        when(client.admin()).thenReturn(admin);
        when(admin.cluster()).thenReturn(cluster);
        when(cluster.prepareListTasks()).thenReturn(new ListTasksRequestBuilder(client, ListTasksAction.INSTANCE));

        return client;
    }
}
