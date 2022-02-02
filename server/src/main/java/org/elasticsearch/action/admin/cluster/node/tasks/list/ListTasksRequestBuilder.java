/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.tasks.list;

import org.elasticsearch.action.support.tasks.TasksRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;

/**
 * Builder for the request to retrieve the list of tasks running on the specified nodes
 */
public class ListTasksRequestBuilder extends TasksRequestBuilder<ListTasksRequest, ListTasksResponse, ListTasksRequestBuilder> {

    public ListTasksRequestBuilder(ElasticsearchClient client, ListTasksAction action) {
        super(client, action, new ListTasksRequest());
    }

    /**
     * Should detailed task information be returned.
     */
    public ListTasksRequestBuilder setDetailed(boolean detailed) {
        request.setDetailed(detailed);
        return this;
    }

    /**
     * Should this request wait for all found tasks to complete?
     */
    public final ListTasksRequestBuilder setWaitForCompletion(boolean waitForCompletion) {
        request.setWaitForCompletion(waitForCompletion);
        return this;
    }

    /**
     * Should the task match specific descriptions.
     *
     * Cannot be used unless {@link ListTasksRequestBuilder#setDetailed(boolean)} is `true`
     * @param descriptions string array containing simple regex or absolute description matching
     * @return the builder with descriptions set
     */
    public final ListTasksRequestBuilder setDescriptions(String... descriptions) {
        request.setDescriptions(descriptions);
        return this;
    }
}
