/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.rest.action;

import org.elasticsearch.action.support.tasks.BaseTasksResponse;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestToXContentListener;


class BaseTasksResponseToXContentListener<T extends BaseTasksResponse & ToXContentObject> extends RestToXContentListener<T> {

    BaseTasksResponseToXContentListener(RestChannel channel) {
        super(channel);
    }

    @Override
    protected RestStatus getStatus(T response) {
        if (response.getNodeFailures().size() > 0 || response.getTaskFailures().size() > 0) {
            return RestStatus.INTERNAL_SERVER_ERROR;
        }
        return RestStatus.OK;
    }
}
