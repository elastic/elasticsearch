/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.tasks.BaseTasksRequest;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * A request to change throttling on a task.
 */
public class RethrottleRequest extends BaseTasksRequest<RethrottleRequest> {
    /**
     * The throttle to apply to all matching requests in sub-requests per second. 0 means set no throttle and that is the default.
     * Throttling is done between batches, as we start the next scroll requests. That way we can increase the scroll's timeout to make sure
     * that it contains any time that we might wait.
     */
    private float requestsPerSecond = 0;

    /**
     * The throttle to apply to all matching requests in sub-requests per second. 0 means set no throttle and that is the default.
     */
    public float getRequestsPerSecond() {
        return requestsPerSecond;
    }

    /**
     * Set the throttle to apply to all matching requests in sub-requests per second. 0 means set no throttle and that is the default.
     */
    public RethrottleRequest setRequestsPerSecond(float requestsPerSecond) {
        this.requestsPerSecond = requestsPerSecond;
        return this;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        for (String action : getActions()) {
            switch (action) {
            case ReindexAction.NAME:
            case UpdateByQueryAction.NAME:
                continue;
            default:
                validationException = addValidationError(
                        "Can only change the throttling on reindex or update-by-query. Not on [" + action + "]", validationException);
            }
        }
        return validationException;
    }
}
