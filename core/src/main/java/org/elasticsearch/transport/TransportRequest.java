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

package org.elasticsearch.transport;

import org.elasticsearch.tasks.Task;

/**
 */
public abstract class TransportRequest extends TransportMessage<TransportRequest> {

    public static class Empty extends TransportRequest {

        public static final Empty INSTANCE = new Empty();

        public Empty() {
            super();
        }

        public Empty(TransportRequest request) {
            super(request);
        }
    }

    public TransportRequest() {
    }

    protected TransportRequest(TransportRequest request) {
        super(request);
    }

    /**
     * Returns the task object that should be used to keep track of the processing of the request.
     *
     * A request can override this method and return null to avoid being tracked by the task manager.
     */
    public Task createTask(long id, String type, String action) {
        return new Task(id, type, action, getDescription());
    }

    /**
     * Returns optional description of the request to be displayed by the task manager
     */
    public String getDescription() {
        return "";
    }

}
