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
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class RethrottlePersistentReindexAction extends ActionType<RethrottlePersistentReindexAction.Response> {
    public static final RethrottlePersistentReindexAction INSTANCE = new RethrottlePersistentReindexAction();
    public static final String NAME = "cluster:admin/reindex/rethrottle/persistent";

    private RethrottlePersistentReindexAction() {
        super(NAME, Response::new);
    }

    public static class Request extends MasterNodeRequest<Request> {

        private final String persistentTaskId;

        /**
         * The throttle to apply to all matching requests in sub-requests per second. 0 means set no throttle.
         */
        private final float requestsPerSecond;

        public Request(String persistentTaskId, float requestsPerSecond) {
            this.persistentTaskId = persistentTaskId;
            this.requestsPerSecond = requestsPerSecond;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.persistentTaskId = in.readString();
            this.requestsPerSecond = in.readFloat();
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (persistentTaskId == null) {
                validationException = ValidateActions.addValidationError("persistent_task_id must be set", validationException);
            }
            if (Float.isNaN(requestsPerSecond) || requestsPerSecond < 0) {
                 validationException = ValidateActions.addValidationError("requests_per_second must be >= 0", validationException);
            }

            return validationException;
        }

        public String getPersistentTaskId() {
            return persistentTaskId;
        }

        public float getRequestsPerSecond() {
            return requestsPerSecond;
        }
    }


    /**
     * Empty response signals success. This is not compatible with existing rethrottle, but that is only a docs issue
     */
    public static class Response extends ActionResponse implements ToXContentObject {

        public Response() {
        }

        public Response(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {

        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder;
        }
    }
}
