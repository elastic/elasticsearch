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

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.action.admin.indices.shrink.ResizeRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;

public abstract class RestResizeHandler extends BaseRestHandler {

    RestResizeHandler() {
    }

    @Override
    public abstract String getName();

    abstract ResizeType getResizeType();

    @Override
    public final RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final ResizeRequest resizeRequest = new ResizeRequest(request.param("target"), request.param("index"));
        resizeRequest.setResizeType(getResizeType());
        request.applyContentParser(resizeRequest::fromXContent);
        resizeRequest.timeout(request.paramAsTime("timeout", resizeRequest.timeout()));
        resizeRequest.masterNodeTimeout(request.paramAsTime("master_timeout", resizeRequest.masterNodeTimeout()));
        resizeRequest.setWaitForActiveShards(ActiveShardCount.parseString(request.param("wait_for_active_shards")));
        return channel -> client.admin().indices().resizeIndex(resizeRequest, new RestToXContentListener<>(channel));
    }

    public static class RestShrinkIndexAction extends RestResizeHandler {

        public RestShrinkIndexAction(final RestController controller) {
            controller.registerHandler(RestRequest.Method.PUT, "/{index}/_shrink/{target}", this);
            controller.registerHandler(RestRequest.Method.POST, "/{index}/_shrink/{target}", this);
        }

        @Override
        public String getName() {
            return "shrink_index_action";
        }

        @Override
        protected ResizeType getResizeType() {
            return ResizeType.SHRINK;
        }

    }

    public static class RestSplitIndexAction extends RestResizeHandler {

        public RestSplitIndexAction(final RestController controller) {
            controller.registerHandler(RestRequest.Method.PUT, "/{index}/_split/{target}", this);
            controller.registerHandler(RestRequest.Method.POST, "/{index}/_split/{target}", this);
        }

        @Override
        public String getName() {
            return "split_index_action";
        }

        @Override
        protected ResizeType getResizeType() {
            return ResizeType.SPLIT;
        }

    }

    public static class RestCloneIndexAction extends RestResizeHandler {

        public RestCloneIndexAction(final RestController controller) {
            controller.registerHandler(RestRequest.Method.PUT, "/{index}/_clone/{target}", this);
            controller.registerHandler(RestRequest.Method.POST, "/{index}/_clone/{target}", this);
        }

        @Override
        public String getName() {
            return "clone_index_action";
        }

        @Override
        protected ResizeType getResizeType() {
            return ResizeType.CLONE;
        }

    }

}
