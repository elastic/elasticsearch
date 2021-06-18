/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

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

import org.elasticsearch.action.admin.indices.diskusage.AnalyzeIndexDiskUsageAction;
import org.elasticsearch.action.admin.indices.diskusage.AnalyzeIndexDiskUsageRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestAnalyzeIndexDiskUsageAction extends BaseRestHandler {

    @Override
    public List<RestHandler.Route> routes() {
        return List.of(new RestHandler.Route(POST, "/{index}/_disk_usage"));
    }

    @Override
    public String getName() {
        return "analyze_index_disk_usage_action";
    }

    @Override
    public BaseRestHandler.RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        if (Booleans.parseBoolean(request.param("run_expensive_tasks"), false) == false) {
            throw new IllegalArgumentException(
                "analyzing the disk usage of an index is expensive and resource-intensive, " +
                    "the parameter [run_expensive_tasks] must be set to [true] in order for the task to be performed.");
        }
        final String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        final IndicesOptions indicesOptions = IndicesOptions.fromRequest(request, AnalyzeIndexDiskUsageRequest.DEFAULT_INDICES_OPTIONS);
        final boolean flush = request.paramAsBoolean("flush", true);
        final AnalyzeIndexDiskUsageRequest analyzeRequest = new AnalyzeIndexDiskUsageRequest(indices, indicesOptions, flush);
        return channel -> {
            final RestCancellableNodeClient cancelClient = new RestCancellableNodeClient(client, request.getHttpChannel());
            cancelClient.execute(AnalyzeIndexDiskUsageAction.INSTANCE, analyzeRequest, new RestToXContentListener<>(channel));
        };
    }
}
