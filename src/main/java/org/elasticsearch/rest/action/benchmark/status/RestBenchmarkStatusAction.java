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

package org.elasticsearch.rest.action.benchmark.status;

import org.elasticsearch.action.benchmark.status.*;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestBuilderListener;

import static org.elasticsearch.rest.RestRequest.Method.*;
import static org.elasticsearch.rest.RestStatus.*;

/**
 * REST handler for benchmark status actions.
 */
public class RestBenchmarkStatusAction extends BaseRestHandler {

    @Inject
    public RestBenchmarkStatusAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(GET, "/_bench/_status", this);
        controller.registerHandler(GET, "/_bench/_status/{name}", this);
    }

    /**
     * Reports on the status of all actively running benchmarks
     */
    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {

        final String[] benchmarkNames = Strings.splitStringByCommaToArray(request.param("name"));
        BenchmarkStatusRequest benchmarkStatusRequest = new BenchmarkStatusRequest(benchmarkNames);

        client.benchmarkStatus(benchmarkStatusRequest, new RestBuilderListener<BenchmarkStatusResponses>(channel) {

            @Override
            public RestResponse buildResponse(BenchmarkStatusResponses response, XContentBuilder builder) throws Exception {
                builder.startObject();
                response.toXContent(builder, request);
                builder.endObject();
                return new BytesRestResponse(OK, builder);
            }
        });
    }
}
