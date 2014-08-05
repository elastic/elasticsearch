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

package org.elasticsearch.rest.action.benchmark.abort;

import org.elasticsearch.action.benchmark.abort.BenchmarkAbortRequest;
import org.elasticsearch.action.benchmark.abort.BenchmarkAbortResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestBuilderListener;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestStatus.OK;

/**
 * REST handler for benchmark status actions.
 */
public class RestBenchmarkAbortAction extends BaseRestHandler {

    @Inject
    public RestBenchmarkAbortAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(POST, "/_bench/_abort/{name}", this);
    }

    /**
     * Aborts actively running benchmark(s)
     */
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {

        final String[] benchmarkNames = Strings.splitStringByCommaToArray(request.param("name"));
        final BenchmarkAbortRequest benchmarkAbortRequest = new BenchmarkAbortRequest(benchmarkNames);

        client.abortBench(benchmarkAbortRequest, new RestBuilderListener<BenchmarkAbortResponse>(channel) {

            @Override
            public RestResponse buildResponse(BenchmarkAbortResponse response, XContentBuilder builder) throws Exception {
                builder.startObject();
                response.toXContent(builder, request);
                builder.endObject();
                return new BytesRestResponse(OK, builder);
            }
        });
    }
}
