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

package org.elasticsearch.rest.action.bench;

import org.elasticsearch.action.bench.BenchmarkStatusRequest;
import org.elasticsearch.action.bench.BenchmarkStatusResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseActionRequestRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.support.RestToXContentListener;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * REST handler for benchmarks status action.
 */
public class RestBenchStatusAction extends BaseActionRequestRestHandler<BenchmarkStatusRequest> {

    @Inject
    public RestBenchStatusAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(GET, "/_bench", this);
        controller.registerHandler(GET, "/{index}/_bench", this);
        controller.registerHandler(GET, "/{index}/{type}/_bench", this);
    }

    @Override
    protected BenchmarkStatusRequest newRequest(RestRequest request) {
        //Reports on the status of all actively running benchmarks
        return new BenchmarkStatusRequest();
    }

    @Override
    protected void doHandleRequest(RestRequest restRequest, RestChannel restChannel, BenchmarkStatusRequest request) {
        client.benchStatus(request, new RestToXContentListener<BenchmarkStatusResponse> (restChannel));
    }
}
