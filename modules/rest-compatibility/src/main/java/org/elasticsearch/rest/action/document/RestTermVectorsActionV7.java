/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.rest.action.document;

import org.elasticsearch.Version;
import org.elasticsearch.action.termvectors.TermVectorsRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.compat.TypeConsumer;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestTermVectorsActionV7 extends RestTermVectorsAction {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestTermVectorsActionV7.class);
    public static final String TYPES_DEPRECATION_MESSAGE = "[types removal] " + "Specifying types in term vector requests is deprecated.";

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/{index}/_termvectors"),
            new Route(POST, "/{index}/_termvectors"),
            new Route(GET, "/{index}/_termvectors/{id}"),
            new Route(POST, "/{index}/_termvectors/{id}"),
            // Deprecated typed endpoints.
            new Route(GET, "/{index}/{type}/_termvectors"),
            new Route(POST, "/{index}/{type}/_termvectors"),
            new Route(GET, "/{index}/{type}/{id}/_termvectors"),
            new Route(POST, "/{index}/{type}/{id}/_termvectors")
        );
    }

    @Override
    public String getName() {
        return super.getName() + "_v7";
    }

    @Override
    public Version compatibleWithVersion() {
        return Version.V_7_0_0;
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        TypeConsumer typeConsumer = new TypeConsumer(request, "_type");

        TermVectorsRequest termVectorsRequest = new TermVectorsRequest(request.param("index"), request.param("id"));

        if (request.hasContentOrSourceParam()) {
            try (XContentParser parser = request.contentOrSourceParamParser()) {
                TermVectorsRequest.parseRequest(termVectorsRequest, parser, typeConsumer);
            }
        }
        readURIParameters(termVectorsRequest, request);

        if (typeConsumer.hasTypes()) {
            request.param("type");
            deprecationLogger.deprecate("termvectors_with_types", TYPES_DEPRECATION_MESSAGE);
        }

        return channel -> client.termVectors(termVectorsRequest, new RestToXContentListener<>(channel));
    }

}
