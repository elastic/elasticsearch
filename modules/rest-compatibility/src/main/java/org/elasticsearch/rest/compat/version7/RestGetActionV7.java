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

package org.elasticsearch.rest.compat.version7;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.Version;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.rest.CompatibleConstants;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.document.RestGetAction;

import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.HEAD;

public class RestGetActionV7 extends RestGetAction {

    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(LogManager.getLogger(RestGetAction.class));
    private static final String TYPES_DEPRECATION_MESSAGE = "[types removal] Specifying types in "
        + "document get requests is deprecated, use the /{index}/_doc/{id} endpoint instead.";
    private static final Consumer<RestRequest> DEPRECATION_WARNING = r -> deprecationLogger.deprecatedAndMaybeLog(
        "get_with_types",
        TYPES_DEPRECATION_MESSAGE
    );

    @Override
    public List<Route> routes() {
        assert Version.CURRENT.major == 8 : "REST API compatibility for version 7 is only supported on version 8";

        return List.of(
            new Route(GET, "/{index}/{type}/{id}"),
            new Route(HEAD, "/{index}/{type}/{id}"));
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, final NodeClient client) throws IOException {
        DEPRECATION_WARNING.accept(request);
        CompatibleHandlers.consumeParameterType(deprecationLogger).accept(request);
        return super.prepareRequest(request, client);
    }

    @Override
    public boolean compatibilityRequired() {
        return true;
    }
}
