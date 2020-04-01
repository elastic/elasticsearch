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

package org.elasticsearch.rest.compat.version7.search;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.Version;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.rest.action.search.RestMultiSearchAction;
import org.elasticsearch.rest.action.search.RestSearchAction;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestMultiSearchActionV7 extends RestMultiSearchAction {
    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(
        LogManager.getLogger(RestMultiSearchAction.class));
    private static final String TYPES_DEPRECATION_MESSAGE = "[types removal]" +
        " Specifying types in multi search requests is deprecated.";

    private static final Set<String> RESPONSE_PARAMS;

    static {
        final Set<String> responseParams = new HashSet<>(
            Arrays.asList(RestSearchAction.TYPED_KEYS_PARAM, RestSearchAction.TOTAL_HITS_AS_INT_PARAM)
        );
        RESPONSE_PARAMS = Collections.unmodifiableSet(responseParams);
    }
    private final boolean allowExplicitIndex;

    public RestMultiSearchActionV7(Settings settings) {
        super(settings);
        this.allowExplicitIndex = MULTI_ALLOW_EXPLICIT_INDEX.get(settings);
    }

    @Override
    public String compatibleWithVersion() {
        return String.valueOf(Version.V_7_0_0.major);
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/_msearch"),
            new Route(POST, "/_msearch"),
            new Route(GET, "/{index}/_msearch"),
            new Route(POST, "/{index}/_msearch"),
            // Deprecated typed endpoints.
            new Route(GET, "/{index}/{type}/_msearch"),
            new Route(POST, "/{index}/{type}/_msearch"));
    }


    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        TypeConsumer typeConsumer = new TypeConsumer();

        MultiSearchRequest multiSearchRequest = parseRequest(request, allowExplicitIndex, typeConsumer);
        if(hasTypes(typeConsumer,request)){
            deprecationLogger.deprecatedAndMaybeLog("msearch_with_types", TYPES_DEPRECATION_MESSAGE);
        }
        return channel -> client.multiSearch(multiSearchRequest, new RestToXContentListener<>(channel));
    }

    private boolean hasTypes(TypeConsumer typeConsumer, RestRequest request) {
        String[] types = Strings.splitStringByCommaToArray(request.param("type"));
        return types.length > 0 || typeConsumer.foundTypeInBody;
    }

    static class TypeConsumer implements Function<String,Boolean> {
        boolean foundTypeInBody = false;
        @Override
        public Boolean apply(String key) {
            if (key.equals("type") || key.equals("types")) {
                foundTypeInBody = true;
                return true;
            }
            return false;
        }
    }
}
