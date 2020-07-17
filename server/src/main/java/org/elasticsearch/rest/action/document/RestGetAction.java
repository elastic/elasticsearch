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

package org.elasticsearch.rest.action.document;

import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.HEAD;
import static org.elasticsearch.rest.RestStatus.NOT_FOUND;
import static org.elasticsearch.rest.RestStatus.OK;

public class RestGetAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "document_get_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/{index}/_doc/{id}"),
            new Route(HEAD, "/{index}/_doc/{id}"));
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        GetRequest getRequest = new GetRequest(request.param("index"), request.param("id"));

        getRequest.refresh(request.paramAsBoolean("refresh", getRequest.refresh()));
        getRequest.routing(request.param("routing"));
        getRequest.preference(request.param("preference"));
        getRequest.realtime(request.paramAsBoolean("realtime", getRequest.realtime()));
        if (request.param("fields") != null) {
            throw new IllegalArgumentException("the parameter [fields] is no longer supported, " +
                "please use [stored_fields] to retrieve stored fields or [_source] to load the field from _source");
        }
        final String fieldsParam = request.param("stored_fields");
        if (fieldsParam != null) {
            final String[] fields = Strings.splitStringByCommaToArray(fieldsParam);
            if (fields != null) {
                getRequest.storedFields(fields);
            }
        }

        getRequest.version(RestActions.parseVersion(request));
        getRequest.versionType(VersionType.fromString(request.param("version_type"), getRequest.versionType()));

        getRequest.fetchSourceContext(FetchSourceContext.parseFromRestRequest(request));

        return channel -> client.get(getRequest, new RestToXContentListener<GetResponse>(channel) {
            @Override
            protected RestStatus getStatus(final GetResponse response) {
                return response.isExists() ? OK : NOT_FOUND;
            }
        });
    }

}
