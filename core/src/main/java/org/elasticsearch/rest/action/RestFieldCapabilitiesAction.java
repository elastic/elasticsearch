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

package org.elasticsearch.rest.action;

import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestStatus.OK;

public class RestFieldCapabilitiesAction extends BaseRestHandler {
    public RestFieldCapabilitiesAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(GET, "/_field_caps", this);
        controller.registerHandler(POST, "/_field_caps", this);
        controller.registerHandler(GET, "/{index}/_field_caps", this);
        controller.registerHandler(POST, "/{index}/_field_caps", this);
    }

    @Override
    public String getName() {
        return "field_capabilities_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request,
                                              final NodeClient client) throws IOException {
        if (request.hasContentOrSourceParam() && request.hasParam("fields")) {
            throw new IllegalArgumentException("can't specify a request body and [fields]" +
                " request parameter, either specify a request body or the" +
                " [fields] request parameter");
        }
        final String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        final FieldCapabilitiesRequest fieldRequest;
        if (request.hasContentOrSourceParam()) {
            try (XContentParser parser = request.contentOrSourceParamParser()) {
                fieldRequest = FieldCapabilitiesRequest.parseFields(parser);
            }
        } else {
            fieldRequest = new FieldCapabilitiesRequest();
            fieldRequest.fields(Strings.splitStringByCommaToArray(request.param("fields")));
        }
        fieldRequest.indices(indices);
        fieldRequest.indicesOptions(
            IndicesOptions.fromRequest(request, fieldRequest.indicesOptions())
        );
        return channel -> client.fieldCaps(fieldRequest,
            new RestBuilderListener<FieldCapabilitiesResponse>(channel) {
            @Override
            public RestResponse buildResponse(FieldCapabilitiesResponse response,
                                              XContentBuilder builder) throws Exception {
                RestStatus status = OK;
                builder.startObject();
                response.toXContent(builder, request);
                builder.endObject();
                return new BytesRestResponse(status, builder);
            }
        });
    }
}
