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
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestFieldCapabilitiesAction extends BaseRestHandler {

    public RestFieldCapabilitiesAction(RestController controller) {
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
        String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        FieldCapabilitiesRequest fieldRequest = new FieldCapabilitiesRequest()
            .fields(Strings.splitStringByCommaToArray(request.param("fields")))
            .indices(indices);

        fieldRequest.indicesOptions(
            IndicesOptions.fromRequest(request, fieldRequest.indicesOptions()));
        fieldRequest.includeUnmapped(request.paramAsBoolean("include_unmapped", false));
        return channel -> client.fieldCaps(fieldRequest, new RestToXContentListener<>(channel));
    }
}
