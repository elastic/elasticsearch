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

package org.elasticsearch.rest.action.admin.indices.seal;

import org.elasticsearch.action.admin.indices.seal.SealIndicesAction;
import org.elasticsearch.action.admin.indices.seal.SealIndicesRequest;
import org.elasticsearch.action.admin.indices.seal.SealIndicesResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestBuilderListener;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 *
 */
public class RestSealIndicesAction extends BaseRestHandler {

    @Inject
    public RestSealIndicesAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(POST, "/_seal", this);
        controller.registerHandler(POST, "/{index}/_seal", this);

        controller.registerHandler(GET, "/_seal", this);
        controller.registerHandler(GET, "/{index}/_seal", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        SealIndicesRequest sealIndicesRequest = new SealIndicesRequest(indices);
        client.admin().indices().execute(SealIndicesAction.INSTANCE, sealIndicesRequest, new RestBuilderListener<SealIndicesResponse>(channel) {
            @Override
            public RestResponse buildResponse(SealIndicesResponse response, XContentBuilder builder) throws Exception {
                builder.startObject();
                builder = response.toXContent(builder, ToXContent.EMPTY_PARAMS);
                builder.endObject();
                return new BytesRestResponse(response.status(), builder);
            }
        });
    }
}
