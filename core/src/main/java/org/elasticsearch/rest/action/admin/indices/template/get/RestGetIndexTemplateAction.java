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
package org.elasticsearch.rest.action.admin.indices.template.get;

import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesRequest;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.support.RestBuilderListener;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestStatus.NOT_FOUND;
import static org.elasticsearch.rest.RestStatus.OK;

/**
 *
 */
public class RestGetIndexTemplateAction extends BaseRestHandler {

    @Inject
    public RestGetIndexTemplateAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);

        controller.registerHandler(GET, "/_template", this);
        controller.registerHandler(GET, "/_template/{name}", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        final String[] names = Strings.splitStringByCommaToArray(request.param("name"));

        GetIndexTemplatesRequest getIndexTemplatesRequest = new GetIndexTemplatesRequest(names);
        getIndexTemplatesRequest.local(request.paramAsBoolean("local", getIndexTemplatesRequest.local()));
        getIndexTemplatesRequest.masterNodeTimeout(request.paramAsTime("master_timeout", getIndexTemplatesRequest.masterNodeTimeout()));

        final boolean implicitAll = getIndexTemplatesRequest.names().length == 0;

        client.admin().indices().getTemplates(getIndexTemplatesRequest, new RestBuilderListener<GetIndexTemplatesResponse>(channel) {
            @Override
            public RestResponse buildResponse(GetIndexTemplatesResponse getIndexTemplatesResponse, XContentBuilder builder) throws Exception {
                boolean templateExists = getIndexTemplatesResponse.getIndexTemplates().size() > 0;

                Map<String, String> paramsMap = new HashMap<>();
                paramsMap.put("reduce_mappings", "true");
                ToXContent.Params params = new ToXContent.DelegatingMapParams(paramsMap, request);

                builder.startObject();
                for (IndexTemplateMetaData indexTemplateMetaData : getIndexTemplatesResponse.getIndexTemplates()) {
                    IndexTemplateMetaData.Builder.toXContent(indexTemplateMetaData, builder, params);
                }
                builder.endObject();

                RestStatus restStatus = (templateExists || implicitAll) ? OK : NOT_FOUND;

                return new BytesRestResponse(restStatus, builder);
            }
        });
    }
}
