/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.rest.action.admin.indices.template.get;

import com.google.common.collect.Maps;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesRequest;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestXContentBuilder;

import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestStatus.NOT_FOUND;
import static org.elasticsearch.rest.RestStatus.OK;

/**
 *
 */
public class RestGetIndexTemplateAction extends BaseRestHandler {

    @Inject
    public RestGetIndexTemplateAction(Settings settings, Client client, RestController controller) {
        super(settings, client);

        controller.registerHandler(GET, "/_template", this);
        controller.registerHandler(GET, "/_template/{name}", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        final String[] names = Strings.splitStringByCommaToArray(request.param("name"));

        GetIndexTemplatesRequest getIndexTemplatesRequest = new GetIndexTemplatesRequest(names);
        getIndexTemplatesRequest.listenerThreaded(false);

        final boolean implicitAll = getIndexTemplatesRequest.names().length == 0;

        client.admin().indices().getTemplates(getIndexTemplatesRequest, new ActionListener<GetIndexTemplatesResponse>() {
            @Override
            public void onResponse(GetIndexTemplatesResponse getIndexTemplatesResponse) {
                try {
                    boolean templateExists = getIndexTemplatesResponse.getIndexTemplates().size() > 0;

                    Map<String, String> paramsMap = Maps.newHashMap();
                    paramsMap.put("reduce_mappings", "true");
                    ToXContent.Params params = new ToXContent.DelegatingMapParams(paramsMap, request);

                    XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                    builder.startObject();
                    for (IndexTemplateMetaData indexTemplateMetaData : getIndexTemplatesResponse.getIndexTemplates()) {
                        IndexTemplateMetaData.Builder.toXContent(indexTemplateMetaData, builder, params);
                    }
                    builder.endObject();

                    RestStatus restStatus = (templateExists || implicitAll) ? OK : NOT_FOUND;

                    channel.sendResponse(new XContentRestResponse(request, restStatus, builder));
                } catch (Throwable e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Throwable e) {
                try {
                    channel.sendResponse(new XContentThrowableRestResponse(request, e));
                } catch (Exception e1) {
                    logger.error("Failed to send failure response", e1);
                }
            }
        });
    }
}
