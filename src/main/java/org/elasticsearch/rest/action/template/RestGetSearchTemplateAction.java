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
package org.elasticsearch.rest.action.template;

import com.google.common.collect.Maps;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesRequest;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.template.get.GetSearchTemplatesRequest;
import org.elasticsearch.action.template.get.GetSearchTemplatesRequestBuilder;
import org.elasticsearch.action.template.get.GetSearchTemplatesResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestBuilderListener;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchService;

import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestStatus.NOT_FOUND;
import static org.elasticsearch.rest.RestStatus.OK;

/**
 *
 */
public class RestGetSearchTemplateAction extends BaseRestHandler {

    @Inject
    public RestGetSearchTemplateAction(Settings settings, Client client, RestController controller) {
        super(settings, client);

        //controller.registerHandler(GET, "/template", this);
        controller.registerHandler(GET, "/_search/template/{id}", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        final String[] ids = Strings.splitStringByCommaToArray(request.param("id"));

        final GetSearchTemplatesRequest getSearchTemplatesRequest = new GetSearchTemplatesRequest(ids);
        getSearchTemplatesRequest.local(request.paramAsBoolean("local", getSearchTemplatesRequest.local()));
        getSearchTemplatesRequest.listenerThreaded(false);

        GetSearchTemplatesRequestBuilder requestBuilder = new GetSearchTemplatesRequestBuilder(client, ids[0]);
        requestBuilder.execute(new RestBuilderListener<GetSearchTemplatesResponse>(channel) {
            @Override
            public RestResponse buildResponse(GetSearchTemplatesResponse getSearchTemplatesResponse, XContentBuilder builder) throws Exception {
                builder.startObject();
                RestStatus restStatus;
                if( getSearchTemplatesResponse != null ) {
                    builder.field("template", getSearchTemplatesResponse.getSearchTemplate());
                    restStatus = OK;
                } else {
                    restStatus = NOT_FOUND;
                }
                builder.endObject();
                return new BytesRestResponse(restStatus,builder);
            }
        });

    }
}
