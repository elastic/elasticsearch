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

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesRequest;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.HEAD;
import static org.elasticsearch.rest.RestStatus.NOT_FOUND;
import static org.elasticsearch.rest.RestStatus.OK;

/**
 * The REST handler for get template and head template APIs.
 */
public class RestGetIndexTemplateAction extends BaseRestHandler {

    public RestGetIndexTemplateAction(final Settings settings, final RestController controller) {
        super(settings);
        controller.registerHandler(GET, "/_template", this);
        controller.registerHandler(GET, "/_template/{name}", this);
        controller.registerHandler(HEAD, "/_template/{name}", this);
    }

    @Override
    public String getName() {
        return "get_index_template_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final String[] names = Strings.splitStringByCommaToArray(request.param("name"));

        final GetIndexTemplatesRequest getIndexTemplatesRequest = new GetIndexTemplatesRequest(names);
        getIndexTemplatesRequest.local(request.paramAsBoolean("local", getIndexTemplatesRequest.local()));
        getIndexTemplatesRequest.masterNodeTimeout(request.paramAsTime("master_timeout", getIndexTemplatesRequest.masterNodeTimeout()));

        final boolean implicitAll = getIndexTemplatesRequest.names().length == 0;

        return channel ->
                client.admin()
                        .indices()
                        .getTemplates(getIndexTemplatesRequest, new RestToXContentListener<GetIndexTemplatesResponse>(channel) {
                            @Override
                            protected RestStatus getStatus(final GetIndexTemplatesResponse response) {
                                final boolean templateExists = response.getIndexTemplates().isEmpty() == false;
                                return (templateExists || implicitAll) ? OK : NOT_FOUND;
                            }
                        });
    }

    @Override
    protected Set<String> responseParams() {
        return Settings.FORMAT_PARAMS;
    }

}
