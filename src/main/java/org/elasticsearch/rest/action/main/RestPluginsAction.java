/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
package org.elasticsearch.rest.action.main;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.internal.InternalSettingsPerparer;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.XContentRestResponse;
import org.elasticsearch.rest.XContentThrowableRestResponse;
import org.elasticsearch.rest.action.support.RestXContentBuilder;

public class RestPluginsAction extends BaseRestHandler {

    @Inject
    public RestPluginsAction(Settings settings, Client client, RestController controller) {
        super(settings, client);

        controller.registerHandler(RestRequest.Method.GET, "/_plugins", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        Tuple<Settings, Environment> tuple = InternalSettingsPerparer.prepareSettings(settings, true);
        final PluginsService pluginsService = new PluginsService(tuple.v1(), tuple.v2());
        try {
            RestStatus status = RestStatus.OK;
            XContentBuilder builder = RestXContentBuilder.restContentBuilder(request).prettyPrint();
            builder.startObject().field("ok", true).startObject("plugins");
            ImmutableMap<String,Plugin> plugins = pluginsService.plugins();
            for (String name : plugins.keySet()) {                
                builder.field(name, plugins.get(name).description());
            }
            builder.endObject().endObject();
            channel.sendResponse(new XContentRestResponse(request, status, builder));
        } catch (Exception e) {
            try {
                channel.sendResponse(new XContentThrowableRestResponse(request, e));
            } catch (IOException e1) {
                logger.warn("Failed to send response", e);
            }
        }
    }
}
