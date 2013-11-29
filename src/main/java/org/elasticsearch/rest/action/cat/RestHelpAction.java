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

package org.elasticsearch.rest.action.cat;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Guice;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.*;

import static org.elasticsearch.rest.RestRequest.Method.GET;


public class RestHelpAction extends BaseRestHandler {

    @Inject
    public RestHelpAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(GET, "/_cat/help", this);
        controller.registerHandler(GET, "/_cat/halp", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        // Maybe build this list from a classloader or the RestActionModule injector

        StringBuilder s = new StringBuilder();
        s.append("Try:\n\n");
        s.append("/_cat/allocation\n");
        s.append("/_cat/count\n");
        s.append("/_cat/count/{index}\n");
        s.append("/_cat/health\n");
        s.append("/_cat/indices\n");
        s.append("/_cat/indices/{index}\n");
        s.append("/_cat/master\n");
        s.append("/_cat/nodes\n");
        s.append("/_cat/pending_tasks\n");
        s.append("/_cat/recovery\n");
        s.append("/_cat/shards\n");
        s.append("/_cat/shards/{index}\n");
        channel.sendResponse(new StringRestResponse(RestStatus.OK, s.toString()));
    }
}
