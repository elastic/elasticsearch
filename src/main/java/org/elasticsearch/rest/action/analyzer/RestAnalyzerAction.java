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

package org.elasticsearch.rest.action.analyzer;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.analysis.BuiltinAsCustom;
import org.elasticsearch.rest.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * Rest action exposing json to build builtin analyzers as custom analyzer. To
 * be used for customizing builtin analyzers.
 */
public class RestAnalyzerAction extends BaseRestHandler {
    private final Set<BuiltinAsCustom> builtinAsCustoms;

    @Inject
    public RestAnalyzerAction(Settings settings, Client client, RestController controller, Set<BuiltinAsCustom> builtinAsCustoms) {
        super(settings, client);
        this.builtinAsCustoms = builtinAsCustoms;
        controller.registerHandler(GET, "/_analyzer/{name}", this);
    }

    @Override
    public void handleRequest(RestRequest request, RestChannel channel) throws Exception {
        XContentType xContentType = XContentType.fromRestContentType(request.param("format", request.header("Content-Type")));
        if (xContentType == null) {
            xContentType = XContentType.JSON;
        }
        
        Map<String, Object> result = new HashMap<String, Object>();
        for (String name: Strings.splitStringByCommaToArray(request.param("name"))) {
            name = name.trim();
            XContentBuilder builder = XContentBuilder.builder(xContentType.xContent());
            builder.startObject();
            boolean found = false;
            for (BuiltinAsCustom builtinAsCustom: builtinAsCustoms) {
                try {
                    if (builtinAsCustom.build(builder, name)) {
                        found = true;
                        break;
                    }
                } catch (IllegalArgumentException e) {
                    channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, e.getMessage()));
                    return;
                }
            }
            if (!found) {
                channel.sendResponse(new BytesRestResponse(RestStatus.NOT_FOUND, "Could not find configuration to build " + name
                        + " as a custom analyzer."));
            }
            builder.endObject();
            XContentHelper.update(result, XContentHelper.convertToMap(builder.bytes(), false).v2());
        }
        XContentBuilder builder = XContentBuilder.builder(xContentType.xContent());
        channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder.map(result)));
    }
}
