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
package org.elasticsearch.rest.action.admin.indices.analyze;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequest;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.rest.action.support.RestToXContentListener;
import org.elasticsearch.rest.support.RestUtils;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 *
 */
public class RestAnalyzeAction extends BaseRestHandler {

    @Inject
    public RestAnalyzeAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(GET, "/_analyze", this);
        controller.registerHandler(GET, "/{index}/_analyze", this);
        controller.registerHandler(POST, "/_analyze", this);
        controller.registerHandler(POST, "/{index}/_analyze", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        String text = request.param("text");
        AnalyzeRequest analyzeRequest = new AnalyzeRequest(request.param("index"), text);

        if (text == null && request.hasContent()) {
            XContentType type = XContentFactory.xContentType(request.content());
            if (type == null) {
                text = request.content().toUtf8();
            } else {
                // NOTE: if rest request with xcontent body has request parameters, these parameters override xcontent values
                analyzeRequest = buildFromContent(request.content(), analyzeRequest);
            }
        }

        analyzeRequest.listenerThreaded(false);
        analyzeRequest.text(text);
        analyzeRequest.preferLocal(request.paramAsBoolean("prefer_local", analyzeRequest.preferLocalShard()));
        analyzeRequest.analyzer(request.param("analyzer"));
        analyzeRequest.field(request.param("field"));
        analyzeRequest.tokenizer(request.param("tokenizer"));
        analyzeRequest.tokenFilters(request.paramAsStringArray("token_filters", request.paramAsStringArray("filters", analyzeRequest.tokenFilters())));
        analyzeRequest.charFilters(request.paramAsStringArray("char_filters", analyzeRequest.charFilters()));

        if (analyzeRequest.text() == null) {
            throw new ElasticsearchIllegalArgumentException("text is missing");
        }

        client.admin().indices().analyze(analyzeRequest, new RestToXContentListener<AnalyzeResponse>(channel));
    }


    public static AnalyzeRequest buildFromContent(BytesReference content, AnalyzeRequest analyzeRequest) {

        try {
            Map<String, Object> contentMap = XContentHelper.convertToMap(content, false).v2();
            for (Map.Entry<String, Object> entry : contentMap.entrySet()) {
                String name = entry.getKey();
                if ("prefer_local".equals(name)) {
                    analyzeRequest.preferLocal(XContentMapValues.nodeBooleanValue(entry.getValue(), analyzeRequest.preferLocalShard()));
                } else if ("analyzer".equals(name)) {
                    analyzeRequest.analyzer(XContentMapValues.nodeStringValue(entry.getValue(), null));
                } else if ("field".equals(name)) {
                    analyzeRequest.field(XContentMapValues.nodeStringValue(entry.getValue(), null));
                } else if ("tokenizer".equals(name)) {
                    analyzeRequest.tokenizer(XContentMapValues.nodeStringValue(entry.getValue(), null));
                } else if ("token_filters".equals(name) || "filters".equals(name)) {
                    if (XContentMapValues.isArray(entry.getValue())) {
                        analyzeRequest.tokenFilters(((List<String>) entry.getValue()).toArray(new String[0]));
                    }
                } else if ("char_filters".equals(name)) {
                    if (XContentMapValues.isArray(entry.getValue())) {
                        analyzeRequest.charFilters(((List<String>) entry.getValue()).toArray(new String[0]));
                    }
                } else if ("text".equals(name)) {
                    analyzeRequest.text(XContentMapValues.nodeStringValue(entry.getValue(), null));
                }
            }
        } catch (ElasticsearchParseException e) {
            throw new ElasticsearchIllegalArgumentException("Failed to parse request body", e);
        }
        if (analyzeRequest.text() == null) {
            throw new ElasticsearchIllegalArgumentException("text is missing");
        }
        return analyzeRequest;
    }





}
