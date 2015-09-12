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

import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequest;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.rest.action.support.RestToXContentListener;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

        String[] texts = request.paramAsStringArrayOrEmptyIfAll("text");

        AnalyzeRequest analyzeRequest = new AnalyzeRequest(request.param("index"));
        analyzeRequest.text(texts);
        analyzeRequest.analyzer(request.param("analyzer"));
        analyzeRequest.field(request.param("field"));
        analyzeRequest.tokenizer(request.param("tokenizer"));
        analyzeRequest.tokenFilters(request.paramAsStringArray("token_filters", request.paramAsStringArray("filters", analyzeRequest.tokenFilters())));
        analyzeRequest.charFilters(request.paramAsStringArray("char_filters", analyzeRequest.charFilters()));

        if (RestActions.hasBodyContent(request)) {
            XContentType type = RestActions.guessBodyContentType(request);
            if (type == null) {
                if (texts == null || texts.length == 0) {
                    texts = new String[]{ RestActions.getRestContent(request).toUtf8() };
                    analyzeRequest.text(texts);
                }
            } else {
                // NOTE: if rest request with xcontent body has request parameters, the parameters does not override xcontent values
                buildFromContent(RestActions.getRestContent(request), analyzeRequest);
            }
        }

        client.admin().indices().analyze(analyzeRequest, new RestToXContentListener<AnalyzeResponse>(channel));
    }

    public static void buildFromContent(BytesReference content, AnalyzeRequest analyzeRequest) {
        try (XContentParser parser = XContentHelper.createParser(content)) {
            if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                throw new IllegalArgumentException("Malforrmed content, must start with an object");
            } else {
                XContentParser.Token token;
                String currentFieldName = null;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if ("text".equals(currentFieldName) && token == XContentParser.Token.VALUE_STRING) {
                        analyzeRequest.text(parser.text());
                    } else if ("text".equals(currentFieldName) && token == XContentParser.Token.START_ARRAY) {
                        List<String> texts = new ArrayList<>();
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            if (token.isValue() == false) {
                                throw new IllegalArgumentException(currentFieldName + " array element should only contain text");
                            }
                            texts.add(parser.text());
                        }
                        analyzeRequest.text(texts.toArray(Strings.EMPTY_ARRAY));
                    } else if ("analyzer".equals(currentFieldName) && token == XContentParser.Token.VALUE_STRING) {
                        analyzeRequest.analyzer(parser.text());
                    } else if ("field".equals(currentFieldName) && token == XContentParser.Token.VALUE_STRING) {
                        analyzeRequest.field(parser.text());
                    } else if ("tokenizer".equals(currentFieldName) && token == XContentParser.Token.VALUE_STRING) {
                        analyzeRequest.tokenizer(parser.text());
                    } else if (("token_filters".equals(currentFieldName) || "filters".equals(currentFieldName)) && token == XContentParser.Token.START_ARRAY) {
                        List<String> filters = new ArrayList<>();
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            if (token.isValue() == false) {
                                throw new IllegalArgumentException(currentFieldName + " array element should only contain token filter's name");
                            }
                            filters.add(parser.text());
                        }
                        analyzeRequest.tokenFilters(filters.toArray(Strings.EMPTY_ARRAY));
                    } else if ("char_filters".equals(currentFieldName) && token == XContentParser.Token.START_ARRAY) {
                        List<String> charFilters = new ArrayList<>();
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            if (token.isValue() == false) {
                                throw new IllegalArgumentException(currentFieldName + " array element should only contain char filter's name");
                            }
                            charFilters.add(parser.text());
                        }
                        analyzeRequest.tokenFilters(charFilters.toArray(Strings.EMPTY_ARRAY));
                    } else {
                        throw new IllegalArgumentException("Unknown parameter [" + currentFieldName + "] in request body or parameter is of the wrong type[" + token + "] ");
                    }
                }
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to parse request body", e);
        }
    }
}
