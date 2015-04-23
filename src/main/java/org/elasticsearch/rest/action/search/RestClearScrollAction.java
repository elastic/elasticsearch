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

package org.elasticsearch.rest.action.search;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.rest.action.support.RestStatusToXContentListener;

import java.io.IOException;
import java.util.Arrays;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;

/**
 */
public class RestClearScrollAction extends BaseRestHandler {

    @Inject
    public RestClearScrollAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);

        controller.registerHandler(DELETE, "/_search/scroll", this);
        controller.registerHandler(DELETE, "/_search/scroll/{scroll_id}", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        String scrollIds = request.param("scroll_id");
        ClearScrollRequest clearRequest = new ClearScrollRequest();
        clearRequest.setScrollIds(Arrays.asList(splitScrollIds(scrollIds)));
        if (RestActions.hasBodyContent(request)) {
            XContentType type = RestActions.guessBodyContentType(request);
           if (type == null) {
               scrollIds = RestActions.getRestContent(request).toUtf8();
               clearRequest.setScrollIds(Arrays.asList(splitScrollIds(scrollIds)));
           } else {
               // NOTE: if rest request with xcontent body has request parameters, these parameters does not override xcontent value
               clearRequest.setScrollIds(null);
               buildFromContent(RestActions.getRestContent(request), clearRequest);
           }
        }

        client.clearScroll(clearRequest, new RestStatusToXContentListener<ClearScrollResponse>(channel));
    }

    public static String[] splitScrollIds(String scrollIds) {
        if (scrollIds == null) {
            return Strings.EMPTY_ARRAY;
        }
        return Strings.splitStringByCommaToArray(scrollIds);
    }

    public static void buildFromContent(BytesReference content, ClearScrollRequest clearScrollRequest) throws ElasticsearchIllegalArgumentException {
        try (XContentParser parser = XContentHelper.createParser(content)) {
            if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                throw new ElasticsearchIllegalArgumentException("Malformed content, must start with an object");
            } else {
                XContentParser.Token token;
                String currentFieldName = null;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if ("scroll_id".equals(currentFieldName) && token == XContentParser.Token.START_ARRAY) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            if (token.isValue() == false) {
                                throw new ElasticsearchIllegalArgumentException("scroll_id array element should only contain scroll_id");
                            }
                            clearScrollRequest.addScrollId(parser.text());
                        }
                    } else {
                        throw new ElasticsearchIllegalArgumentException("Unknown parameter [" + currentFieldName + "] in request body or parameter is of the wrong type[" + token + "] ");
                    }
                }
            }
        } catch (IOException e) {
            throw new ElasticsearchIllegalArgumentException("Failed to parse request body", e);
        }
    }

}
