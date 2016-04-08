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

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.rest.action.support.RestStatusToXContentListener;
import org.elasticsearch.search.Scroll;

import java.io.IOException;

import static org.elasticsearch.common.unit.TimeValue.parseTimeValue;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 *
 */
public class RestSearchScrollAction extends BaseRestHandler {

    @Inject
    public RestSearchScrollAction(Settings settings, RestController controller, Client client) {
        super(settings, client);

        controller.registerHandler(GET, "/_search/scroll", this);
        controller.registerHandler(POST, "/_search/scroll", this);
        controller.registerHandler(GET, "/_search/scroll/{scroll_id}", this);
        controller.registerHandler(POST, "/_search/scroll/{scroll_id}", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        String scrollId = request.param("scroll_id");
        SearchScrollRequest searchScrollRequest = new SearchScrollRequest();
        searchScrollRequest.scrollId(scrollId);
        String scroll = request.param("scroll");
        if (scroll != null) {
            searchScrollRequest.scroll(new Scroll(parseTimeValue(scroll, null, "scroll")));
        }

        if (RestActions.hasBodyContent(request)) {
            XContentType type = XContentFactory.xContentType(RestActions.getRestContent(request));
            if (type == null) {
                if (scrollId == null) {
                    scrollId = RestActions.getRestContent(request).toUtf8();
                    searchScrollRequest.scrollId(scrollId);
                }
            } else {
                // NOTE: if rest request with xcontent body has request parameters, these parameters override xcontent values
                buildFromContent(RestActions.getRestContent(request), searchScrollRequest);
            }
        }
        client.searchScroll(searchScrollRequest, new RestStatusToXContentListener<SearchResponse>(channel));
    }

    public static void buildFromContent(BytesReference content, SearchScrollRequest searchScrollRequest) {
        try (XContentParser parser = XContentHelper.createParser(content)) {
            if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                throw new IllegalArgumentException("Malformed content, must start with an object");
            } else {
                XContentParser.Token token;
                String currentFieldName = null;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if ("scroll_id".equals(currentFieldName) && token == XContentParser.Token.VALUE_STRING) {
                        searchScrollRequest.scrollId(parser.text());
                    } else if ("scroll".equals(currentFieldName) && token == XContentParser.Token.VALUE_STRING) {
                        searchScrollRequest.scroll(new Scroll(TimeValue.parseTimeValue(parser.text(), null, "scroll")));
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
