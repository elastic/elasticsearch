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
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollAction;
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
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.rest.action.support.RestStatusToXContentListener;
import org.elasticsearch.search.Scroll;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.common.unit.TimeValue.parseTimeValue;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 *
 */
public class RestSearchScrollAction extends BaseRestHandler {

    @Inject
    public RestSearchScrollAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);

        controller.registerHandler(GET, "/_search/scroll", this);
        controller.registerHandler(POST, "/_search/scroll", this);
        controller.registerHandler(GET, "/_search/scroll/{scroll_id}", this);
        controller.registerHandler(POST, "/_search/scroll/{scroll_id}", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        String scrollId = request.param("scroll_id");
        SearchScrollRequest searchScrollRequest = new SearchScrollRequest();
        if (scrollId == null && request.hasContent()) {
            XContentType type = XContentFactory.xContentType(request.content());
            if (type == null) {
                scrollId = RestActions.getRestContent(request).toUtf8();
            } else {
                // NOTE: if rest request with xcontent body has request parameters, these parameters override xcontent values
                searchScrollRequest = buildFromContent(request.content(), searchScrollRequest);
            }
        }

        searchScrollRequest.listenerThreaded(false);
        searchScrollRequest.scrollId(scrollId);
        String scroll = request.param("scroll");
        if (scroll != null) {
            searchScrollRequest.scroll(new Scroll(parseTimeValue(scroll, null)));
        }

        client.searchScroll(searchScrollRequest, new RestStatusToXContentListener<SearchResponse>(channel));
    }

    public static SearchScrollRequest buildFromContent(BytesReference content, SearchScrollRequest searchScrollRequest) throws ElasticsearchIllegalArgumentException {
        try (XContentParser parser = XContentHelper.createParser(content)) {
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.VALUE_STRING) {
                    if ("scroll_id".equals(currentFieldName)) {
                        searchScrollRequest.scrollId(parser.text());
                    } else if ("scroll".equals(currentFieldName)) {
                        searchScrollRequest.scroll(new Scroll(TimeValue.parseTimeValue(parser.text(), null)));
                    }
                }
            }
        } catch (IOException e) {
            throw new ElasticsearchIllegalArgumentException("Failed to parse request body", e);
        }

        return searchScrollRequest;
    }

}
