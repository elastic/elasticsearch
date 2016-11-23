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

import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.rest.action.RestStatusToXContentListener;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestClearScrollAction extends BaseRestHandler {

    @Inject
    public RestClearScrollAction(Settings settings, RestController controller) {
        super(settings);
        //the DELETE endpoints are both deprecated
        controller.registerHandler(DELETE, "/_search/scroll", this);
        controller.registerHandler(DELETE, "/_search/scroll/{scroll_id}", this);
        controller.registerHandler(POST, "/_search/clear_scroll", this);
        //the POST endpoint that accepts scroll_id in the url is deprecated too
        controller.registerHandler(POST, "/_search/clear_scroll/{scroll_id}", this);
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        if (request.method() == DELETE) {
            deprecationLogger.deprecated("Deprecated [DELETE /_search/scroll] endpoint used, expected [POST /_search/scroll] instead");
        }
        if (request.method() == POST && request.hasParam("scroll_id")) {
            deprecationLogger.deprecated("Deprecated [POST /_search/clear_scroll/{scroll_id}] endpoint used, " +
                    "expected [POST /_search/clear_scroll] instead. The scroll_id must be provided as part of the request body");
        }
        final ClearScrollRequest clearRequest;
        if (RestActions.hasBodyContent(request)) {
            //consume the scroll_id param, it will get ignored though (bw comp)
            request.param("scroll_id");
            XContentType type = RestActions.guessBodyContentType(request);
            if (type == null) {
                String scrollIds = RestActions.getRestContent(request).utf8ToString();
                clearRequest = new ClearScrollRequest(splitScrollIds(scrollIds));
            } else {
                clearRequest = parseClearScrollRequest(RestActions.getRestContent(request));
            }
        } else {
            clearRequest = new ClearScrollRequest(splitScrollIds(request.param("scroll_id")));
        }

        return channel -> client.clearScroll(clearRequest, new RestStatusToXContentListener<>(channel));
    }

    private static List<String> splitScrollIds(String scrollIds) {
        if (scrollIds == null) {
            return Collections.emptyList();
        }
        return Arrays.asList(Strings.splitStringByCommaToArray(scrollIds));
    }

    public static ClearScrollRequest parseClearScrollRequest(BytesReference content) {
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        try (XContentParser parser = XContentHelper.createParser(content)) {
            if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                throw new IllegalArgumentException("Malformed content, must start with an object");
            } else {
                XContentParser.Token token;
                String currentFieldName = null;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if ("scroll_id".equals(currentFieldName) && token == XContentParser.Token.START_ARRAY) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            if (token.isValue() == false) {
                                throw new IllegalArgumentException("scroll_id array element should only contain scroll_id");
                            }
                            clearScrollRequest.addScrollId(parser.text());
                        }
                    } else {
                        throw new IllegalArgumentException("Unknown parameter [" + currentFieldName
                                + "] in request body or parameter is of the wrong type[" + token + "] ");
                    }
                }
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to parse request body", e);
        }
        return clearScrollRequest;
    }
}
