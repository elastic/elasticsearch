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

package org.elasticsearch.legacy.rest.action.search;

import org.elasticsearch.legacy.action.search.ClearScrollRequest;
import org.elasticsearch.legacy.action.search.ClearScrollResponse;
import org.elasticsearch.legacy.client.Client;
import org.elasticsearch.legacy.common.Strings;
import org.elasticsearch.legacy.common.inject.Inject;
import org.elasticsearch.legacy.common.settings.Settings;
import org.elasticsearch.legacy.rest.BaseRestHandler;
import org.elasticsearch.legacy.rest.RestChannel;
import org.elasticsearch.legacy.rest.RestController;
import org.elasticsearch.legacy.rest.RestRequest;
import org.elasticsearch.legacy.rest.action.support.RestActions;
import org.elasticsearch.legacy.rest.action.support.RestStatusToXContentListener;

import java.util.Arrays;

import static org.elasticsearch.legacy.rest.RestRequest.Method.DELETE;

/**
 */
public class RestClearScrollAction extends BaseRestHandler {

    @Inject
    public RestClearScrollAction(Settings settings, Client client, RestController controller) {
        super(settings, client);

        controller.registerHandler(DELETE, "/_search/scroll", this);
        controller.registerHandler(DELETE, "/_search/scroll/{scroll_id}", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        String scrollIds = request.param("scroll_id");
        if (scrollIds == null) {
            scrollIds = RestActions.getRestContent(request).toUtf8();
        }

        ClearScrollRequest clearRequest = new ClearScrollRequest();
        clearRequest.setScrollIds(Arrays.asList(splitScrollIds(scrollIds)));
        client.clearScroll(clearRequest, new RestStatusToXContentListener<ClearScrollResponse>(channel));
    }

    public static String[] splitScrollIds(String scrollIds) {
        if (scrollIds == null) {
            return Strings.EMPTY_ARRAY;
        }
        return Strings.splitStringByCommaToArray(scrollIds);
    }
}
