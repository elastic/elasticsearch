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

package org.elasticsearch.rest.action.termvectors;

import org.elasticsearch.action.termvectors.MultiTermVectorsRequest;
import org.elasticsearch.action.termvectors.MultiTermVectorsResponse;
import org.elasticsearch.action.termvectors.TermVectorsRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.rest.action.support.RestToXContentListener;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestMultiTermVectorsAction extends BaseRestHandler {

    @Inject
    public RestMultiTermVectorsAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(GET, "/_mtermvectors", this);
        controller.registerHandler(POST, "/_mtermvectors", this);
        controller.registerHandler(GET, "/{index}/_mtermvectors", this);
        controller.registerHandler(POST, "/{index}/_mtermvectors", this);
        controller.registerHandler(GET, "/{index}/{type}/_mtermvectors", this);
        controller.registerHandler(POST, "/{index}/{type}/_mtermvectors", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) throws Exception {
        MultiTermVectorsRequest multiTermVectorsRequest = new MultiTermVectorsRequest();
        multiTermVectorsRequest.listenerThreaded(false);
        TermVectorsRequest template = new TermVectorsRequest();
        template.index(request.param("index"));
        template.type(request.param("type"));
        RestTermVectorsAction.readURIParameters(template, request);
        multiTermVectorsRequest.ids(Strings.commaDelimitedListToStringArray(request.param("ids")));
        multiTermVectorsRequest.add(template, RestActions.getRestContent(request));

        client.multiTermVectors(multiTermVectorsRequest, new RestToXContentListener<MultiTermVectorsResponse>(channel));
    }
}
