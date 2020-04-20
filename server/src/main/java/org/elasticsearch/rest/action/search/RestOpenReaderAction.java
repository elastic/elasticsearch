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

import org.elasticsearch.action.search.OpenReaderRequest;
import org.elasticsearch.action.search.TransportOpenReaderAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestOpenReaderAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "open_reader_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/{index}/_open_reader"));
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        final IndicesOptions indicesOptions = IndicesOptions.fromRequest(request, OpenReaderRequest.DEFAULT_INDICES_OPTIONS);
        final String routing = request.param("routing");
        final String preference = request.param("preference");
        final TimeValue keepAlive = TimeValue.parseTimeValue(request.param("keep_alive"), null, "keep_alive");
        final OpenReaderRequest openReaderRequest = new OpenReaderRequest(indices, indicesOptions, keepAlive, routing, preference);
        return channel -> client.execute(TransportOpenReaderAction.INSTANCE, openReaderRequest, new RestToXContentListener<>(channel));
    }
}
