/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.plugin.tasks;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RestTestAction extends BaseRestHandler {
    @Override
    public List<Route> routes() {
        return Collections.singletonList(new Route(RestRequest.Method.POST, "/_test_blocking"));
    }

    @Override
    public String getName() {
        return "test_blocking";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        final String id = Objects.requireNonNull(request.param("id"));
        final Set<TestRequest.Target> targets = Stream.of(request.paramAsStringArray("targets", new String[0])).map(s -> {
            final String[] parts = s.split(":");
            return new TestRequest.Target(parts[0], parts[1]);
        }).collect(Collectors.toSet());
        return chanel -> client.execute(TransportTestAction.ACTION, new TestRequest(id, targets), new RestToXContentListener<>(chanel));
    }
}
