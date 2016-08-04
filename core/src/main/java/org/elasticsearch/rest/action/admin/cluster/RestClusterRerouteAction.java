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

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommandRegistry;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommands;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParseFieldMatcherSupplier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.support.AcknowledgedRestListener;

import java.io.IOException;
import java.util.EnumSet;

/**
 */
public class RestClusterRerouteAction extends BaseRestHandler {
    private static final ObjectParser<ClusterRerouteRequest, ParseContext> PARSER = new ObjectParser<>("cluster_reroute");
    static {
        PARSER.declareField((p, v, c) -> v.commands(AllocationCommands.fromXContent(p, c.getParseFieldMatcher(), c.registry)),
                new ParseField("commands"), ValueType.OBJECT_ARRAY);
        PARSER.declareBoolean(ClusterRerouteRequest::dryRun, new ParseField("dry_run"));
    }

    private static final String DEFAULT_METRICS = Strings
            .arrayToCommaDelimitedString(EnumSet.complementOf(EnumSet.of(ClusterState.Metric.METADATA)).toArray());

    private final SettingsFilter settingsFilter;
    private final AllocationCommandRegistry registry;

    @Inject
    public RestClusterRerouteAction(Settings settings, RestController controller, SettingsFilter settingsFilter,
            AllocationCommandRegistry registry) {
        super(settings);
        this.settingsFilter = settingsFilter;
        this.registry = registry;
        controller.registerHandler(RestRequest.Method.POST, "/_cluster/reroute", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final NodeClient client) throws Exception {
        ClusterRerouteRequest clusterRerouteRequest = createRequest(request, registry, parseFieldMatcher);
        client.admin().cluster().reroute(clusterRerouteRequest, new AcknowledgedRestListener<ClusterRerouteResponse>(channel) {
            @Override
            protected void addCustomFields(XContentBuilder builder, ClusterRerouteResponse response) throws IOException {
                builder.startObject("state");
                // by default, return everything but metadata
                if (request.param("metric") == null) {
                    request.params().put("metric", DEFAULT_METRICS);
                }
                settingsFilter.addFilterSettingParams(request);
                response.getState().toXContent(builder, request);
                builder.endObject();
                if (clusterRerouteRequest.explain()) {
                    assert response.getExplanations() != null;
                    response.getExplanations().toXContent(builder, ToXContent.EMPTY_PARAMS);
                }
            }
        });
    }

    public static ClusterRerouteRequest createRequest(RestRequest request, AllocationCommandRegistry registry,
            ParseFieldMatcher parseFieldMatcher) throws IOException {
        ClusterRerouteRequest clusterRerouteRequest = Requests.clusterRerouteRequest();
        clusterRerouteRequest.dryRun(request.paramAsBoolean("dry_run", clusterRerouteRequest.dryRun()));
        clusterRerouteRequest.explain(request.paramAsBoolean("explain", clusterRerouteRequest.explain()));
        clusterRerouteRequest.timeout(request.paramAsTime("timeout", clusterRerouteRequest.timeout()));
        clusterRerouteRequest.setRetryFailed(request.paramAsBoolean("retry_failed", clusterRerouteRequest.isRetryFailed()));
        clusterRerouteRequest.masterNodeTimeout(request.paramAsTime("master_timeout", clusterRerouteRequest.masterNodeTimeout()));
        if (request.hasContent()) {
            try (XContentParser parser = XContentHelper.createParser(request.content())) {
                PARSER.parse(parser, clusterRerouteRequest, new ParseContext(registry, parseFieldMatcher));
            }
        }
        return clusterRerouteRequest;
    }

    private static class ParseContext implements ParseFieldMatcherSupplier {
        private final AllocationCommandRegistry registry;
        private final ParseFieldMatcher parseFieldMatcher;

        private ParseContext(AllocationCommandRegistry registry, ParseFieldMatcher parseFieldMatcher) {
            this.registry = registry;
            this.parseFieldMatcher = parseFieldMatcher;
        }

        @Override
        public ParseFieldMatcher getParseFieldMatcher() {
            return parseFieldMatcher;
        }
    }
}
