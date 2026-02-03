/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.resolve.ResolveIndexAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.search.crossproject.CrossProjectModeDecider;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

@ServerlessScope(Scope.PUBLIC)
public class RestResolveIndexAction extends BaseRestHandler {
    private static final Set<String> CAPABILITIES = Set.of("mode_filter");
    private final CrossProjectModeDecider crossProjectModeDecider;

    public RestResolveIndexAction(Settings settings) {
        this.crossProjectModeDecider = new CrossProjectModeDecider(settings);
    }

    @Override
    public String getName() {
        return "resolve_index_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_resolve/index/{name}"), new Route(POST, "/_resolve/index/{name}"));
    }

    @Override
    public Set<String> supportedCapabilities() {
        return CAPABILITIES;
    }

    @Override
    protected BaseRestHandler.RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String[] indices = Strings.splitStringByCommaToArray(request.param("name"));
        String modeParam = request.param("mode");

        final boolean crossProjectEnabled = crossProjectModeDecider.crossProjectEnabled();
        AtomicReference<String> projectRouting = new AtomicReference<>();
        IndicesOptions indicesOptions = IndicesOptions.fromRequest(request, ResolveIndexAction.Request.DEFAULT_INDICES_OPTIONS);

        if (crossProjectEnabled) {
            request.withContentOrSourceParamParserOrNull(parser -> {
                try {
                    // If parser is null, there's no request body. projectRouting will then yield `null`.
                    if (parser != null) {
                        projectRouting.set(parseProjectRouting(parser));
                    }
                } catch (Exception e) {
                    throw new ElasticsearchException("Couldn't parse request body", e);
                }
            });

            indicesOptions = IndicesOptions.builder(indicesOptions)
                .crossProjectModeOptions(new IndicesOptions.CrossProjectModeOptions(true))
                .build();
        }

        ResolveIndexAction.Request resolveRequest = new ResolveIndexAction.Request(
            indices,
            indicesOptions,
            modeParam == null
                ? null
                : Arrays.stream(modeParam.split(","))
                    .map(IndexMode::fromString)
                    .collect(() -> EnumSet.noneOf(IndexMode.class), EnumSet::add, EnumSet::addAll),
            projectRouting.get()
        );
        return channel -> client.admin().indices().resolveIndex(resolveRequest, new RestToXContentListener<>(channel));
    }

    private static String parseProjectRouting(XContentParser parser) throws ParsingException {
        try {
            XContentParser.Token first = parser.nextToken();
            if (first == null) {
                return null;
            }

            if (first != XContentParser.Token.START_OBJECT) {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    "Expected [" + XContentParser.Token.START_OBJECT + "] but found [" + first + "]",
                    parser.getTokenLocation()
                );
            }

            String projectRouting = null;
            for (XContentParser.Token token = parser.nextToken(); token != XContentParser.Token.END_OBJECT; token = parser.nextToken()) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    String currentName = parser.currentName();
                    if ("project_routing".equals(currentName)) {
                        parser.nextToken();
                        projectRouting = parser.text();
                    } else {
                        throw new ParsingException(parser.getTokenLocation(), "request does not support [" + parser.currentName() + "]");
                    }
                }
            }

            return projectRouting;
        } catch (ParsingException e) {
            throw e;
        } catch (Exception e) {
            throw new ParsingException(parser == null ? null : parser.getTokenLocation(), "Failed to parse", e);
        }
    }
}
