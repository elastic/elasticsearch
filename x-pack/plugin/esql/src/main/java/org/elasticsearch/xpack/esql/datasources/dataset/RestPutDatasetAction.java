/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.dataset;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.FilteredRestRequest;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequestFilter;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.esql.datasources.EsqlDataSourcesCapabilities;
import org.elasticsearch.xpack.esql.datasources.ExternalFailures;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.rest.RestRequest.Method.PUT;

@ServerlessScope(Scope.PUBLIC)
public class RestPutDatasetAction extends BaseRestHandler implements RestRequestFilter {

    private final Set<String> filteredFields;

    public RestPutDatasetAction(Set<String> secretSettingNames) {
        this.filteredFields = secretSettingNames.stream().map(name -> "settings." + name).collect(Collectors.toUnmodifiableSet());
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, "/_query/dataset/{name}"));
    }

    @Override
    public String getName() {
        return "esql_put_dataset";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String name = request.param("name");
        try (XContentParser parser = request.contentParser()) {
            PutDatasetAction.Request putRequest = PutDatasetAction.Request.fromXContent(
                parser,
                RestUtils.getMasterNodeTimeout(request),
                RestUtils.getAckTimeout(request),
                name
            );
            return channel -> client.execute(PutDatasetAction.INSTANCE, putRequest, new RestToXContentListener<>(channel));
        }
    }

    @Override
    public Set<String> getFilteredFields() {
        return filteredFields;
    }

    /**
     * Filters secret setting names under {@code settings} and redacts secret parts of {@code resource}
     * (query string, fragment, and user info on {@code http}/{@code https} URLs) before the body is audited.
     * Overrides the default so redaction still runs when there are no secret setting names to drop.
     * <p>
     * Malformed bodies fall back to the original content: {@code FilteredRestRequest} parses before
     * audit-body rendering can catch parse errors, and an uncaught failure there would turn a client
     * 400 into a 500.
     */
    @Override
    public RestRequest getFilteredRequest(RestRequest restRequest) {
        if (restRequest.hasContent()) {
            return new FilteredRestRequest(restRequest, filteredFields) {
                @Override
                public ReleasableBytesReference content() {
                    try {
                        return super.content();
                    } catch (Exception e) {
                        // Safe to swallow: AuditUtil then renders the raw bytes as "Invalid Format: ...",
                        // and prepareRequest still returns 400 from contentParser().
                        return restRequest.content();
                    }
                }

                @Override
                protected Map<String, Object> transformBody(Map<String, Object> map) {
                    Map<String, Object> filtered = super.transformBody(map);
                    Object resource = filtered.get("resource");
                    if (resource instanceof String resourceString) {
                        filtered.put("resource", ExternalFailures.redactHttpUrl(resourceString));
                    }
                    return filtered;
                }
            };
        } else {
            return restRequest;
        }
    }

    @Override
    public Set<String> supportedCapabilities() {
        return Set.of(
            EsqlDataSourcesCapabilities.DATA_SOURCES,
            EsqlDataSourcesCapabilities.DATASET_DECLARED_SCHEMA,
            EsqlDataSourcesCapabilities.DATA_SOURCES_SERVERLESS_SCOPE,
            EsqlDataSourcesCapabilities.DATASET_REGION,
            EsqlDataSourcesCapabilities.DATASET_TEXT_TYPE_NOT_DECLARABLE,
            EsqlDataSourcesCapabilities.DATASET_ID_NOT_DECLARABLE
        );
    }
}
