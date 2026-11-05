/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.PUT;
import static org.elasticsearch.rest.RestUtils.getAckTimeout;
import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;

@ServerlessScope(Scope.PUBLIC)
public class RestUpdateSettingsAction extends BaseRestHandler {

    private static final Set<String> SUPPORTED_QUERY_PARAMETERS = Set.of(
        RestUtils.REST_MASTER_TIMEOUT_PARAM,
        RestUtils.REST_TIMEOUT_PARAM,
        IndicesOptions.WildcardOptions.EXPAND_WILDCARDS,
        IndicesOptions.ConcreteTargetOptions.IGNORE_UNAVAILABLE,
        IndicesOptions.WildcardOptions.ALLOW_NO_INDICES,
        IndicesOptions.GatekeeperOptions.IGNORE_THROTTLED,
        "preserve_existing",
        "reopen",
        Settings.FLAT_SETTINGS_PARAM
    );
    private static final Set<String> ALL_SUPPORTED_PARAMETERS = Sets.addToCopy(
        Sets.union(SUPPORTED_QUERY_PARAMETERS, Settings.FORMAT_PARAMS),
        "index"
    );

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, "/{index}/_settings"), new Route(PUT, "/_settings"));
    }

    @Override
    public String getName() {
        return "update_settings_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(indices);
        updateSettingsRequest.ackTimeout(getAckTimeout(request));
        updateSettingsRequest.setPreserveExisting(request.paramAsBoolean("preserve_existing", updateSettingsRequest.isPreserveExisting()));
        updateSettingsRequest.masterNodeTimeout(getMasterNodeTimeout(request));
        updateSettingsRequest.indicesOptions(IndicesOptions.fromRequest(request, updateSettingsRequest.indicesOptions()));
        updateSettingsRequest.reopen(request.paramAsBoolean("reopen", false));
        try (var parser = request.contentParser()) {
            updateSettingsRequest.fromXContent(parser);
        }

        return channel -> client.admin().indices().updateSettings(updateSettingsRequest, new RestToXContentListener<>(channel));
    }

    @Override
    protected Set<String> responseParams() {
        return Settings.FORMAT_PARAMS;
    }

    @Override
    public Set<String> supportedQueryParameters() {
        return SUPPORTED_QUERY_PARAMETERS;
    }

    @Override
    public Set<String> allSupportedParameters() {
        return ALL_SUPPORTED_PARAMETERS;
    }

}
