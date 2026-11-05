/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.admin.cluster.settings.UpdateSecureSettingsAction;
import org.elasticsearch.action.admin.cluster.settings.UpdateSecureSettingsRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.FilteredRestRequest;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequestFilter;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.elasticsearch.rest.RestRequest.Method.PUT;
import static org.elasticsearch.rest.RestUtils.getAckTimeout;
import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;

@ServerlessScope(Scope.INTERNAL)
public class RestClusterUpdateSettingsAction extends BaseRestHandler implements RestRequestFilter {
    private static final String PERSISTENT = "persistent";
    private static final String TRANSIENT = "transient";

    public static final String SUPPORTS_DEFAULT_REPO_SETTING = "repositories.default_repository_setting";
    private static final Set<String> CAPABILITIES = Set.of(SUPPORTS_DEFAULT_REPO_SETTING);

    private final ClusterSettings clusterSettings;
    private final Predicate<NodeFeature> clusterSupportsFeature;

    public RestClusterUpdateSettingsAction(ClusterSettings clusterSettings, Predicate<NodeFeature> clusterSupportsFeature) {
        this.clusterSettings = clusterSettings;
        this.clusterSupportsFeature = clusterSupportsFeature;
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, "/_cluster/settings"));
    }

    @Override
    public String getName() {
        return "cluster_update_settings_action";
    }

    @Override
    @SuppressWarnings("unchecked")
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        Map<String, Object> source;
        try (XContentParser parser = request.contentParser()) {
            source = parser.map();
        }
        if (UpdateSecureSettingsAction.SECURE_SETTINGS_API_FEATURE_FLAG.isEnabled() && containsSecureSettings(source)) {
            return prepareSecureSettingsRequest(request, client, source);
        }

        final ClusterUpdateSettingsRequest clusterUpdateSettingsRequest = new ClusterUpdateSettingsRequest(
            getMasterNodeTimeout(request),
            getAckTimeout(request)
        );
        if (source.containsKey(TRANSIENT)) {
            clusterUpdateSettingsRequest.transientSettings((Map<String, ?>) source.get(TRANSIENT));
        }
        if (source.containsKey(PERSISTENT)) {
            clusterUpdateSettingsRequest.persistentSettings((Map<String, ?>) source.get(PERSISTENT));
        }

        return channel -> client.admin().cluster().updateSettings(clusterUpdateSettingsRequest, new RestToXContentListener<>(channel));
    }

    private RestChannelConsumer prepareSecureSettingsRequest(RestRequest request, NodeClient client, Map<String, Object> source) {
        final Settings transientSettings = settingsFrom(source, TRANSIENT);
        final Settings persistentSettings = settingsFrom(source, PERSISTENT);
        if (secureSettingNames(transientSettings).isEmpty() == false) {
            throw new IllegalArgumentException("secure settings can only be set as persistent settings");
        }
        if (transientSettings.isEmpty() == false || secureSettingNames(persistentSettings).size() != persistentSettings.size()) {
            throw new IllegalArgumentException("a request that sets secure settings cannot also set regular settings");
        }
        if ("https".equals(request.getHttpRequest().getScheme()) == false) {
            throw new IllegalArgumentException("secure settings can only be set over HTTPS");
        }
        if (clusterSupportsFeature.test(UpdateSecureSettingsAction.SECURE_SETTINGS_API) == false) {
            throw new IllegalArgumentException("secure settings cannot be set until all nodes in the cluster support it");
        }

        final Map<String, SecureString> values = new HashMap<>();
        for (String name : persistentSettings.keySet()) {
            final String value = persistentSettings.get(name);
            values.put(name, value == null ? null : new SecureString(value.toCharArray()));
        }
        final UpdateSecureSettingsRequest secureSettingsRequest = new UpdateSecureSettingsRequest(
            getMasterNodeTimeout(request),
            getAckTimeout(request),
            values
        );
        return channel -> client.execute(
            UpdateSecureSettingsAction.INSTANCE,
            secureSettingsRequest,
            new RestToXContentListener<ClusterUpdateSettingsResponse>(channel).map(
                response -> new ClusterUpdateSettingsResponse(response.isAcknowledged(), Settings.EMPTY, Settings.EMPTY)
            )
        );
    }

    @SuppressWarnings("unchecked")
    private static Settings settingsFrom(Map<String, Object> source, String key) {
        final Object value = source.get(key);
        if (value == null) {
            return Settings.EMPTY;
        }
        if (value instanceof Map<?, ?> map) {
            return Settings.builder().loadFromMap((Map<String, ?>) map).build();
        }
        throw new IllegalArgumentException("[" + key + "] must be an object");
    }

    private Set<String> secureSettingNames(Settings settings) {
        return settings.keySet().stream().filter(name -> clusterSettings.get(name) instanceof SecureSetting<?>).collect(Collectors.toSet());
    }

    private boolean containsSecureSettings(Map<String, Object> source) {
        return secureSettingNames(settingsFrom(source, PERSISTENT)).isEmpty() == false
            || secureSettingNames(settingsFrom(source, TRANSIENT)).isEmpty() == false;
    }

    @Override
    public Set<String> getFilteredFields() {
        return Set.of();
    }

    /**
     * Omits the whole body from audit logs when it names a secure setting, since it then holds plain text secret values. A body that
     * can't be parsed is omitted too, rather than exposing raw bytes that may contain credentials.
     */
    @Override
    public RestRequest getFilteredRequest(RestRequest restRequest) {
        if (restRequest.hasContent() == false || UpdateSecureSettingsAction.SECURE_SETTINGS_API_FEATURE_FLAG.isEnabled() == false) {
            return restRequest;
        }
        return new FilteredRestRequest(restRequest, Set.of()) {
            @Override
            public ReleasableBytesReference content() {
                try {
                    return super.content();
                } catch (Exception e) {
                    return ReleasableBytesReference.wrap(BytesArray.EMPTY);
                }
            }

            @Override
            protected Map<String, Object> transformBody(Map<String, Object> map) {
                try {
                    return containsSecureSettings(map) ? Map.of() : map;
                } catch (RuntimeException e) {
                    return Map.of();
                }
            }
        };
    }

    @Override
    protected Set<String> responseParams() {
        return Settings.FORMAT_PARAMS;
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }

    @Override
    public Set<String> supportedCapabilities() {
        return CAPABILITIES;
    }
}
