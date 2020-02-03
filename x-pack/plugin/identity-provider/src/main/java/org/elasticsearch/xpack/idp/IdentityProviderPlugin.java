/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.idp;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.ssl.X509KeyPairSettings;
import org.elasticsearch.xpack.idp.action.SamlInitiateSingleSignOnAction;
import org.elasticsearch.xpack.idp.action.TransportSamlInitiateSingleSignOnAction;
import org.elasticsearch.xpack.idp.rest.action.RestSamlInitiateSingleSignOnAction;
import org.elasticsearch.xpack.idp.saml.idp.CloudIdp;
import org.elasticsearch.xpack.idp.saml.support.SamlUtils;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;

import org.elasticsearch.xpack.core.ssl.X509KeyPairSettings;
import org.elasticsearch.xpack.idp.saml.idp.CloudIdp;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

/**
 * This plugin provides the backend for an IdP built on top of Elasticsearch security features.
 * It is used internally within Elastic and is not intended for general use.
 */
public class IdentityProviderPlugin extends Plugin implements ActionPlugin {

    private static final Setting<Boolean> ENABLED_SETTING = Setting.boolSetting("xpack.idp.enabled", false, Setting.Property.NodeScope);
    public static final Setting<String> IDP_ENTITY_ID = Setting.simpleString("xpack.idp.entity_id", Setting.Property.NodeScope);
    public static final Setting<String> IDP_SSO_REDIRECT_ENDPOINT = Setting.simpleString("xpack.idp.sso_endpoint.redirect", value -> {
        try {
            new URI(value);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid value [" + value + "] for  [xpack.idp.sso_endpoint.redirect]. Not a valid URI", e);
        }
    }, Setting.Property.NodeScope);
    public static final Setting<String> IDP_SSO_POST_ENDPOINT = Setting.simpleString("xpack.idp.sso_endpoint.post", value -> {
        try {
            new URI(value);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid value [" + value + "] for  [xpack.idp.sso_endpoint.post]. Not a valid URI", e);
        }
    }, Setting.Property.NodeScope);
    public static final Setting<String> IDP_SLO_REDIRECT_ENDPOINT = Setting.simpleString("xpack.idp.slo_endpoint.redirect", value -> {
        try {
            new URI(value);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid value [" + value + "] for  [xpack.idp.slo_endpoint.redirect]. Not a valid URI", e);
        }
    }, Setting.Property.NodeScope);
    public static final Setting<String> IDP_SLO_POST_ENDPOINT = Setting.simpleString("xpack.idp.slo_endpoint.post", value -> {
        try {
            new URI(value);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid value [" + value + "] for  [xpack.idp.slo_endpoint.post]. Not a valid URI", e);
        }
    }, Setting.Property.NodeScope);
    public static final Setting<String> IDP_SIGNING_KEY_ALIAS = Setting.simpleString("xpack.idp.signing.keystore.alias",
        Setting.Property.NodeScope);

    private final Logger logger = LogManager.getLogger();
    private boolean enabled;
    private Settings settings;

    @Override
    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
                                               ResourceWatcherService resourceWatcherService, ScriptService scriptService,
                                               NamedXContentRegistry xContentRegistry, Environment environment,
                                               NodeEnvironment nodeEnvironment, NamedWriteableRegistry namedWriteableRegistry) {
        settings = environment.settings();
        enabled = ENABLED_SETTING.get(settings);
        if (enabled == false) {
            return List.of();
        }

        SamlUtils.initialize();
        CloudIdp idp = new CloudIdp(environment, settings);
        return List.of();
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {

        enabled = ENABLED_SETTING.get(settings);
        if (enabled == false) {
            return Collections.emptyList();
        }
        return Collections.singletonList(
            new ActionHandler<>(SamlInitiateSingleSignOnAction.INSTANCE, TransportSamlInitiateSingleSignOnAction.class)
        );
    }

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
                                             IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter,
                                             IndexNameExpressionResolver indexNameExpressionResolver,
                                             Supplier<DiscoveryNodes> nodesInCluster) {
        if (enabled == false) {
            return Collections.emptyList();
        }
        return Collections.singletonList(new RestSamlInitiateSingleSignOnAction(restController));
    }

    @Override
    public List<Setting<?>> getSettings() {
        List<Setting<?>> settings = new ArrayList<>();
        settings.add(ENABLED_SETTING);
        settings.add(IDP_ENTITY_ID);
        settings.add(IDP_SLO_REDIRECT_ENDPOINT);
        settings.add(IDP_SLO_POST_ENDPOINT);
        settings.add(IDP_SSO_REDIRECT_ENDPOINT);
        settings.add(IDP_SSO_POST_ENDPOINT);
        settings.addAll(X509KeyPairSettings.withPrefix("xpack.idp.signing.", false).getAllSettings());
        return Collections.unmodifiableList(settings);
    }
}
