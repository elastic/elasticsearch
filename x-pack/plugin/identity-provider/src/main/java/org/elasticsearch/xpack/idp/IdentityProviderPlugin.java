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
import org.elasticsearch.xpack.idp.action.SamlMetadataAction;
import org.elasticsearch.xpack.idp.action.SamlValidateAuthnRequestAction;
import org.elasticsearch.xpack.idp.action.TransportSamlInitiateSingleSignOnAction;
import org.elasticsearch.xpack.idp.action.TransportSamlMetadataAction;
import org.elasticsearch.xpack.idp.action.TransportSamlValidateAuthnRequestAction;
import org.elasticsearch.xpack.idp.rest.RestSamlMetadataAction;
import org.elasticsearch.xpack.idp.rest.RestSamlValidateAuthenticationRequestAction;
import org.elasticsearch.xpack.idp.rest.action.RestSamlInitiateSingleSignOnAction;
import org.elasticsearch.xpack.idp.saml.idp.SamlIdentityProvider;
import org.elasticsearch.xpack.idp.saml.idp.SamlIdentityProviderBuilder;
import org.elasticsearch.xpack.idp.saml.support.SamlFactory;
import org.elasticsearch.xpack.idp.saml.support.SamlInit;

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

    private final Logger logger = LogManager.getLogger();
    private boolean enabled;
    private Settings settings;

    @Override
    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
                                               ResourceWatcherService resourceWatcherService, ScriptService scriptService,
                                               NamedXContentRegistry xContentRegistry, Environment environment,
                                               NodeEnvironment nodeEnvironment, NamedWriteableRegistry namedWriteableRegistry,
                                               IndexNameExpressionResolver indexNameExpressionResolver) {
        settings = environment.settings();
        enabled = ENABLED_SETTING.get(settings);
        if (enabled == false) {
            return List.of();
        }

        SamlInit.initialize();
        final SamlIdentityProvider idp = SamlIdentityProvider.builder().fromSettings(environment).build();
        final SamlFactory factory = new SamlFactory();
        return List.of(
            idp,
            factory
        );
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        if (enabled == false) {
            return Collections.emptyList();
        }
        return List.of(
            new ActionHandler<>(SamlInitiateSingleSignOnAction.INSTANCE, TransportSamlInitiateSingleSignOnAction.class),
            new ActionHandler<>(SamlValidateAuthnRequestAction.INSTANCE, TransportSamlValidateAuthnRequestAction.class),
            new ActionHandler<>(SamlMetadataAction.INSTANCE, TransportSamlMetadataAction.class)
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
        return List.of(
            new RestSamlInitiateSingleSignOnAction(),
            new RestSamlValidateAuthenticationRequestAction(),
            new RestSamlMetadataAction());
    }

    @Override
    public List<Setting<?>> getSettings() {
        List<Setting<?>> settings = new ArrayList<>();
        settings.add(ENABLED_SETTING);
        settings.addAll(SamlIdentityProviderBuilder.getSettings());
        settings.addAll(X509KeyPairSettings.withPrefix("xpack.idp.signing.", false).getAllSettings());
        settings.addAll(X509KeyPairSettings.withPrefix("xpack.idp.metadata_signing.", false).getAllSettings());
        return Collections.unmodifiableList(settings);
    }

}
