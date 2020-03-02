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
import org.elasticsearch.xpack.idp.action.PutSamlServiceProviderAction;
import org.elasticsearch.xpack.idp.action.SamlInitiateSingleSignOnAction;
import org.elasticsearch.xpack.idp.action.TransportPutSamlServiceProviderAction;
import org.elasticsearch.xpack.idp.action.SamlMetadataAction;
import org.elasticsearch.xpack.idp.action.TransportSamlInitiateSingleSignOnAction;
import org.elasticsearch.xpack.idp.action.TransportSamlMetadataAction;
import org.elasticsearch.xpack.idp.rest.RestSamlMetadataAction;
import org.elasticsearch.xpack.idp.rest.action.RestSamlInitiateSingleSignOnAction;
import org.elasticsearch.xpack.idp.action.SamlValidateAuthnRequestAction;
import org.elasticsearch.xpack.idp.action.TransportSamlValidateAuthnRequestAction;
import org.elasticsearch.xpack.idp.rest.RestSamlValidateAuthenticationRequestAction;
import org.elasticsearch.xpack.idp.saml.idp.CloudIdp;
import org.elasticsearch.xpack.idp.saml.support.SamlInit;
import org.elasticsearch.xpack.idp.saml.rest.action.RestPutSamlServiceProviderAction;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProviderIndex;

import java.net.MalformedURLException;
import java.net.URL;
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
    public static final Setting<URL> IDP_SSO_REDIRECT_ENDPOINT = new Setting<>("xpack.idp.sso_endpoint.redirect", "https:",
        value -> parseUrl("xpack.idp.sso_endpoint.redirect", value), Setting.Property.NodeScope);
    public static final Setting<URL> IDP_SSO_POST_ENDPOINT = new Setting<>("xpack.idp.sso_endpoint.post", "https:",
        value -> parseUrl("xpack.idp.sso_endpoint.post", value), Setting.Property.NodeScope);
    public static final Setting<URL> IDP_SLO_REDIRECT_ENDPOINT = new Setting<>("xpack.idp.slo_endpoint.redirect", "https:",
        value -> parseUrl("xpack.idp.slo_endpoint.redirect", value), Setting.Property.NodeScope);
    public static final Setting<URL> IDP_SLO_POST_ENDPOINT = new Setting<>("xpack.idp.slo_endpoint.post", "https:",
        value -> parseUrl("xpack.idp.slo_endpoint.post", value), Setting.Property.NodeScope);
    public static final Setting<String> IDP_SIGNING_KEY_ALIAS = Setting.simpleString("xpack.idp.signing.keystore.alias",
        Setting.Property.NodeScope);
    public static final Setting<String> IDP_METADATA_SIGNING_KEY_ALIAS = Setting.simpleString("xpack.idp.metadata.signing.keystore.alias",
        Setting.Property.NodeScope);
    public static final Setting<String> IDP_ORGANIZATION_NAME = Setting.simpleString("xpack.idp.organization.name",
        Setting.Property.NodeScope);

    public static final Setting<String> IDP_ORGANIZATION_DISPLAY_NAME = Setting.simpleString("xpack.idp.organization.display_name",
        IDP_ORGANIZATION_NAME, Setting.Property.NodeScope);

    public static final Setting<URL> IDP_ORGANIZATION_URL = new Setting<>("xpack.idp.organization.url", "http:",
        value -> parseUrl("xpack.idp.organization.url", value), Setting.Property.NodeScope);

    public static final Setting<String> IDP_CONTACT_GIVEN_NAME = Setting.simpleString("xpack.idp.contact.given_name",
        Setting.Property.NodeScope);

    public static final Setting<String> IDP_CONTACT_SURNAME = Setting.simpleString("xpack.idp.contact.surname",
        Setting.Property.NodeScope);

    public static final Setting<String> IDP_CONTACT_EMAIL = Setting.simpleString("xpack.idp.contact.email", Setting.Property.NodeScope);


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
        CloudIdp idp = new CloudIdp(environment, settings);
        SamlServiceProviderIndex index = new SamlServiceProviderIndex(client, clusterService);
        return List.of(index);
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        if (enabled == false) {
            return List.of();
        }
        return List.of(
            new ActionHandler<>(SamlInitiateSingleSignOnAction.INSTANCE, TransportSamlInitiateSingleSignOnAction.class),
            new ActionHandler<>(SamlValidateAuthnRequestAction.INSTANCE, TransportSamlValidateAuthnRequestAction.class),
            new ActionHandler<>(SamlMetadataAction.INSTANCE, TransportSamlMetadataAction.class),
            new ActionHandler<>(PutSamlServiceProviderAction.INSTANCE, TransportPutSamlServiceProviderAction.class)
        );
    }

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
                                             IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter,
                                             IndexNameExpressionResolver indexNameExpressionResolver,
                                             Supplier<DiscoveryNodes> nodesInCluster) {
        if (enabled == false) {
            return List.of();
        }
        return List.of(
            new RestSamlInitiateSingleSignOnAction(),
            new RestSamlValidateAuthenticationRequestAction(),
            new RestSamlMetadataAction(),
            new RestPutSamlServiceProviderAction()
        );
    }

    @Override
    public List<Setting<?>> getSettings() {
        List<Setting<?>> settings = new ArrayList<>();
        settings.addAll(List.of(ENABLED_SETTING, IDP_ENTITY_ID, IDP_SLO_REDIRECT_ENDPOINT, IDP_SLO_POST_ENDPOINT,
            IDP_SSO_REDIRECT_ENDPOINT, IDP_SSO_POST_ENDPOINT, IDP_SIGNING_KEY_ALIAS, IDP_METADATA_SIGNING_KEY_ALIAS,
            IDP_ORGANIZATION_NAME, IDP_ORGANIZATION_DISPLAY_NAME, IDP_ORGANIZATION_URL, IDP_CONTACT_GIVEN_NAME, IDP_CONTACT_SURNAME,
            IDP_CONTACT_EMAIL));
        settings.addAll(X509KeyPairSettings.withPrefix("xpack.idp.signing.", false).getAllSettings());
        settings.addAll(X509KeyPairSettings.withPrefix("xpack.idp.metadata_signing.", false).getAllSettings());
        return Collections.unmodifiableList(settings);
    }

    private static URL parseUrl(String key, String value) {
        try {
            return new URL(value);
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("Invalid value [" + value + "] for [" + key + "]. Not a valid URL", e);
        }
    }
}
