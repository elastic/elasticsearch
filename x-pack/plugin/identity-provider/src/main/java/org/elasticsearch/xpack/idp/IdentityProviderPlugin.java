/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.idp;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.ssl.X509KeyPairSettings;
import org.elasticsearch.xpack.idp.action.DeleteSamlServiceProviderAction;
import org.elasticsearch.xpack.idp.action.PutSamlServiceProviderAction;
import org.elasticsearch.xpack.idp.action.SamlInitiateSingleSignOnAction;
import org.elasticsearch.xpack.idp.action.SamlMetadataAction;
import org.elasticsearch.xpack.idp.action.SamlValidateAuthnRequestAction;
import org.elasticsearch.xpack.idp.action.TransportDeleteSamlServiceProviderAction;
import org.elasticsearch.xpack.idp.action.TransportPutSamlServiceProviderAction;
import org.elasticsearch.xpack.idp.action.TransportSamlInitiateSingleSignOnAction;
import org.elasticsearch.xpack.idp.action.TransportSamlMetadataAction;
import org.elasticsearch.xpack.idp.action.TransportSamlValidateAuthnRequestAction;
import org.elasticsearch.xpack.idp.privileges.ApplicationActionsResolver;
import org.elasticsearch.xpack.idp.privileges.UserPrivilegeResolver;
import org.elasticsearch.xpack.idp.saml.idp.SamlIdentityProvider;
import org.elasticsearch.xpack.idp.saml.idp.SamlIdentityProviderBuilder;
import org.elasticsearch.xpack.idp.saml.rest.action.RestDeleteSamlServiceProviderAction;
import org.elasticsearch.xpack.idp.saml.rest.action.RestPutSamlServiceProviderAction;
import org.elasticsearch.xpack.idp.saml.rest.action.RestSamlInitiateSingleSignOnAction;
import org.elasticsearch.xpack.idp.saml.rest.action.RestSamlMetadataAction;
import org.elasticsearch.xpack.idp.saml.rest.action.RestSamlValidateAuthenticationRequestAction;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProviderFactory;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProviderIndex;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProviderResolver;
import org.elasticsearch.xpack.idp.saml.sp.ServiceProviderCacheSettings;
import org.elasticsearch.xpack.idp.saml.sp.ServiceProviderDefaults;
import org.elasticsearch.xpack.idp.saml.sp.WildcardServiceProviderResolver;
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

    private final Logger logger = LogManager.getLogger(IdentityProviderPlugin.class);
    private boolean enabled;
    private Settings settings;

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        settings = environment.settings();
        enabled = ENABLED_SETTING.get(settings);
        if (enabled == false) {
            return List.of();
        }

        SamlInit.initialize();
        final SamlServiceProviderIndex index = new SamlServiceProviderIndex(client, clusterService);
        final SecurityContext securityContext = new SecurityContext(settings, threadPool.getThreadContext());

        final ServiceProviderDefaults serviceProviderDefaults = ServiceProviderDefaults.forSettings(settings);
        final ApplicationActionsResolver actionsResolver = new ApplicationActionsResolver(settings, serviceProviderDefaults, client);
        final UserPrivilegeResolver userPrivilegeResolver = new UserPrivilegeResolver(client, securityContext, actionsResolver);

        final SamlServiceProviderFactory serviceProviderFactory = new SamlServiceProviderFactory(serviceProviderDefaults);
        final SamlServiceProviderResolver registeredServiceProviderResolver = new SamlServiceProviderResolver(
            settings,
            index,
            serviceProviderFactory
        );
        final WildcardServiceProviderResolver wildcardServiceProviderResolver = WildcardServiceProviderResolver.create(
            environment,
            resourceWatcherService,
            scriptService,
            serviceProviderFactory
        );
        final SamlIdentityProvider idp = SamlIdentityProvider.builder(registeredServiceProviderResolver, wildcardServiceProviderResolver)
            .fromSettings(environment)
            .serviceProviderDefaults(serviceProviderDefaults)
            .build();

        final SamlFactory factory = new SamlFactory();

        return List.of(index, idp, factory, userPrivilegeResolver);
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
            new ActionHandler<>(PutSamlServiceProviderAction.INSTANCE, TransportPutSamlServiceProviderAction.class),
            new ActionHandler<>(DeleteSamlServiceProviderAction.INSTANCE, TransportDeleteSamlServiceProviderAction.class)
        );
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings unused,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        if (enabled == false) {
            return List.of();
        }
        return List.of(
            new RestSamlInitiateSingleSignOnAction(getLicenseState()),
            new RestSamlValidateAuthenticationRequestAction(getLicenseState()),
            new RestSamlMetadataAction(getLicenseState()),
            new RestPutSamlServiceProviderAction(getLicenseState()),
            new RestDeleteSamlServiceProviderAction(getLicenseState())
        );
    }

    @Override
    public List<Setting<?>> getSettings() {
        List<Setting<?>> settingList = new ArrayList<>();
        settingList.add(ENABLED_SETTING);
        settingList.addAll(SamlIdentityProviderBuilder.getSettings());
        settingList.addAll(ServiceProviderCacheSettings.getSettings());
        settingList.addAll(ServiceProviderDefaults.getSettings());
        settingList.addAll(WildcardServiceProviderResolver.getSettings());
        settingList.addAll(ApplicationActionsResolver.getSettings());
        settingList.addAll(X509KeyPairSettings.withPrefix("xpack.idp.signing.", false).getEnabledSettings());
        settingList.addAll(X509KeyPairSettings.withPrefix("xpack.idp.metadata_signing.", false).getEnabledSettings());
        return Collections.unmodifiableList(settingList);
    }

    protected XPackLicenseState getLicenseState() {
        return XPackPlugin.getSharedLicenseState();
    }

}
