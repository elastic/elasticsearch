/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin;

import org.elasticsearch.action.ActionModule;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.license.plugin.action.delete.DeleteLicenseAction;
import org.elasticsearch.license.plugin.action.delete.TransportDeleteLicenseAction;
import org.elasticsearch.license.plugin.action.get.GetLicenseAction;
import org.elasticsearch.license.plugin.action.get.TransportGetLicenseAction;
import org.elasticsearch.license.plugin.action.put.PutLicenseAction;
import org.elasticsearch.license.plugin.action.put.TransportPutLicenseAction;
import org.elasticsearch.license.plugin.core.LicensesMetaData;
import org.elasticsearch.license.plugin.core.LicensesService;
import org.elasticsearch.license.plugin.rest.RestDeleteLicenseAction;
import org.elasticsearch.license.plugin.rest.RestGetLicenseAction;
import org.elasticsearch.license.plugin.rest.RestPutLicenseAction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

public class Licensing {

    public static final String NAME = "license";
    private final boolean isEnabled;
    protected final boolean transportClient;

    static {
        MetaData.registerPrototype(LicensesMetaData.TYPE, LicensesMetaData.PROTO);
    }

    @Inject
    public Licensing(Settings settings) {
        this.transportClient = TransportClient.CLIENT_TYPE.equals(settings.get(Client.CLIENT_TYPE_SETTING_S.getKey()));
        isEnabled = transportClient == false;
    }

    public void onModule(NetworkModule module) {
        if (transportClient == false) {
            module.registerRestHandler(RestPutLicenseAction.class);
            module.registerRestHandler(RestGetLicenseAction.class);
            module.registerRestHandler(RestDeleteLicenseAction.class);
        }
    }

    public void onModule(ActionModule module) {
        module.registerAction(PutLicenseAction.INSTANCE, TransportPutLicenseAction.class);
        module.registerAction(GetLicenseAction.INSTANCE, TransportGetLicenseAction.class);
        module.registerAction(DeleteLicenseAction.INSTANCE, TransportDeleteLicenseAction.class);
    }

    public Collection<Class<? extends LifecycleComponent>> nodeServices() {
        Collection<Class<? extends LifecycleComponent>> services = new ArrayList<>();
        if (isEnabled) {
            services.add(LicensesService.class);
        }
        return services;
    }

    public Collection<Module> nodeModules() {
        if (isEnabled) {
            return Collections.<Module>singletonList(new LicensingModule());
        }
        return Collections.emptyList();
    }

    public void onModule(SettingsModule module) {
        // TODO convert this wildcard to a real setting
        module.registerSetting(Setting.groupSetting("license.", Setting.Property.NodeScope));
    }
}
