/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin;

import org.elasticsearch.action.ActionModule;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
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
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestModule;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

public class LicensePlugin extends Plugin {

    public static final String NAME = "license";
    private final boolean isEnabled;

    static {
        MetaData.registerPrototype(LicensesMetaData.TYPE, LicensesMetaData.PROTO);
    }

    @Inject
    public LicensePlugin(Settings settings) {
        if (DiscoveryNode.clientNode(settings)) {
            // Enable plugin only on node clients
            this.isEnabled = "node".equals(settings.get(Client.CLIENT_TYPE_SETTING));
        } else {
            this.isEnabled = true;
        }
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public String description() {
        return "Internal Elasticsearch Licensing Plugin";
    }

    public void onModule(RestModule module) {
        // Register REST endpoint
        module.addRestAction(RestPutLicenseAction.class);
        module.addRestAction(RestGetLicenseAction.class);
        module.addRestAction(RestDeleteLicenseAction.class);
    }

    public void onModule(ActionModule module) {
        module.registerAction(PutLicenseAction.INSTANCE, TransportPutLicenseAction.class);
        module.registerAction(GetLicenseAction.INSTANCE, TransportGetLicenseAction.class);
        module.registerAction(DeleteLicenseAction.INSTANCE, TransportDeleteLicenseAction.class);
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> nodeServices() {
        Collection<Class<? extends LifecycleComponent>> services = new ArrayList<>();
        if (isEnabled) {
            services.add(LicensesService.class);
        }
        return services;
    }


    @Override
    public Collection<Module> nodeModules() {
        if (isEnabled) {
            return Collections.<Module>singletonList(new LicenseModule());
        }
        return Collections.emptyList();
    }
}
