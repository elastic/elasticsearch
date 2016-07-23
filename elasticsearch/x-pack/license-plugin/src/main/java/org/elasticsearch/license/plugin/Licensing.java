/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.license.plugin.action.delete.DeleteLicenseAction;
import org.elasticsearch.license.plugin.action.delete.TransportDeleteLicenseAction;
import org.elasticsearch.license.plugin.action.get.GetLicenseAction;
import org.elasticsearch.license.plugin.action.get.TransportGetLicenseAction;
import org.elasticsearch.license.plugin.action.put.PutLicenseAction;
import org.elasticsearch.license.plugin.action.put.TransportPutLicenseAction;
import org.elasticsearch.license.plugin.core.LicensesMetaData;
import org.elasticsearch.license.plugin.core.LicenseService;
import org.elasticsearch.license.plugin.core.XPackLicenseState;
import org.elasticsearch.license.plugin.rest.RestDeleteLicenseAction;
import org.elasticsearch.license.plugin.rest.RestGetLicenseAction;
import org.elasticsearch.license.plugin.rest.RestPutLicenseAction;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.support.clock.Clock;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.XPackPlugin.isTribeNode;
import static org.elasticsearch.xpack.XPackPlugin.transportClientMode;


public class Licensing implements ActionPlugin {

    public static final String NAME = "license";
    protected final Settings settings;
    protected final boolean isTransportClient;
    private final boolean isTribeNode;

    static {
        MetaData.registerPrototype(LicensesMetaData.TYPE, LicensesMetaData.PROTO);
    }

    public Licensing(Settings settings) {
        this.settings = settings;
        isTransportClient = transportClientMode(settings);
        isTribeNode = isTribeNode(settings);
    }

    public Collection<Module> nodeModules() {
        return Collections.emptyList();
    }

    @Override
    public List<ActionHandler<? extends ActionRequest<?>, ? extends ActionResponse>> getActions() {
        if (isTribeNode) {
            return emptyList();
        }
        return Arrays.asList(new ActionHandler<>(PutLicenseAction.INSTANCE, TransportPutLicenseAction.class),
                new ActionHandler<>(GetLicenseAction.INSTANCE, TransportGetLicenseAction.class),
                new ActionHandler<>(DeleteLicenseAction.INSTANCE, TransportDeleteLicenseAction.class));
    }

    @Override
    public List<Class<? extends RestHandler>> getRestHandlers() {
        if (isTribeNode) {
            return emptyList();
        }
        return Arrays.asList(RestPutLicenseAction.class,
                RestGetLicenseAction.class,
                RestDeleteLicenseAction.class);
    }

    public Collection<Object> createComponents(ClusterService clusterService, Clock clock, Environment environment,
                                               ResourceWatcherService resourceWatcherService,
                                               XPackLicenseState licenseState) {
        LicenseService licenseService = new LicenseService(settings, clusterService, clock,
                environment, resourceWatcherService, licenseState);
        return Arrays.asList(licenseService, licenseState);
    }

    public List<Setting<?>> getSettings() {
        // TODO convert this wildcard to a real setting
        return Collections.singletonList(Setting.groupSetting("license.", Setting.Property.NodeScope));
    }
}
