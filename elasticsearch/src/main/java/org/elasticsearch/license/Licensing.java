/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.rest.RestHandler;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.XPackPlugin.isTribeNode;
import static org.elasticsearch.xpack.XPackPlugin.transportClientMode;

public class Licensing implements ActionPlugin {

    public static final String NAME = "license";
    protected final Settings settings;
    protected final boolean isTransportClient;
    private final boolean isTribeNode;

    static {
        // we have to make sure we don't override the prototype, if we already
        // registered. This causes class cast exceptions while casting license
        // meta data on tribe node, as the registration happens for every tribe
        // client nodes and the tribe node itself
        if (MetaData.lookupPrototype(LicensesMetaData.TYPE) == null) {
            MetaData.registerPrototype(LicensesMetaData.TYPE, LicensesMetaData.PROTO);
        }
    }

    public Licensing(Settings settings) {
        this.settings = settings;
        isTransportClient = transportClientMode(settings);
        isTribeNode = isTribeNode(settings);
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        if (isTribeNode) {
            return Collections.singletonList(new ActionHandler<>(GetLicenseAction.INSTANCE, TransportGetLicenseAction.class));
        }
        return Arrays.asList(new ActionHandler<>(PutLicenseAction.INSTANCE, TransportPutLicenseAction.class),
                new ActionHandler<>(GetLicenseAction.INSTANCE, TransportGetLicenseAction.class),
                new ActionHandler<>(DeleteLicenseAction.INSTANCE, TransportDeleteLicenseAction.class));
    }

    @Override
    public List<Class<? extends RestHandler>> getRestHandlers() {
        if (isTribeNode) {
            return Collections.singletonList(RestGetLicenseAction.class);
        }
        return Arrays.asList(RestPutLicenseAction.class,
                RestGetLicenseAction.class,
                RestDeleteLicenseAction.class);
    }

    public List<Setting<?>> getSettings() {
        // TODO convert this wildcard to a real setting
        return Collections.singletonList(Setting.groupSetting("license.", Setting.Property.NodeScope));
    }

}
