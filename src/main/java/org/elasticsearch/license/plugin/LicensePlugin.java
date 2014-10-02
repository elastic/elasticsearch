/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin;

import org.elasticsearch.action.ActionModule;
import org.elasticsearch.license.plugin.action.get.GetLicenseAction;
import org.elasticsearch.license.plugin.action.get.TransportGetLicenseAction;
import org.elasticsearch.license.plugin.action.put.PutLicenseAction;
import org.elasticsearch.license.plugin.action.put.TransportPutLicenseAction;
import org.elasticsearch.license.plugin.rest.RestGetLicenseAction;
import org.elasticsearch.license.plugin.rest.RestPutLicenseAction;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;

//TODO: plugin hooks
public class LicensePlugin extends AbstractPlugin {

    @Override
    public String name() {
        return "license";
    }

    @Override
    public String description() {
        return "Internal Elasticsearch Licensing Plugin";
    }

    public void onModule(RestModule module) {
        // Register REST endpoint
        module.addRestAction(RestPutLicenseAction.class);
        module.addRestAction(RestGetLicenseAction.class);
    }

    public void onModule(ActionModule module) {
        module.registerAction(PutLicenseAction.INSTANCE, TransportPutLicenseAction.class);
        module.registerAction(GetLicenseAction.INSTANCE, TransportGetLicenseAction.class);
    }
}
