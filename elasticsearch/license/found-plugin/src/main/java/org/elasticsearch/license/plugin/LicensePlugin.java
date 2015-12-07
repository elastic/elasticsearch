/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin;

import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.plugin.core.FoundLicensesService;
import org.elasticsearch.plugins.Plugin;

import java.util.ArrayList;
import java.util.Collection;

public class LicensePlugin extends Plugin {

    public static final String NAME = "license";

    @Inject
    public LicensePlugin(Settings settings) {
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public String description() {
        return "Internal Elasticsearch Licensing Plugin";
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> nodeServices() {
        Collection<Class<? extends LifecycleComponent>> services = new ArrayList<>();
        services.add(FoundLicensesService.class);
        return services;
    }

    @Override
    public Collection<Module> nodeModules() {
        Collection<Module> modules = new ArrayList<Module>();
        modules.add(new LicenseModule());
        return modules;
    }
}
