/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher;

import org.elasticsearch.bootstrap.Elasticsearch;
import org.elasticsearch.license.plugin.LicensePlugin;

/**
 * Main class to easily run Watcher from a IDE.
 * It sets all the options to run the Watcher plugin and access it from Sense, but doesn't run with Shield.
 *
 * During startup an error will be printed that the config directory can't be found, to fix this:
 * 1) Add a config directly to the top level project directory
 * 2) or set `-Des.path.home=` to a location where there is a config directory on your machine.
 */
public class WatcherF {

    public static void main(String[] args) throws Throwable {
        System.setProperty("es.http.cors.enabled", "true");
        System.setProperty("es.script.inline", "on");
        System.setProperty("es.shield.enabled", "false");
        System.setProperty("es.security.manager.enabled", "false");
        System.setProperty("es.plugins.load_classpath_plugins", "false");
        System.setProperty("es.plugin.types", WatcherPlugin.class.getName() + "," + LicensePlugin.class.getName());
        System.setProperty("es.cluster.name", WatcherF.class.getSimpleName());

        Elasticsearch.main(args);
    }

}
