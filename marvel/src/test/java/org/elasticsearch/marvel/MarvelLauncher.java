/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel;

import org.elasticsearch.bootstrap.Elasticsearch;
import org.elasticsearch.license.plugin.LicensePlugin;

/**
 * Main class to easily run Marvel from a IDE.
 * <p/>
 * In order to run this class set configure the following:
 * 1) Set `-Des.path.home=` to a directory containing an ES config directory
 */
public class MarvelLauncher {

    public static void main(String[] args) throws Throwable {
        System.setProperty("es.script.inline", "on");
        System.setProperty("es.security.manager.enabled", "false");
        System.setProperty("es.plugins.load_classpath_plugins", "false");
        System.setProperty("es.plugin.types", MarvelPlugin.class.getName() + "," + LicensePlugin.class.getName());
        System.setProperty("es.cluster.name", MarvelLauncher.class.getSimpleName());

        Elasticsearch.main(new String[]{"start"});
    }

}
