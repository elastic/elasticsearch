/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel;

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.plugin.LicensePlugin;
import org.elasticsearch.node.MockNode;
import org.elasticsearch.node.Node;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

/**
 * Main class to easily run Marvel from a IDE.
 * <p/>
 * In order to run this class set configure the following:
 * 1) Set `-Des.path.home=` to a directory containing an ES config directory
 */
public class MarvelLauncher {

    public static void main(String[] args) throws Throwable {
        Settings.Builder settings = Settings.builder();
        settings.put("script.inline", "on");
        settings.put("security.manager.enabled", "false");
        settings.put("plugins.load_classpath_plugins", "false");
        settings.put("cluster.name", MarvelLauncher.class.getSimpleName());

        final CountDownLatch latch = new CountDownLatch(1);
        final Node node = new MockNode(settings.build(), false, Version.CURRENT, Arrays.asList(MarvelPlugin.class, LicensePlugin.class));
        Runtime.getRuntime().addShutdownHook(new Thread() {

            @Override
            public void run() {
                node.close();
                latch.countDown();
            }
        });
        node.start();
        latch.await();
    }

}
