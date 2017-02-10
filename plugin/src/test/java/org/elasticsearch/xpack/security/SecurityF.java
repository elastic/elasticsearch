/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.MockNode;
import org.elasticsearch.node.Node;
import org.elasticsearch.xpack.security.authc.esnative.NativeRealm;
import org.elasticsearch.xpack.security.authc.file.FileRealm;
import org.elasticsearch.xpack.security.test.SecurityTestUtils;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.xpack.XPackPlugin;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

import static org.elasticsearch.xpack.security.test.SecurityTestUtils.writeFile;

/**
 * Main class to easily run X-Pack Security from a IDE.
 *
 * During startup an error will be printed that the config directory can't be found, to fix this:
 * set `-Des.path.home=` to a location where there is a config directory on your machine.
 */
public class SecurityF {

    public static void main(String[] args) throws Throwable {
        Settings.Builder settings = Settings.builder();
        settings.put("http.cors.enabled", "true");
        settings.put("http.cors.allow-origin", "*");
        settings.put("script.inline", "true");
        settings.put("xpack.security.enabled", "true");
        // Disable Monitoring to prevent cluster activity
        settings.put("xpack.monitoring.enabled", "false");
        settings.put("cluster.name", SecurityF.class.getSimpleName());

        String homeDir = System.getProperty("es.path.home");
        if (homeDir == null || Files.exists(PathUtils.get(homeDir)) == false) {
            throw new IllegalStateException("es.path.home must be set and exist");
        }
        Path folder = SecurityTestUtils.createFolder(SecurityTestUtils.createFolder(PathUtils.get(homeDir), "config"), "x-pack");
        settings.put(Environment.PATH_CONF_SETTING.getKey(), PathUtils.get(homeDir).resolve("config"));
        writeFile(folder, "users", SecuritySettingsSource.CONFIG_STANDARD_USER);
        writeFile(folder, "users_roles", SecuritySettingsSource.CONFIG_STANDARD_USER_ROLES);
        writeFile(folder, "roles.yml", SecuritySettingsSource.CONFIG_ROLE_ALLOW_ALL);

        settings.put("xpack.security.authc.realms.file.type", FileRealm.TYPE);
        settings.put("xpack.security.authc.realms.file.order", "0");
        settings.put("xpack.security.authc.realms.esnative.type", NativeRealm.TYPE);
        settings.put("xpack.security.authc.realms.esnative.order", "1");

        final CountDownLatch latch = new CountDownLatch(1);
        final Node node = new MockNode(settings.build(), Arrays.asList(XPackPlugin.class));
        Runtime.getRuntime().addShutdownHook(new Thread() {

            @Override
            public void run() {
                try {
                    IOUtils.close(node);
                } catch (IOException ex) {
                    throw new ElasticsearchException(ex);
                } finally {
                    latch.countDown();
                }
            }
        });
        node.start();
        latch.await();
    }
}
