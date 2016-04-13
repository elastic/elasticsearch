/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield;

import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.MockNode;
import org.elasticsearch.node.Node;
import org.elasticsearch.shield.authc.esnative.NativeRealm;
import org.elasticsearch.shield.authc.file.FileRealm;
import org.elasticsearch.shield.authz.store.FileRolesStore;
import org.elasticsearch.shield.test.ShieldTestUtils;
import org.elasticsearch.test.ShieldSettingsSource;
import org.elasticsearch.xpack.XPackPlugin;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

import static org.elasticsearch.shield.test.ShieldTestUtils.writeFile;

/**
 * Main class to easily run Shield from a IDE.
 *
 * During startup an error will be printed that the config directory can't be found, to fix this:
 * set `-Des.path.home=` to a location where there is a config directory on your machine.
 */
public class ShieldF {

    public static void main(String[] args) throws Throwable {
        Settings.Builder settings = Settings.builder();
        settings.put("http.cors.enabled", "true");
        settings.put("http.cors.allow-origin", "*");
        settings.put("script.inline", "true");
        settings.put("xpack.shield.enabled", "true");
        // Disable Monitoring to prevent cluster activity
        settings.put("xpack.monitoring.enabled", "false");
        settings.put("cluster.name", ShieldF.class.getSimpleName());

        String homeDir = System.getProperty("es.path.home");
        if (homeDir == null || Files.exists(PathUtils.get(homeDir)) == false) {
            throw new IllegalStateException("es.path.home must be set and exist");
        }
        Path folder = ShieldTestUtils.createFolder(ShieldTestUtils.createFolder(PathUtils.get(homeDir), "config"), "shield");

        settings.put("xpack.security.authc.realms.file.type", FileRealm.TYPE);
        settings.put("xpack.security.authc.realms.file.order", "0");
        settings.put("xpack.security.authc.realms.file.files.users",
                writeFile(folder, "users", ShieldSettingsSource.CONFIG_STANDARD_USER));
        settings.put("xpack.security.authc.realms.file.files.users_roles", writeFile(folder, "users_roles",
                ShieldSettingsSource.CONFIG_STANDARD_USER_ROLES));
        settings.put("xpack.security.authc.realms.esnative.type", NativeRealm.TYPE);
        settings.put("xpack.security.authc.realms.esnative.order", "1");
        settings.put(FileRolesStore.ROLES_FILE_SETTING.getKey(),
                writeFile(folder, "roles.yml", ShieldSettingsSource.CONFIG_ROLE_ALLOW_ALL));

        final CountDownLatch latch = new CountDownLatch(1);
        final Node node = new MockNode(settings.build(), Version.CURRENT, Arrays.asList(XPackPlugin.class));
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
