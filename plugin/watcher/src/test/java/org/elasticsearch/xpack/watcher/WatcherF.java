/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.node.MockNode;
import org.elasticsearch.node.Node;
import org.elasticsearch.xpack.core.XPackPlugin;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

/**
 * Main class to easily run Watcher from a IDE.
 * It sets all the options to run the Watcher plugin and access it from Sense, but doesn't run with security.
 *
 * In order to run this class set configure the following:
 * 1) Set `-Des.path.home=` to a directory containing an ES config directory
 */
public class WatcherF {

    @SuppressForbidden(reason = "not really code or a test")
    public static void main(String[] args) throws Throwable {
        Settings.Builder settings = Settings.builder();
        settings.put("http.cors.enabled", "true");
        settings.put("http.cors.allow-origin", "*");
        settings.put("xpack.security.enabled", "false");
        settings.put("security.manager.enabled", "false");
        settings.put("cluster.name", WatcherF.class.getSimpleName());

        // this is for the `test-watcher-integration` group level integration in HipChat
        settings.put("xpack.notification.hipchat.account.integration.profile", "integration");
        settings.put("xpack.notification.hipchat.account.integration.auth_token", "huuS9v7ccuOy3ZBWWWr1vt8Lqu3sQnLUE81nrLZU");
        settings.put("xpack.notification.hipchat.account.integration.room", "test-watcher");

        // this is for the Watcher Test account in HipChat
        settings.put("xpack.notification.hipchat.account.user.profile", "user");
        settings.put("xpack.notification.hipchat.account.user.auth_token", "12rNQUuQ0wObfRVeoVD8OeoAnosCT8tSTV5UjsII");

        // this is for the `test-watcher-v1` notification token (hipchat)
        settings.put("xpack.notification.hipchat.account.v1.profile", "v1");
        settings.put("xpack.notification.hipchat.account.v1.auth_token", "a734baf62df618b96dda55b323fc30");

        // this is for our test slack incoming webhook (under elasticsearch team)
        System.setProperty("es.xpack.notification.slack.account.a1.url",
                "https://hooks.slack.com/services/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");

        System.setProperty("es.xpack.notification.pagerduty.account.service1.service_api_key",
                "fc082467005d4072a914e0bb041882d0");

        final CountDownLatch latch = new CountDownLatch(1);
        final Node node = new MockNode(settings.build(), Arrays.asList(XPackPlugin.class, XPackPlugin.class));
        Runtime.getRuntime().addShutdownHook(new Thread() {

            @Override
            public void run() {
                try {
                    IOUtils.close(node);
                } catch (IOException e) {
                    throw new ElasticsearchException(e);
                } finally {
                    latch.countDown();
                }
            }
        });
        node.start();
        latch.await();
    }

}
