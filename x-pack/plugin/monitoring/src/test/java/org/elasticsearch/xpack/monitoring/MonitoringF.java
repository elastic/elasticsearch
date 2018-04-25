/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.node.MockNode;
import org.elasticsearch.node.Node;
import org.elasticsearch.xpack.core.XPackPlugin;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

/**
 * Main class to easily run Monitoring from a IDE.
 * <p>
 * In order to run this class set configure the following:
 * 1) Set `-Des.path.home=` to a directory containing an ES config directory
 *
 * It accepts collectors names as program arguments.
 */
public class MonitoringF {

    public static void main(String[] args) throws Throwable {
        Settings.Builder settings = Settings.builder();
        settings.put("security.manager.enabled", "false");
        settings.put("cluster.name", MonitoringF.class.getSimpleName());
        settings.put("xpack.monitoring.collection.interval", "1s");
        if (!CollectionUtils.isEmpty(args)) {
            settings.putList("xpack.monitoring.collection.collectors", args);
        }

        final CountDownLatch latch = new CountDownLatch(1);
        final Node node = new MockNode(settings.build(),
                Arrays.asList(XPackPlugin.class, XPackPlugin.class, XPackPlugin.class));
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
