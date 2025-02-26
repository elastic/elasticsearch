/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import net.jqwik.api.Property;
import net.jqwik.api.lifecycle.AfterContainer;
import net.jqwik.api.lifecycle.BeforeContainer;

import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;

public class StandardVsLogsDbRestTest {
    public static OnDemandLocalCluster cluster = new OnDemandClusterBuilder().distribution(DistributionType.DEFAULT)
        .module("data-streams")
        .module("x-pack-stack")
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("cluster.logsdb.enabled", "true")
        .build();

    @BeforeContainer
    static void beforeContainer() {
        LogConfigurator.loadLog4jPlugins();
        LogConfigurator.configureESLogging();

        cluster.init();
    }

    @AfterContainer
    static void afterContainer() {
        cluster.teardown();
    }

    @Property
    public void testStuff() {
        System.out.println("hey");
    }
}
