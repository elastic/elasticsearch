/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.packaging.test;

import org.elasticsearch.packaging.util.ServerUtils;
import org.elasticsearch.packaging.util.docker.DockerRun;

import java.util.Map;

import static org.elasticsearch.packaging.util.docker.Docker.runContainer;
import static org.elasticsearch.packaging.util.docker.DockerRun.builder;

public class MemoryLockingTests extends PackagingTestCase {

    public void test10Install() throws Exception {
        install();
    }

    public void test20MemoryLockingEnabled() throws Exception {
        configureAndRun(
            Map.of(
                "bootstrap.memory_lock",
                "true",
                "xpack.security.enabled",
                "false",
                "xpack.security.http.ssl.enabled",
                "false",
                "xpack.security.enrollment.enabled",
                "false",
                "discovery.type",
                "single-node"
            )
        );
        // TODO: very locking worked. logs? check memory of process? at least we know the process started successfully
        stopElasticsearch();
    }

    public void configureAndRun(Map<String, String> settings) throws Exception {
        if (distribution().isDocker()) {
            DockerRun builder = builder();
            settings.forEach(builder::envVar);
            runContainer(distribution(), builder);
        } else {

            for (var setting : settings.entrySet()) {
                ServerUtils.addSettingToExistingConfiguration(installation.config, setting.getKey(), setting.getValue());
            }
            ServerUtils.removeSettingFromExistingConfiguration(installation.config, "cluster.initial_master_nodes");
        }

        startElasticsearch();
    }
}
