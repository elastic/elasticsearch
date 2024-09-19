/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.packaging.test;

import org.elasticsearch.packaging.util.ServerUtils;
import org.elasticsearch.packaging.util.docker.DockerRun;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.elasticsearch.packaging.util.docker.Docker.runContainer;
import static org.elasticsearch.packaging.util.docker.DockerRun.builder;

public class BootstrapCheckTests extends PackagingTestCase {

    public void test10Install() throws Exception {
        install();
    }

    public void test20RunWithBootstrapChecks() throws Exception {
        configureBootstrapChecksAndRun(
            Map.of(
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
        stopElasticsearch();
    }

    private void configureBootstrapChecksAndRun(Map<String, String> settings) throws Exception {
        if (distribution().isDocker()) {
            DockerRun builder = builder().envVar("ES_JAVA_OPTS", "-Des.enforce.bootstrap.checks=true");
            settings.forEach(builder::envVar);
            runContainer(distribution(), builder);
        } else {
            Path jvmOptionsDir = installation.config.resolve("jvm.options.d");
            Path enableBootstrap = jvmOptionsDir.resolve("enable_bootstrap.options");
            Files.writeString(enableBootstrap, "-Des.enforce.bootstrap.checks=true");

            for (var setting : settings.entrySet()) {
                ServerUtils.addSettingToExistingConfiguration(installation.config, setting.getKey(), setting.getValue());
            }
            ServerUtils.removeSettingFromExistingConfiguration(installation.config, "cluster.initial_master_nodes");
        }

        startElasticsearch();
    }
}
