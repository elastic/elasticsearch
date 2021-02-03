/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.test;

import org.elasticsearch.gradle.ExportElasticsearchBuildResourcesTask;
import org.elasticsearch.gradle.internal.precommit.ForbiddenPatternsTask;
import org.elasticsearch.gradle.testclusters.ElasticsearchCluster;
import org.elasticsearch.gradle.testclusters.TestClustersAware;
import org.elasticsearch.gradle.testclusters.TestClustersPlugin;
import org.elasticsearch.gradle.util.Util;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.TaskProvider;

import java.io.File;

public class TestWithSslPlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {
        File keyStoreDir = new File(project.getBuildDir(), "keystore");
        TaskProvider<ExportElasticsearchBuildResourcesTask> exportKeyStore = project.getTasks()
            .register("copyTestCertificates", ExportElasticsearchBuildResourcesTask.class, (t) -> {
                t.copy("test/ssl/test-client.crt");
                t.copy("test/ssl/test-client.jks");
                t.copy("test/ssl/test-node.crt");
                t.copy("test/ssl/test-node.jks");
                t.setOutputDir(keyStoreDir);
            });

        project.getPlugins().withType(StandaloneRestTestPlugin.class).configureEach(restTestPlugin -> {
            SourceSet testSourceSet = Util.getJavaTestSourceSet(project).get();
            testSourceSet.getResources().srcDir(new File(keyStoreDir, "test/ssl"));
            testSourceSet.compiledBy(exportKeyStore);

            project.getTasks().withType(TestClustersAware.class).configureEach(clusterAware -> clusterAware.dependsOn(exportKeyStore));

            // Tell the tests we're running with ssl enabled
            project.getTasks()
                .withType(RestIntegTestTask.class)
                .configureEach(runner -> runner.systemProperty("tests.ssl.enabled", "true"));
        });

        project.getPlugins().withType(TestClustersPlugin.class).configureEach(clustersPlugin -> {
            File keystoreDir = new File(project.getBuildDir(), "keystore/test/ssl");
            File nodeKeystore = new File(keystoreDir, "test-node.jks");
            File clientKeyStore = new File(keystoreDir, "test-client.jks");
            NamedDomainObjectContainer<ElasticsearchCluster> clusters = (NamedDomainObjectContainer<ElasticsearchCluster>) project
                .getExtensions()
                .getByName(TestClustersPlugin.EXTENSION_NAME);
            clusters.all(c -> {
                // ceremony to set up ssl
                c.setting("xpack.security.transport.ssl.keystore.path", "test-node.jks");
                c.setting("xpack.security.http.ssl.keystore.path", "test-node.jks");
                c.keystore("xpack.security.transport.ssl.keystore.secure_password", "keypass");
                c.keystore("xpack.security.http.ssl.keystore.secure_password", "keypass");

                // copy keystores & certs into config/
                c.extraConfigFile(nodeKeystore.getName(), nodeKeystore);
                c.extraConfigFile(clientKeyStore.getName(), clientKeyStore);
            });
        });

        project.getTasks()
            .withType(ForbiddenPatternsTask.class)
            .configureEach(forbiddenPatternTask -> forbiddenPatternTask.exclude("**/*.crt"));
    }
}
