/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.test;

import org.elasticsearch.gradle.internal.ExportElasticsearchBuildResourcesTask;
import org.elasticsearch.gradle.internal.conventions.util.Util;
import org.elasticsearch.gradle.internal.info.BuildParameterExtension;
import org.elasticsearch.gradle.internal.precommit.FilePermissionsPrecommitPlugin;
import org.elasticsearch.gradle.internal.precommit.ForbiddenPatternsPrecommitPlugin;
import org.elasticsearch.gradle.internal.precommit.ForbiddenPatternsTask;
import org.elasticsearch.gradle.internal.test.rest.LegacyJavaRestTestPlugin;
import org.elasticsearch.gradle.testclusters.ElasticsearchCluster;
import org.elasticsearch.gradle.testclusters.TestClustersAware;
import org.elasticsearch.gradle.testclusters.TestClustersPlugin;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.TaskProvider;

import java.io.File;

import static org.elasticsearch.gradle.internal.precommit.FilePermissionsPrecommitPlugin.FILEPERMISSIONS_TASK_NAME;
import static org.elasticsearch.gradle.internal.precommit.ForbiddenPatternsPrecommitPlugin.FORBIDDEN_PATTERNS_TASK_NAME;

public class TestWithSslPlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {
        File keyStoreDir = new File(project.getBuildDir(), "keystore");
        var buildParams = project.getRootProject().getExtensions().getByType(BuildParameterExtension.class);
        TaskProvider<ExportElasticsearchBuildResourcesTask> exportKeyStore = project.getTasks()
            .register("copyTestCertificates", ExportElasticsearchBuildResourcesTask.class, (t) -> {
                t.copy("test/ssl/test-client.crt");
                t.copy("test/ssl/test-client.key");
                t.copy("test/ssl/test-client.jks");
                t.copy("test/ssl/test-node.crt");
                t.copy("test/ssl/test-node.key");
                t.copy("test/ssl/test-node.jks");
                t.setOutputDir(keyStoreDir);
            });
        project.getPlugins()
            .withType(ForbiddenPatternsPrecommitPlugin.class)
            .configureEach(plugin -> project.getTasks().named(FORBIDDEN_PATTERNS_TASK_NAME).configure(t -> t.dependsOn(exportKeyStore)));
        project.getPlugins()
            .withType(FilePermissionsPrecommitPlugin.class)
            .configureEach(
                filePermissionPlugin -> project.getTasks().named(FILEPERMISSIONS_TASK_NAME).configure(t -> t.dependsOn(exportKeyStore))
            );
        project.getPlugins().withType(StandaloneRestTestPlugin.class).configureEach(restTestPlugin -> {
            SourceSet testSourceSet = Util.getJavaTestSourceSet(project).get();
            testSourceSet.getResources().srcDir(new File(keyStoreDir, "test/ssl"));
            project.getTasks().named(testSourceSet.getProcessResourcesTaskName()).configure(t -> t.dependsOn(exportKeyStore));
            project.getTasks().withType(TestClustersAware.class).configureEach(clusterAware -> clusterAware.dependsOn(exportKeyStore));
            // Tell the tests we're running with ssl enabled
            project.getTasks()
                .withType(RestIntegTestTask.class)
                .configureEach(runner -> runner.systemProperty("tests.ssl.enabled", "true"));
        });
        project.getPlugins().withType(LegacyJavaRestTestPlugin.class).configureEach(restTestPlugin -> {
            SourceSet testSourceSet = Util.getJavaSourceSets(project).getByName(LegacyJavaRestTestPlugin.SOURCE_SET_NAME);
            testSourceSet.getResources().srcDir(new File(keyStoreDir, "test/ssl"));
            project.getTasks().named(testSourceSet.getProcessResourcesTaskName()).configure(t -> t.dependsOn(exportKeyStore));
            project.getTasks().withType(TestClustersAware.class).configureEach(clusterAware -> clusterAware.dependsOn(exportKeyStore));
            // Tell the tests we're running with ssl enabled
            project.getTasks()
                .withType(RestIntegTestTask.class)
                .configureEach(runner -> runner.systemProperty("tests.ssl.enabled", "true"));
        });

        project.getPlugins().withType(TestClustersPlugin.class).configureEach(clustersPlugin -> {
            File keyMaterialDir = new File(project.getBuildDir(), "keystore/test/ssl");
            File nodeKeystore = new File(keyMaterialDir, "test-node.jks");
            File nodeCertificate = new File(keyMaterialDir, "test-node.crt");
            File nodeKey = new File(keyMaterialDir, "test-node.key");
            File clientKeyStore = new File(keyMaterialDir, "test-client.jks");
            File clientCertificate = new File(keyMaterialDir, "test-client.crt");
            File clientKey = new File(keyMaterialDir, "test-client.key");
            @SuppressWarnings("unchecked")
            NamedDomainObjectContainer<ElasticsearchCluster> clusters = (NamedDomainObjectContainer<ElasticsearchCluster>) project
                .getExtensions()
                .getByName(TestClustersPlugin.EXTENSION_NAME);
            clusters.configureEach(c -> {
                if (buildParams.getInFipsJvm()) {
                    c.setting("xpack.security.transport.ssl.key", "test-node.key");
                    c.keystore("xpack.security.transport.ssl.secure_key_passphrase", "test-node-key-password");
                    c.setting("xpack.security.transport.ssl.certificate", "test-node.crt");
                    c.setting("xpack.security.http.ssl.key", "test-node.key");
                    c.keystore("xpack.security.http.ssl.secure_key_passphrase", "test-node-key-password");
                    c.setting("xpack.security.http.ssl.certificate", "test-node.crt");
                    c.extraConfigFile(nodeKey.getName(), nodeKey);
                    c.extraConfigFile(nodeCertificate.getName(), nodeCertificate);
                    c.extraConfigFile(clientKey.getName(), clientKey);
                    c.extraConfigFile(clientCertificate.getName(), clientCertificate);
                } else {
                    // ceremony to set up ssl
                    c.setting("xpack.security.transport.ssl.keystore.path", "test-node.jks");
                    c.setting("xpack.security.http.ssl.keystore.path", "test-node.jks");
                    c.keystore("xpack.security.transport.ssl.keystore.secure_password", "keypass");
                    c.keystore("xpack.security.http.ssl.keystore.secure_password", "keypass");
                    // copy keystores & certs into config/
                    c.extraConfigFile(nodeKeystore.getName(), nodeKeystore);
                    c.extraConfigFile(clientKeyStore.getName(), clientKeyStore);
                }
            });
        });

        project.getTasks()
            .withType(ForbiddenPatternsTask.class)
            .configureEach(forbiddenPatternTask -> forbiddenPatternTask.exclude("**/*.crt", "**/*.key"));
    }
}
