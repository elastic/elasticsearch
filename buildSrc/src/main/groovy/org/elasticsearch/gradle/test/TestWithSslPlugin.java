/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.gradle.test;

import org.elasticsearch.gradle.ExportElasticsearchBuildResourcesTask;
import org.elasticsearch.gradle.info.BuildParams;
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
                t.copy("test/ssl/test-client.key");
                t.copy("test/ssl/test-client.jks");
                t.copy("test/ssl/test-node.crt");
                t.copy("test/ssl/test-node.key");
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
            File keyMaterialDir = new File(project.getBuildDir(), "keystore/test/ssl");
            File nodeKeystore = new File(keyMaterialDir, "test-node.jks");
            File nodeCertificate = new File(keyMaterialDir, "test-node.crt");
            File nodeKey = new File(keyMaterialDir, "test-node.key");
            File clientKeyStore = new File(keyMaterialDir, "test-client.jks");
            File clientCertificate = new File(keyMaterialDir, "test-client.crt");
            File clientKey = new File(keyMaterialDir, "test-client.key");
            NamedDomainObjectContainer<ElasticsearchCluster> clusters = (NamedDomainObjectContainer<ElasticsearchCluster>) project
                .getExtensions()
                .getByName(TestClustersPlugin.EXTENSION_NAME);
            clusters.all(c -> {
                if (BuildParams.isInFipsJvm()) {
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
