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
import org.elasticsearch.gradle.precommit.ForbiddenPatternsTask;
import org.elasticsearch.gradle.testclusters.ElasticsearchCluster;
import org.elasticsearch.gradle.testclusters.TestClustersPlugin;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.TaskProvider;

import java.io.File;

public class TestWithSslPlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {
        File keyStoreDir = new File(project.getBuildDir(), "keystore");
        TaskProvider<ExportElasticsearchBuildResourcesTask> exportKeyStore = project.getTasks()
            .register("copyTestCertificates", ExportElasticsearchBuildResourcesTask.class, (t) -> {
                t.copy("test/ssl/test-client.cert");
                t.copy("test/ssl/test-client.jks");
                t.copy("test/ssl/test-node.cert");
                t.copy("test/ssl/test-node.jks");
                t.setOutputDir(keyStoreDir);
            });

        project.getPlugins().withType(StandaloneRestTestPlugin.class).configureEach(restTestPlugin -> {
            project.getTasks().named(JavaPlugin.PROCESS_TEST_RESOURCES_TASK_NAME);
            SourceSetContainer sourceSets = project.getExtensions().getByType(SourceSetContainer.class);
            SourceSet testSourceSet = sourceSets.getByName("test");
            testSourceSet.getResources().srcDir(new File(keyStoreDir, "test/ssl"));
            testSourceSet.compiledBy(exportKeyStore);

            project.getTasks()
                .withType(org.elasticsearch.gradle.testclusters.TestClustersAware.class)
                .configureEach(clusterAware -> clusterAware.dependsOn(exportKeyStore));

            // Tell the tests we're running with ssl enabled
            project.getTasks()
                .withType(RestIntegTestTask.class)
                .configureEach(integTest -> integTest.runner.systemProperty("tests.ssl.enabled", "true"));
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
            .configureEach(forbiddenPatternTask -> forbiddenPatternTask.exclude("**/*.cert"));
    }
}
