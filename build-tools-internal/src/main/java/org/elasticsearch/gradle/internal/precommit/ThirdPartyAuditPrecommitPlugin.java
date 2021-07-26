/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.precommit;

import org.elasticsearch.gradle.internal.ExportElasticsearchBuildResourcesTask;
import org.elasticsearch.gradle.dependencies.CompileOnlyResolvePlugin;
import org.elasticsearch.gradle.internal.info.BuildParams;
import org.elasticsearch.gradle.internal.InternalPlugin;
import org.elasticsearch.gradle.internal.conventions.precommit.PrecommitPlugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.internal.jvm.Jvm;

import java.nio.file.Path;

public class ThirdPartyAuditPrecommitPlugin extends PrecommitPlugin implements InternalPlugin {

    public static final String JDK_JAR_HELL_CONFIG_NAME = "jdkJarHell";
    public static final String LIBS_ELASTICSEARCH_CORE_PROJECT_PATH = ":libs:elasticsearch-core";

    @Override
    public TaskProvider<? extends Task> createTask(Project project) {
        project.getPlugins().apply(CompileOnlyResolvePlugin.class);
        project.getConfigurations().create("forbiddenApisCliJar");
        project.getDependencies().add("forbiddenApisCliJar", "de.thetaphi:forbiddenapis:3.1");
        Configuration jdkJarHellConfig = project.getConfigurations().create(JDK_JAR_HELL_CONFIG_NAME);
        if (project.getPath().equals(LIBS_ELASTICSEARCH_CORE_PROJECT_PATH) == false) {
            // Internal projects are not all plugins, so make sure the check is available
            // we are not doing this for this project itself to avoid jar hell with itself
            project.getDependencies().add(JDK_JAR_HELL_CONFIG_NAME, project.project(LIBS_ELASTICSEARCH_CORE_PROJECT_PATH));
        }

        TaskProvider<ExportElasticsearchBuildResourcesTask> resourcesTask = project.getTasks()
            .register("thirdPartyAuditResources", ExportElasticsearchBuildResourcesTask.class);
        Path resourcesDir = project.getBuildDir().toPath().resolve("third-party-audit-config");
        resourcesTask.configure(t -> {
            t.setOutputDir(resourcesDir.toFile());
            t.copy("forbidden/third-party-audit.txt");
        });
        TaskProvider<ThirdPartyAuditTask> audit = project.getTasks().register("thirdPartyAudit", ThirdPartyAuditTask.class);
        audit.configure(t -> {
            t.dependsOn(resourcesTask);
            t.setJavaHome(Jvm.current().getJavaHome().getPath());
            t.getTargetCompatibility().set(project.provider(BuildParams::getRuntimeJavaVersion));
            t.setSignatureFile(resourcesDir.resolve("forbidden/third-party-audit.txt").toFile());
        });
        project.getTasks().withType(ThirdPartyAuditTask.class).configureEach(t -> t.setJdkJarHellClasspath(jdkJarHellConfig));
        return audit;
    }
}
