/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.run

import org.elasticsearch.gradle.fixtures.AbstractProjectBuilderPluginSpec
import org.elasticsearch.gradle.internal.info.GlobalBuildInfoPlugin
import org.gradle.api.Project
import org.gradle.api.artifacts.ProjectDependency
import spock.lang.TempDir

class InternalRunPluginSpec extends AbstractProjectBuilderPluginSpec {

    @TempDir
    File workspace

    Project rootProject

    @Override
    Class<InternalRunPlugin> getPluginClassUnderTest() {
        return InternalRunPlugin
    }

    def setup() {
        new File(workspace, ".ci").mkdirs()
        new File(workspace, ".ci/dockerOnLinuxExclusions").text = ""
        new File(workspace, "branches.json").text = """
            {
              "branches": [
                { "branch": "main", "version": "9.1.0" },
                { "branch": "9.0", "version": "9.0.3" }
              ]
            }
        """
        writeMinimalElasticsearchRepoLayout(workspace)

        rootProject = buildProject("elasticsearch", null, workspace)
        // mirrors settings.gradle, which applies elasticsearch.global-build-info to the root project of the build
        rootProject.pluginManager.apply(GlobalBuildInfoPlugin)
        rootProject.gradle.startParameter.offline = true
    }

    def "applies conventions to run tasks"() {
        given:
        applyPluginUnderTest(rootProject)

        when:
        RunTask task = rootProject.tasks.register("run", RunTask).get()

        then:
        task.clusterName.get() == "runTask"
        task.distribution.get() == "DEFAULT"
        task.numberOfNodes.get() == 1
        task.httpPort.get() == 9200
        task.transportPort.get() == 9300
        task.debugServerPort.get() == 5007
        task.debugCliPort.get() == 5107
        task.workingDir.get().asFile == new File(rootProject.layout.buildDirectory.get().asFile, "testclusters")
        task.debugJvm.get() == false
        task.preserveData.get() == false
        rootProject.configurations.getByName(InternalRunPlugin.CLUSTER_RUNNER_CONFIGURATION).canBeResolved
    }

    def "plugin and module projects are added to the task's own configurations"() {
        given:
        Project plugins = buildProject("plugins", rootProject)
        buildProject("analysis-icu", plugins)
        buildProject("my-plugin", rootProject)
        Project modules = buildProject("modules", rootProject)
        buildProject("my-module", modules)
        applyPluginUnderTest(rootProject)

        when:
        RunTask task = rootProject.tasks.register("run", RunTask) {
            it.withPlugins = "analysis-icu"
            it.plugin(":my-plugin")
            it.module(":modules:my-module")
        }.get()

        then:
        projectDependencies(task.pluginsConfigurationName()) == [":plugins:analysis-icu[zip]", ":my-plugin[zip]"]
        projectDependencies(task.modulesConfigurationName()) == [":modules:my-module[null]"]
    }

    def "plugin and module run tasks use the integration test distribution"() {
        given:
        Project test = buildProject("test", rootProject)
        buildProject("test-clusters-cli", test)
        Project plugins = buildProject("plugins", rootProject)
        Project pluginProject = buildProject("my-plugin", plugins)
        pluginProject.tasks.register("bundlePlugin")

        when:
        InternalRunPlugin.registerPluginRunTask(pluginProject)
        RunTask task = pluginProject.tasks.named("run", RunTask).get()

        then:
        task.distribution.get() == "INTEG_TEST"
        pluginProject.configurations.getByName(task.pluginsConfigurationName()).dependencies.size() == 1
    }

    def "no plugin run task is registered without the cluster runner project"() {
        given:
        Project pluginProject = buildProject("my-plugin", rootProject)

        when:
        InternalRunPlugin.registerPluginRunTask(pluginProject)

        then:
        pluginProject.tasks.findByName("run") == null
        pluginProject.plugins.hasPlugin(InternalRunPlugin) == false
    }

    private List<String> projectDependencies(String configurationName) {
        return rootProject.configurations.getByName(configurationName).dependencies.collect { dependency ->
            ProjectDependency projectDependency = (ProjectDependency) dependency
            "${projectDependency.path}[${projectDependency.targetConfiguration}]".toString()
        }
    }
}
