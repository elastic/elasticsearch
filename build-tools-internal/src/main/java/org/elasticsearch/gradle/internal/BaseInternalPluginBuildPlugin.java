/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal;

import groovy.lang.Closure;

import org.elasticsearch.gradle.internal.conventions.util.Util;
import org.elasticsearch.gradle.plugin.PluginBuildPlugin;
import org.elasticsearch.gradle.plugin.PluginPropertiesExtension;
import org.elasticsearch.gradle.testclusters.ElasticsearchCluster;
import org.elasticsearch.gradle.testclusters.TestClustersPlugin;
import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.tasks.bundling.Zip;

import java.util.Optional;

/**
 * Base plugin for building internal plugins or modules. This plugin should only be directly applied by internal test-only plugins or
 * modules. That is, plugins not externally published and modules only included in snapshot builds. Otherwise
 * {@link InternalPluginBuildPlugin} should be used.
 */
public class BaseInternalPluginBuildPlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {
        project.getPluginManager().apply(PluginBuildPlugin.class);
        project.getPluginManager().apply(ElasticsearchJavaPlugin.class);
        // Clear default dependencies added by public PluginBuildPlugin as we add our
        // own project dependencies for internal builds
        // TODO remove once we removed default dependencies from PluginBuildPlugin
        project.getConfigurations().getByName("compileOnly").getDependencies().clear();
        project.getConfigurations().getByName("testImplementation").getDependencies().clear();

        var extension = project.getExtensions().getByType(PluginPropertiesExtension.class);

        // We've ported this from multiple build scripts where we see this pattern into
        // an extension method as a first step of consolidation.
        // We might want to port this into a general pattern later on.
        project.getExtensions()
            .getExtraProperties()
            .set("addQaCheckDependencies", new Closure<Object>(BaseInternalPluginBuildPlugin.this, BaseInternalPluginBuildPlugin.this) {
                public void doCall(Object it) {
                    project.afterEvaluate(project1 -> {
                        // let check depend on check tasks of qa sub-projects
                        final var checkTaskProvider = project1.getTasks().named("check");
                        Optional<Project> qaSubproject = project1.getSubprojects()
                            .stream()
                            .filter(p -> p.getPath().equals(project1.getPath() + ":qa"))
                            .findFirst();
                        qaSubproject.ifPresent(
                            qa -> qa.getSubprojects()
                                .forEach(p -> checkTaskProvider.configure(task -> task.dependsOn(p.getPath() + ":check")))
                        );
                    });
                }

                public void doCall() {
                    doCall(null);
                }
            });

        project.afterEvaluate(p -> {
            boolean isModule = GradleUtils.isModuleProject(p.getPath());
            boolean isXPackModule = isModule && p.getPath().startsWith(":x-pack");
            if (isModule == false || isXPackModule) {
                addNoticeGeneration(p, extension);
            }

            @SuppressWarnings("unchecked")
            NamedDomainObjectContainer<ElasticsearchCluster> testClusters = (NamedDomainObjectContainer<ElasticsearchCluster>) project
                .getExtensions()
                .getByName(TestClustersPlugin.EXTENSION_NAME);
            p.getExtensions().getByType(PluginPropertiesExtension.class).getExtendedPlugins().forEach(pluginName -> {
                // Auto add any dependent modules
                findModulePath(project, pluginName).ifPresent(
                    path -> testClusters.configureEach(elasticsearchCluster -> elasticsearchCluster.module(path))
                );
            });
        });
    }

    Optional<String> findModulePath(Project project, String pluginName) {
        return project.getRootProject()
            .getAllprojects()
            .stream()
            .filter(p -> GradleUtils.isModuleProject(p.getPath()))
            .filter(p -> p.getPlugins().hasPlugin(PluginBuildPlugin.class))
            .filter(p -> p.getExtensions().getByType(PluginPropertiesExtension.class).getName().equals(pluginName))
            .findFirst()
            .map(Project::getPath);
    }

    /**
     * Configure the pom for the main jar of this plugin
     */
    protected static void addNoticeGeneration(final Project project, PluginPropertiesExtension extension) {
        final var licenseFile = extension.getLicenseFile();
        var tasks = project.getTasks();
        if (licenseFile != null) {
            tasks.withType(Zip.class).named("bundlePlugin").configure(zip -> zip.from(licenseFile.getParentFile(), copySpec -> {
                copySpec.include(licenseFile.getName());
                copySpec.rename(s -> "LICENSE.txt");
            }));
        }

        final var noticeFile = extension.getNoticeFile();
        if (noticeFile != null) {
            final var generateNotice = tasks.register("generateNotice", NoticeTask.class, noticeTask -> {
                noticeTask.setInputFile(noticeFile);
                noticeTask.source(Util.getJavaMainSourceSet(project).get().getAllJava());
            });
            tasks.withType(Zip.class).named("bundlePlugin").configure(task -> task.from(generateNotice));
        }
    }
}
