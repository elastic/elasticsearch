/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal;

import groovy.lang.Closure;

import org.elasticsearch.gradle.internal.conventions.util.Util;
import org.elasticsearch.gradle.internal.info.BuildParameterExtension;
import org.elasticsearch.gradle.internal.precommit.JarHellPrecommitPlugin;
import org.elasticsearch.gradle.internal.test.ClusterFeaturesMetadataPlugin;
import org.elasticsearch.gradle.internal.transport.TransportVersionManagementPlugin;
import org.elasticsearch.gradle.plugin.PluginBuildPlugin;
import org.elasticsearch.gradle.plugin.PluginPropertiesExtension;
import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.Plugin;
import org.gradle.api.Project;

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
        project.getPluginManager().apply(JarHellPrecommitPlugin.class);
        project.getPluginManager().apply(ElasticsearchJavaPlugin.class);
        project.getPluginManager().apply(ClusterFeaturesMetadataPlugin.class);
        project.getPluginManager().apply(TransportVersionManagementPlugin.class);
        boolean isCi = project.getRootProject().getExtensions().getByType(BuildParameterExtension.class).getCi();
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
            .set("addQaCheckDependencies", new Closure<Project>(BaseInternalPluginBuildPlugin.this, BaseInternalPluginBuildPlugin.this) {
                public void doCall(Project proj) {
                    // This is only a convenience for local developers so make this a noop when running in CI
                    if (isCi == false) {
                        proj.afterEvaluate(project1 -> {
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
                }

                public void doCall() {
                    doCall();
                }
            });

        boolean isModule = GradleUtils.isModuleProject(project.getPath());
        boolean isXPackModule = isModule && project.getPath().startsWith(":x-pack");
        if (isModule == false || isXPackModule) {
            addNoticeGeneration(project, extension);
        }
    }

    /**
     * Configure the pom for the main jar of this plugin
     */
    protected static void addNoticeGeneration(final Project project, PluginPropertiesExtension extension) {
        final var licenseFile = extension.getLicenseFile();
        var tasks = project.getTasks();
        var bundleSpec = extension.getBundleSpec();
        if (licenseFile != null) {
            bundleSpec.from(licenseFile.getParentFile(), s -> {
                s.include(licenseFile.getName());
                s.rename(f -> "LICENSE.txt");
            });
        }

        final var noticeFile = extension.getNoticeFile();
        if (noticeFile != null) {
            final var generateNotice = tasks.register("generateNotice", NoticeTask.class, noticeTask -> {
                noticeTask.setInputFile(noticeFile);
                noticeTask.source(Util.getJavaMainSourceSet(project).get().getAllJava());
            });
            bundleSpec.from(generateNotice);
        }
    }
}
