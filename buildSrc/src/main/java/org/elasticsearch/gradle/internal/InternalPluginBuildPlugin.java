/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal;

import groovy.lang.Closure;
import org.elasticsearch.gradle.internal.precommit.TestingConventionsTasks;
import org.elasticsearch.gradle.internal.test.RestTestBasePlugin;
import org.elasticsearch.gradle.internal.conventions.util.Util;
import org.elasticsearch.gradle.plugin.PluginBuildPlugin;
import org.elasticsearch.gradle.plugin.PluginPropertiesExtension;
import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.Project;
import org.gradle.api.tasks.bundling.Zip;

import java.util.Optional;

public class InternalPluginBuildPlugin implements InternalPlugin {
    @Override
    public void apply(Project project) {
        project.getPluginManager().apply(BuildPlugin.class);
        project.getPluginManager().apply(PluginBuildPlugin.class);
        // Clear default dependencies added by public PluginBuildPlugin as we add our
        // own project dependencies for internal builds
        // TODO remove once we removed default dependencies from PluginBuildPlugin
        project.getConfigurations().getByName("compileOnly").getDependencies().clear();
        project.getConfigurations().getByName("testImplementation").getDependencies().clear();

        project.getPluginManager().apply(RestTestBasePlugin.class);
        var extension = project.getExtensions().getByType(PluginPropertiesExtension.class);

        // We've ported this from multiple build scripts where we see this pattern into
        // an extension method as a first step of consolidation.
        // We might want to port this into a general pattern later on.
        project.getExtensions()
            .getExtraProperties()
            .set("addQaCheckDependencies", new Closure<Object>(InternalPluginBuildPlugin.this, InternalPluginBuildPlugin.this) {
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

        project.getTasks().withType(TestingConventionsTasks.class).named("testingConventions").configure(t -> {
            t.getNaming().clear();
            t.getNaming()
                .create("Tests", testingConventionRule -> testingConventionRule.baseClass("org.apache.lucene.util.LuceneTestCase"));
            t.getNaming().create("IT", testingConventionRule -> {
                testingConventionRule.baseClass("org.elasticsearch.test.ESIntegTestCase");
                testingConventionRule.baseClass("org.elasticsearch.test.rest.ESRestTestCase");
                testingConventionRule.baseClass("org.elasticsearch.test.ESSingleNodeTestCase");
            });
        });

        project.afterEvaluate(p -> {
            boolean isModule = GradleUtils.isModuleProject(p.getPath());
            boolean isXPackModule = isModule && p.getPath().startsWith(":x-pack");
            if (isModule == false || isXPackModule) {
                addNoticeGeneration(p, extension);
            }
        });
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
