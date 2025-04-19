/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal;

import com.github.jengelman.gradle.plugins.shadow.ShadowPlugin;

import org.gradle.api.Action;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.ProjectDependency;
import org.gradle.api.plugins.BasePluginExtension;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.tasks.javadoc.Javadoc;
import org.gradle.external.javadoc.JavadocOfflineLink;
import org.gradle.external.javadoc.StandardJavadocDocletOptions;

import java.io.File;
import java.util.List;

// Handle javadoc dependencies across projects. Order matters: the linksOffline for
// org.elasticsearch:elasticsearch must be the last one or all the links for the
// other packages (e.g org.elasticsearch.client) will point to server rather than
// their own artifacts.
public class ElasticsearchJavadocPlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {
        // ignore missing javadocs
        project.getTasks().withType(Javadoc.class).configureEach(javadoc -> {
            // the -quiet here is because of a bug in gradle, in that adding a string option
            // by itself is not added to the options. By adding quiet, both this option and
            // the "value" -quiet is added, separated by a space. This is ok since the javadoc
            // command already adds -quiet, so we are just duplicating it
            // see https://discuss.gradle.org/t/add-custom-javadoc-option-that-does-not-take-an-argument/5959
            javadoc.getOptions().setEncoding("UTF8");
            ((StandardJavadocDocletOptions) javadoc.getOptions()).addStringOption("Xdoclint:all,-missing", "-quiet");

            // ensure that modular dependencies can be found on the module path
            javadoc.doFirst(new Action<Task>() {
                @Override
                public void execute(Task task) {
                    javadoc.getOptions().modulePath(javadoc.getClasspath().getFiles().stream().toList());
                }
            });
        });

        // Relying on configurations introduced by the java plugin
        project.getPlugins().withType(JavaPlugin.class, javaPlugin -> project.afterEvaluate(project1 -> {
            var withShadowPlugin = project1.getPlugins().hasPlugin(ShadowPlugin.class);
            var compileClasspath = project.getConfigurations().getByName("compileClasspath");

            var copiedCompileClasspath = project.getConfigurations().create("copiedCompileClasspath");
            copiedCompileClasspath.extendsFrom(compileClasspath);
            if (withShadowPlugin) {
                var shadowConfiguration = project.getConfigurations().getByName("shadow");
                var shadowedDependencies = shadowConfiguration.getAllDependencies();
                var nonShadowedCompileClasspath = copiedCompileClasspath.copyRecursive(
                    dependency -> shadowedDependencies.contains(dependency) == false
                );
                configureJavadocForConfiguration(project, false, nonShadowedCompileClasspath);
                configureJavadocForConfiguration(project, true, shadowConfiguration);
            } else {
                configureJavadocForConfiguration(project, false, compileClasspath);
            }
        }));
    }

    private void configureJavadocForConfiguration(Project project, boolean shadow, Configuration configuration) {
        configuration.getAllDependencies().configureEach(dependency -> {
            if (dependency instanceof ProjectDependency) {
                configureDependency(project, shadow, (ProjectDependency) dependency);
            }
        });
    }

    private void configureDependency(Project project, boolean shadowed, ProjectDependency dep) {
        // we should use variant aware dependency management to resolve artifacts required for javadoc here
        Project upstreamProject = project.project(dep.getPath());
        if (upstreamProject == null) {
            return;
        }
        if (shadowed) {
            /*
             * Include the source of shadowed upstream projects so we don't
             * have to publish their javadoc.
             */
            project.evaluationDependsOn(upstreamProject.getPath());
            project.getTasks().named("javadoc", Javadoc.class).configure(javadoc -> {
                Javadoc upstreamJavadoc = upstreamProject.getTasks().named("javadoc", Javadoc.class).get();
                javadoc.setSource(javadoc.getSource().plus(upstreamJavadoc.getSource()));
                javadoc.setClasspath(javadoc.getClasspath().plus(upstreamJavadoc.getClasspath()));
            });
            /*
             * Instead we need the upstream project's javadoc classpath so
             * we don't barf on the classes that it references.
             */
        } else {
            project.getTasks().named("javadoc", Javadoc.class).configure(javadoc -> {
                // Link to non-shadowed dependant projects
                javadoc.dependsOn(upstreamProject.getPath() + ":javadoc");
                String externalLinkName = upstreamProject.getExtensions().getByType(BasePluginExtension.class).getArchivesName().get();
                String artifactPath = dep.getGroup().replace('.', '/') + '/' + externalLinkName.replace('.', '/') + '/' + dep.getVersion();
                var options = (StandardJavadocDocletOptions) javadoc.getOptions();
                options.linksOffline(
                    artifactHost(project) + "/javadoc/" + artifactPath,
                    upstreamProject.getBuildDir().getPath() + "/docs/javadoc/"
                );
                /*
                 *some dependent javadoc tasks are explicitly skipped. We need to ignore those external links as
                 * javadoc would fail otherwise.
                 * Using Action here instead of lambda to keep gradle happy and don't trigger deprecation
                 */
                javadoc.doFirst(new Action<Task>() {
                    @Override
                    public void execute(Task task) {
                        List<JavadocOfflineLink> existingJavadocOfflineLinks = ((StandardJavadocDocletOptions) javadoc.getOptions())
                            .getLinksOffline()
                            .stream()
                            .filter(javadocOfflineLink -> new File(javadocOfflineLink.getPackagelistLoc()).exists())
                            .toList();
                        ((StandardJavadocDocletOptions) javadoc.getOptions()).setLinksOffline(existingJavadocOfflineLinks);

                    }
                });

            });
        }
    }

    private String artifactHost(Project project) {
        return project.getVersion().toString().endsWith("-SNAPSHOT") ? "https://snapshots.elastic.co" : "https://artifacts.elastic.co";
    }
}
