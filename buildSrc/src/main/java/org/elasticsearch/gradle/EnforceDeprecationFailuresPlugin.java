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

package org.elasticsearch.gradle;

import org.gradle.api.GradleException;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.plugins.JavaBasePlugin;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;

/**
 * This plugin enforces build failure if certain deprecated apis are used.
 * <p>
 * For now we support build failure on:
 * - declaring dependencies for testCompile
 * - resolving testCompile configuration
 */
public class EnforceDeprecationFailuresPlugin implements Plugin<Project> {

    private Project project;

    @Override
    public void apply(Project project) {
        this.project = project;
        handleDeprecatedConfigurations();
    }

    private void handleDeprecatedConfigurations() {
        project.getPlugins().withType(JavaBasePlugin.class, javaBasePlugin -> {
            SourceSetContainer sourceSetContainer = project.getExtensions().getByType(SourceSetContainer.class);
            sourceSetContainer.all(
                sourceSet -> {
                    // TODO: remove that guard once we removed general compile usage from es build
                    if (sourceSet.getName().equals("test")) {
                        failOnCompileConfigurationResolution(sourceSet);
                        failOnCompileConfigurationDependencyDeclaration(sourceSet);
                    }
                }
            );
        });
    }

    private void failOnCompileConfigurationDependencyDeclaration(SourceSet sourceSet) {
        // fail on using deprecated testCompile
        project.getConfigurations().getByName(sourceSet.getCompileConfigurationName()).withDependencies(dependencies -> {
            if (dependencies.size() > 0) {
                throw new GradleException(
                    "Declaring dependencies for configuration "
                        + sourceSet.getCompileConfigurationName()
                        + " is no longer supported. Use "
                        + sourceSet.getImplementationConfigurationName()
                        + " instead."
                );
            }
        });
    }

    private void failOnCompileConfigurationResolution(SourceSet sourceSet) {
        project.getConfigurations()
            .getByName(sourceSet.getCompileConfigurationName())
            .getIncoming()
            .beforeResolve(resolvableDependencies -> {
                if (resolvableDependencies.getDependencies().size() > 0) {
                    throw new GradleException(
                        "Resolving configuration "
                            + sourceSet.getCompileConfigurationName()
                            + " is no longer supported. Use "
                            + sourceSet.getImplementationConfigurationName()
                            + " instead."
                    );
                }
            });
    }

}
