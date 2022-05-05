/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal;

import org.gradle.api.Action;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Dependency;
import org.gradle.api.artifacts.ProjectDependency;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.javadoc.Javadoc;
import org.gradle.internal.BiAction;

// Handle javadoc dependencies across projects. Order matters: the linksOffline for
// org.elasticsearch:elasticsearch must be the last one or all the links for the
// other packages (e.g org.elasticsearch.client) will point to server rather than
// their own artifacts.
public class ElasticsearchJavadoc implements Plugin<Project> {
    @Override
    public void apply(Project project) {


        new BiAction<Boolean, Dependency>() {

            @Override
            public void execute(Boolean shadowed, Dependency dep) {
                if ((dep instanceof ProjectDependency) == false) {
                    return;
                }
                var projectDep = (ProjectDependency) dep;
                Project upstreamProject = projectDep.getDependencyProject();
                if (upstreamProject == null) {
                    return;
                }
                if (shadowed) {
                    /*
                     * Include the source of shadowed upstream projects so we don't
                     * have to publish their javadoc.
                     */
                    project.evaluationDependsOn(upstreamProject.getPath());
                    TaskProvider<Javadoc> javadoc = project.getTasks().named("javadoc", Javadoc.class);
                    javadoc.configure(new Action<Javadoc>() {
                        @Override
                        public void execute(Javadoc javadoc) {

                        }
                    });

                    project.javadoc.source += upstreamProject.javadoc.source
                    /*
                     * Instead we need the upstream project's javadoc classpath so
                     * we don't barf on the classes that it references.
                     */
                    project.javadoc.classpath += upstreamProject.javadoc.classpath
                } else {
                    // Link to non-shadowed dependant projects
                    project.javadoc.dependsOn "${upstreamProject.path}:javadoc"
                    String externalLinkName = upstreamProject.archivesBaseName
                    String artifactPath = dep.group.replaceAll('\\.', '/') + '/' + externalLinkName.replaceAll('\\.', '/') + '/' + dep.version
                    project.javadoc.options.linksOffline artifactsHost + "/javadoc/" + artifactPath, "${upstreamProject.buildDir}/docs/javadoc/"
                }
            }
        };
        // TODO revert the dependency and just apply the javadoc plugin in the build plugin later on
        project.getPlugins().withType(BuildPlugin.class, new Action<BuildPlugin>() {
            @Override
            public void execute(BuildPlugin buildPlugin) {

            }
        });
    }
}
