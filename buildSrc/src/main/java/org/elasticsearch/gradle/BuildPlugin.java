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

import org.codehaus.groovy.runtime.DefaultGroovyMethods;
import org.elasticsearch.gradle.info.BuildParams;
import org.elasticsearch.gradle.info.GlobalBuildInfoPlugin;
import org.elasticsearch.gradle.internal.InternalPlugin;
import org.elasticsearch.gradle.internal.precommit.InternalPrecommitTasks;
import org.elasticsearch.gradle.precommit.PrecommitTasks;
import org.gradle.api.GradleException;
import org.gradle.api.InvalidUserDataException;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.plugins.ExtraPropertiesExtension;
import org.gradle.api.tasks.bundling.Jar;

import java.io.File;

/**
 * Encapsulates build configuration for elasticsearch projects.
 */
public class BuildPlugin implements Plugin<Project> {
    @Override
    public void apply(final Project project) {
        // make sure the global build info plugin is applied to the root project
        project.getRootProject().getPluginManager().apply(GlobalBuildInfoPlugin.class);
        checkExternalInternalPluginUsages(project);

        if (project.getPluginManager().hasPlugin("elasticsearch.standalone-rest-test")) {
            throw new InvalidUserDataException(
                "elasticsearch.standalone-test, " + "elasticsearch.standalone-rest-test, and elasticsearch.build are mutually exclusive"
            );
        }

        project.getPluginManager().apply("elasticsearch.java");
        configureLicenseAndNotice(project);
        project.getPluginManager().apply("elasticsearch.publish");
        project.getPluginManager().apply(DependenciesInfoPlugin.class);
        project.getPluginManager().apply(DependenciesGraphPlugin.class);

        BuildParams.withInternalBuild(() -> InternalPrecommitTasks.create(project, true)).orElse(() -> PrecommitTasks.create(project));

    }

    private static void checkExternalInternalPluginUsages(Project project) {
        if (BuildParams.isInternal().equals(false)) {
            project.getPlugins()
                .withType(
                    InternalPlugin.class,
                    internalPlugin -> { throw new GradleException(internalPlugin.getExternalUseErrorMessage()); }
                );
        }
    }

    public static void configureLicenseAndNotice(final Project project) {
        final ExtraPropertiesExtension ext = project.getExtensions().getByType(ExtraPropertiesExtension.class);
        ext.set("licenseFile", null);
        ext.set("noticeFile", null);
        // add license/notice files
        project.afterEvaluate(p -> p.getTasks().withType(Jar.class).configureEach(jar -> {
            if (ext.has("licenseFile") == false
                || ext.get("licenseFile") == null
                || ext.has("noticeFile") == false
                || ext.get("noticeFile") == null) {
                throw new GradleException("Must specify license and notice file for project " + p.getPath());
            }
            final File licenseFile = DefaultGroovyMethods.asType(ext.get("licenseFile"), File.class);
            final File noticeFile = DefaultGroovyMethods.asType(ext.get("noticeFile"), File.class);
            jar.metaInf(spec -> {
                spec.from(licenseFile.getParent(), from -> {
                    from.include(licenseFile.getName());
                    from.rename(s -> "LICENSE.txt");
                });
                spec.from(noticeFile.getParent(), from -> {
                    from.include(noticeFile.getName());
                    from.rename(s -> "NOTICE.txt");
                });
            });
        }));
    }
}
