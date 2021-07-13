/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal;

import org.codehaus.groovy.runtime.DefaultGroovyMethods;
import org.elasticsearch.gradle.internal.info.GlobalBuildInfoPlugin;
import org.elasticsearch.gradle.internal.precommit.InternalPrecommitTasks;
import org.elasticsearch.gradle.internal.precommit.JarHellPrecommitPlugin;
import org.elasticsearch.gradle.internal.precommit.SplitPackagesAuditPrecommitPlugin;
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

        InternalPrecommitTasks.create(project, true);
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
