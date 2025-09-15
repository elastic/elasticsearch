/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal;

import org.elasticsearch.gradle.internal.conventions.LicensingPlugin;
import org.elasticsearch.gradle.internal.info.GlobalBuildInfoPlugin;
import org.elasticsearch.gradle.internal.precommit.InternalPrecommitTasks;
import org.elasticsearch.gradle.internal.snyk.SnykDependencyMonitoringGradlePlugin;
import org.elasticsearch.gradle.internal.test.HistoricalFeaturesMetadataPlugin;
import org.gradle.api.InvalidUserDataException;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.file.ProjectLayout;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.plugins.ExtraPropertiesExtension;
import org.gradle.api.provider.ProviderFactory;
import org.gradle.api.tasks.bundling.Jar;
import org.gradle.initialization.layout.BuildLayout;

import java.io.File;

import javax.inject.Inject;

/**
 * Encapsulates build configuration for elasticsearch projects.
 */
public class BuildPlugin implements Plugin<Project> {

    public static final String LICENSE_PATH = "licenses/AGPL-3.0+SSPL-1.0+ELASTIC-LICENSE-2.0.txt";

    private final BuildLayout buildLayout;
    private final ObjectFactory objectFactory;
    private final ProviderFactory providerFactory;
    private final ProjectLayout projectLayout;

    @Inject
    BuildPlugin(BuildLayout buildLayout, ObjectFactory objectFactory, ProviderFactory providerFactory, ProjectLayout projectLayout) {
        this.buildLayout = buildLayout;
        this.objectFactory = objectFactory;
        this.providerFactory = providerFactory;
        this.projectLayout = projectLayout;
    }

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
        project.getPluginManager().apply(ElasticsearchJavadocPlugin.class);
        project.getPluginManager().apply(DependenciesInfoPlugin.class);
        project.getPluginManager().apply(LicensingPlugin.class);
        project.getPluginManager().apply(SnykDependencyMonitoringGradlePlugin.class);
        project.getPluginManager().apply(HistoricalFeaturesMetadataPlugin.class);
        InternalPrecommitTasks.create(project, true);
        configureLicenseAndNotice(project);
    }

    public void configureLicenseAndNotice(final Project project) {
        final ExtraPropertiesExtension ext = project.getExtensions().getByType(ExtraPropertiesExtension.class);
        RegularFileProperty licenseFileProperty = objectFactory.fileProperty();
        RegularFileProperty noticeFileProperty = objectFactory.fileProperty();
        ext.set("licenseFile", licenseFileProperty);
        ext.set("noticeFile", noticeFileProperty);

        configureLicenseDefaultConvention(licenseFileProperty);
        configureNoticeDefaultConvention(noticeFileProperty);

        updateJarTasksMetaInf(project);
    }

    private void updateJarTasksMetaInf(Project project) {
        final ExtraPropertiesExtension ext = project.getExtensions().getByType(ExtraPropertiesExtension.class);
        project.getTasks().withType(Jar.class).configureEach(jar -> {
            final RegularFileProperty licenseFileExtProperty = (RegularFileProperty) ext.get("licenseFile");
            final RegularFileProperty noticeFileExtProperty = (RegularFileProperty) ext.get("noticeFile");
            File licenseFile = licenseFileExtProperty.getAsFile().get();
            File noticeFile = noticeFileExtProperty.getAsFile().get();
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
        });
    }

    private void configureLicenseDefaultConvention(RegularFileProperty licenseFileProperty) {
        File licenseFileDefault = new File(buildLayout.getRootDirectory(), LICENSE_PATH);
        licenseFileProperty.convention(projectLayout.file(providerFactory.provider(() -> licenseFileDefault)));
    }

    private void configureNoticeDefaultConvention(RegularFileProperty noticeFileProperty) {
        File noticeFileDefault = new File(buildLayout.getRootDirectory(), "NOTICE.txt");
        noticeFileProperty.convention(projectLayout.file(providerFactory.provider(() -> noticeFileDefault)));
    }
}
