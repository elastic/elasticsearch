/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal;

import org.elasticsearch.gradle.VersionProperties;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.file.ProjectLayout;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.plugins.ExtraPropertiesExtension;
import org.gradle.api.provider.ProviderFactory;
import org.gradle.api.tasks.bundling.Jar;
import org.gradle.initialization.layout.BuildLayout;

import javax.inject.Inject;
import java.io.File;

/**
 * basic configuration applied to every elasticsearch subproject
 * */
public class ElasticsearchBasePlugin implements Plugin<Project> {

    public static final String SSPL_LICENSE_PATH = "licenses/SSPL-1.0+ELASTIC-LICENSE-2.0.txt";
    private BuildLayout buildLayout;
    private ObjectFactory objectFactory;
    private ProviderFactory providerFactory;
    private ProjectLayout projectLayout;

    @Inject
    ElasticsearchBasePlugin(BuildLayout buildLayout, ObjectFactory objectFactory, ProviderFactory providerFactory, ProjectLayout projectLayout){
        this.buildLayout = buildLayout;
        this.objectFactory = objectFactory;
        this.providerFactory = providerFactory;
        this.projectLayout = projectLayout;
    }

    @Override
    public void apply(Project project) {
        project.setDescription("Elasticsearch subproject " + project.getPath());
        configureProjectCoordinates(project);
        configureLicenseAndNotice(project);
    }

    private void configureProjectCoordinates(Project project) {
        // common maven publishing configuration
        project.setGroup("org.elasticsearch");
        project.setVersion(VersionProperties.getElasticsearch());
    }

    public void configureLicenseAndNotice(final Project project) {
        final ExtraPropertiesExtension ext = project.getExtensions().getByType(ExtraPropertiesExtension.class);
        File licenseFileDefault = new File(buildLayout.getRootDirectory(), SSPL_LICENSE_PATH);
        File noticeFileDefault = new File(buildLayout.getRootDirectory(), "NOTICE.txt");
        RegularFileProperty licenseFileProperty = objectFactory.fileProperty()
                .convention(projectLayout.file(providerFactory.provider(() -> licenseFileDefault)));
        RegularFileProperty noticeFileProperty = objectFactory.fileProperty()
                .convention(projectLayout.file(providerFactory.provider(() -> noticeFileDefault)));
        ext.set("licenseFile", licenseFileProperty);
        ext.set("noticeFile", noticeFileProperty);

        // add license/notice files to archives
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
}
