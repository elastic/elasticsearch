/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.conventions;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.provider.MapProperty;
import org.gradle.api.provider.Provider;
import org.gradle.api.provider.ProviderFactory;

import javax.inject.Inject;
import java.util.Map;

public class LicensingPlugin implements Plugin<Project> {
    static final String ELASTIC_LICENSE_URL_PREFIX = "https://raw.githubusercontent.com/elastic/elasticsearch/";
    static final String ELASTIC_LICENSE_URL_POSTFIX = "/licenses/ELASTIC-LICENSE-2.0.txt";

    private ProviderFactory providerFactory;

    @Inject
    public LicensingPlugin(ProviderFactory providerFactory) {
        this.providerFactory = providerFactory;
    }

    @Override
    public void apply(Project project) {
        Provider<String> revision = project.getRootProject().getPlugins().apply(GitInfoPlugin.class).getRevision();
        Provider<String> licenseCommitProvider = providerFactory.provider(() ->
             isSnapshotVersion(project) ? revision.get() : "v" + project.getVersion()
        );

        Provider<String> projectLicenseURL = licenseCommitProvider.map(licenseCommit -> ELASTIC_LICENSE_URL_PREFIX +
                licenseCommit + ELASTIC_LICENSE_URL_POSTFIX);
        // But stick the Elastic license url in project.ext so we can get it if we need to switch to it
        project.getExtensions().getExtraProperties().set("elasticLicenseUrl", projectLicenseURL);

        MapProperty<String, String> licensesProperty = project.getObjects().mapProperty(String.class, String.class).convention(
                providerFactory.provider(() -> Map.of(
                        "Server Side Public License, v 1", "https://www.mongodb.com/licensing/server-side-public-license",
                        "Elastic License 2.0", projectLicenseURL.get())
                )
        );

        // Default to the SSPL+Elastic dual license
        project.getExtensions().getExtraProperties().set("projectLicenses", licensesProperty);
    }

    private boolean isSnapshotVersion(Project project) {
        return project.getVersion().toString().endsWith("-SNAPSHOT");
    }

}