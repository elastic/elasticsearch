/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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
    static final String AGPL_ELASTIC_LICENSE_URL_POSTFIX = "/licenses/AGPL-3.0+SSPL-1.0+ELASTIC-LICENSE-2.0.txt";

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

        Provider<String> elasticLicenseURL = licenseCommitProvider.map(licenseCommit -> ELASTIC_LICENSE_URL_PREFIX +
                licenseCommit + ELASTIC_LICENSE_URL_POSTFIX);
        Provider<String> agplLicenseURL = licenseCommitProvider.map(licenseCommit -> ELASTIC_LICENSE_URL_PREFIX +
            licenseCommit + AGPL_ELASTIC_LICENSE_URL_POSTFIX);
        // But stick the Elastic license url in project.ext so we can get it if we need to switch to it
        project.getExtensions().getExtraProperties().set("elasticLicenseUrl", elasticLicenseURL);

        MapProperty<String, String> licensesProperty = project.getObjects().mapProperty(String.class, String.class).convention(
                providerFactory.provider(() -> Map.of(
                        "Server Side Public License, v 1", "https://www.mongodb.com/licensing/server-side-public-license",
                        "Elastic License 2.0", elasticLicenseURL.get(),
                        "GNU Affero General Public License Version 3", agplLicenseURL.get())
                )
        );

        // Default to the SSPL+Elastic dual license
        project.getExtensions().getExtraProperties().set("projectLicenses", licensesProperty);
    }

    private boolean isSnapshotVersion(Project project) {
        return project.getVersion().toString().endsWith("-SNAPSHOT");
    }

}
