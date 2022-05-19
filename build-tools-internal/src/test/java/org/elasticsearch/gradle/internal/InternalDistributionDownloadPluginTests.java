/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal;

import org.elasticsearch.gradle.AbstractDistributionDownloadPluginTests;
import org.elasticsearch.gradle.ElasticsearchDistributionType;
import org.elasticsearch.gradle.VersionProperties;
import org.elasticsearch.gradle.internal.distribution.InternalElasticsearchDistributionTypes;
import org.gradle.api.Project;
import org.gradle.testfixtures.ProjectBuilder;

import java.io.File;

public class InternalDistributionDownloadPluginTests extends AbstractDistributionDownloadPluginTests {

    public void testLocalCurrentVersionPackages() {
        ElasticsearchDistributionType[] types = { InternalElasticsearchDistributionTypes.RPM, InternalElasticsearchDistributionTypes.DEB };
        for (ElasticsearchDistributionType packageType : types) {
            for (boolean bundledJdk : new boolean[] { true, false }) {
                Project project = createProject(BWC_MINOR);
                String projectName = projectName(packageType.toString(), bundledJdk);
                Project packageProject = ProjectBuilder.builder().withParent(packagesProject).withName(projectName).build();
                packageProject.getConfigurations().create("default");
                packageProject.getArtifacts().add("default", new File("doesnotmatter"));
                createDistro(project, "distro", VersionProperties.getElasticsearch(), packageType, null, bundledJdk);
            }
        }
    }

    public void testLocalBwcPackages() {
        ElasticsearchDistributionType[] types = { InternalElasticsearchDistributionTypes.RPM, InternalElasticsearchDistributionTypes.DEB };
        for (ElasticsearchDistributionType packageType : types) {
            // note: no non bundled jdk for bwc
            String configName = projectName(packageType.toString(), true);
            checkBwc("minor", configName, BWC_MINOR_VERSION.elasticsearch(), packageType, null, BWC_MINOR);
            checkBwc("staged", configName, BWC_STAGED_VERSION.elasticsearch(), packageType, null, BWC_STAGED);
            checkBwc("bugfix", configName, BWC_BUGFIX_VERSION.elasticsearch(), packageType, null, BWC_BUGFIX);
            checkBwc("maintenance", configName, BWC_MAINTENANCE_VERSION.elasticsearch(), packageType, null, BWC_MAINTENANCE);
        }
    }
}
