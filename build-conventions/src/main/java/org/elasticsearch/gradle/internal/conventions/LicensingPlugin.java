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

import java.util.Map;

public class LicensingPlugin implements Plugin<Project> {
    final static String ELASTIC_LICENSE_URL =
            "https://raw.githubusercontent.com/elastic/elasticsearch/${licenseCommit}/licenses/ELASTIC-LICENSE-2.0.txt";

    @Override
    public void apply(Project project) {
        // Default to the SSPL+Elastic dual license
        project.getExtensions().getExtraProperties().set("projectLicenses", Map.of(
                "Server Side Public License, v 1", "https://www.mongodb.com/licensing/server-side-public-license",
                "Elastic License 2.0", ELASTIC_LICENSE_URL));

        // But stick the Elastic license url in project.ext so we can get it if we need to switch to it
        project.getExtensions().getExtraProperties().set("elasticLicenseUrl", ELASTIC_LICENSE_URL);
    }
}