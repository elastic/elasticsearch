/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.distribution;

import org.gradle.api.Plugin;
import org.gradle.api.Project;

/**
 * Provides an extension named 'distro' to provide common operations used when setting up and bundling
 * elasticsearch distributions. Having those here instead of in the build scripts makes testing and maintaining
 * this common functionality easier
 * */
public class ElasticsearchDistributionPlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {
        project.getExtensions().create("distro", ElasticsearchDistributionExtension.class, project);
    }
}
