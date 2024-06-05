/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.provider.ProviderFactory;

import javax.inject.Inject;

public class InternalTestArtifactBasePlugin implements Plugin<Project> {
    private final ProviderFactory providerFactory;

    @Inject
    public InternalTestArtifactBasePlugin(ProviderFactory providerFactory) {
        this.providerFactory = providerFactory;
    }

    @Override
    public void apply(Project project) {
        project.getPlugins().apply(ElasticsearchJavaBasePlugin.class);
        project.getExtensions().create("testArtifacts", InternalTestArtifactExtension.class, project, providerFactory);
    }
}
