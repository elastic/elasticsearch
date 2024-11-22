/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.multiproject;

import org.elasticsearch.cluster.project.DefaultProjectResolver;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.project.ProjectResolverFactory;

public class MultiProjectResolverFactory implements ProjectResolverFactory {

    private final MultiProjectPlugin plugin;

    public MultiProjectResolverFactory() {
        throw new IllegalStateException("Provider must be constructed using PluginsService");
    }

    public MultiProjectResolverFactory(MultiProjectPlugin plugin) {
        this.plugin = plugin;
    }

    @Override
    public ProjectResolver create() {
        if (plugin.multiProjectEnabled()) {
            return new MultiProjectResolver(plugin::getThreadPool);
        } else {
            return DefaultProjectResolver.INSTANCE;
        }
    }
}
