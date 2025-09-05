/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.validation;

import org.elasticsearch.action.support.MappedActionFilter;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class DotPrefixValidationPlugin extends Plugin implements ActionPlugin {
    private final AtomicReference<List<MappedActionFilter>> actionFilters = new AtomicReference<>();

    public DotPrefixValidationPlugin() {}

    @Override
    public Collection<?> createComponents(PluginServices services) {
        ThreadContext context = services.threadPool().getThreadContext();
        ClusterService clusterService = services.clusterService();

        actionFilters.set(
            List.of(
                new CreateIndexDotValidator(context, clusterService),
                new AutoCreateDotValidator(context, clusterService),
                new IndexTemplateDotValidator(context, clusterService)
            )
        );

        return Set.of();
    }

    @Override
    public Collection<MappedActionFilter> getMappedActionFilters() {
        return actionFilters.get();
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(DotPrefixValidator.VALIDATE_DOT_PREFIXES, DotPrefixValidator.IGNORED_INDEX_PATTERNS_SETTING);
    }
}
