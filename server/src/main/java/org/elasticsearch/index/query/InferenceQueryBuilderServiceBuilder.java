/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.plugins.SearchPlugin;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;

public class InferenceQueryBuilderServiceBuilder {
    private PluginsService pluginsService;

    public InferenceQueryBuilderServiceBuilder pluginsService(PluginsService pluginsService) {
        this.pluginsService = pluginsService;
        return this;
    }

    public InferenceQueryBuilderService build() {
        Objects.requireNonNull(pluginsService);

        List<BiFunction<String, String, AbstractQueryBuilder<?>>> definedInferenceQueryBuilders = new ArrayList<>();

        List<SearchPlugin> searchPlugins = pluginsService.filterPlugins(SearchPlugin.class).toList();
        for (SearchPlugin searchPlugin : searchPlugins) {
            if (searchPlugin.getDefaultInferenceQueryBuilder() != null) {
                definedInferenceQueryBuilders.add(searchPlugin.getDefaultInferenceQueryBuilder());
            }
        }

        if (definedInferenceQueryBuilders.size() != 1) {
            throw new IllegalStateException(
                "Expected exactly one default inference query builder, but found " + definedInferenceQueryBuilders.size()
            );
        }

        return new InferenceQueryBuilderService(definedInferenceQueryBuilders.getFirst());
    }
}
