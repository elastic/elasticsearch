/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.query;

import org.elasticsearch.common.TriFunction;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.plugins.SearchPlugin;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class InferenceQueryBuilderService {
    private final TriFunction<String, String, Boolean, AbstractQueryBuilder<?>> defaultInferenceQueryBuilder;

    public InferenceQueryBuilderService(TriFunction<String, String, Boolean, AbstractQueryBuilder<?>> defaultInferenceQueryBuilder) {
        this.defaultInferenceQueryBuilder = defaultInferenceQueryBuilder;
    }

    public AbstractQueryBuilder<?> getDefaultInferenceQueryBuilder(String fieldName, String query, boolean throwOnUnsupportedQueries) {
        return defaultInferenceQueryBuilder != null
            ? defaultInferenceQueryBuilder.apply(fieldName, query, throwOnUnsupportedQueries)
            : null;
    }

    public static InferenceQueryBuilderService build(PluginsService pluginsService) {
        Objects.requireNonNull(pluginsService);

        List<TriFunction<String, String, Boolean, AbstractQueryBuilder<?>>> definedInferenceQueryBuilders = new ArrayList<>();

        List<SearchPlugin> searchPlugins = pluginsService.filterPlugins(SearchPlugin.class).toList();
        for (SearchPlugin searchPlugin : searchPlugins) {
            if (searchPlugin.getDefaultInferenceQueryBuilder() != null) {
                definedInferenceQueryBuilders.add(searchPlugin.getDefaultInferenceQueryBuilder());
            }
        }

        if (definedInferenceQueryBuilders.isEmpty()) {
            // Backwards compatibility
            return new InferenceQueryBuilderService(null);
        }

        if (definedInferenceQueryBuilders.size() != 1) {
            throw new IllegalStateException(
                "Expected a single default inference query builder, but found " + definedInferenceQueryBuilders.size()
            );
        }

        return new InferenceQueryBuilderService(definedInferenceQueryBuilders.getFirst());
    }
}
