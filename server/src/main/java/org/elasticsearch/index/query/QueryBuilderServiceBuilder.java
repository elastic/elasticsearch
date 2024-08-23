/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.plugins.SearchPlugin;

import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;

public class QueryBuilderServiceBuilder {
    private PluginsService pluginsService;

    public QueryBuilderServiceBuilder pluginsService(PluginsService pluginsService) {
        this.pluginsService = pluginsService;
        return this;
    }

    public QueryBuilderService build() {
        Objects.requireNonNull(pluginsService);

        ImmutableOpenMap.Builder<String, BiFunction<String, String, AbstractQueryBuilder<?>>> functionMapBuilder = ImmutableOpenMap
            .builder();
        List<SearchPlugin> searchPlugins = pluginsService.filterPlugins(SearchPlugin.class).toList();
        for (SearchPlugin searchPlugin : searchPlugins) {
            // TODO: Detect query name conflicts before adding to map
            functionMapBuilder.putAllFromMap(searchPlugin.getQueryBuilders());
        }

        return new QueryBuilderService(functionMapBuilder.build());
    }
}
