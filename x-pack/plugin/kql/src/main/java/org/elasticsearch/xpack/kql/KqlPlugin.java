/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kql;

import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.xpack.kql.query.KqlQueryBuilder;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.xpack.kql.KqlFeatures.KQL_QUERY_SUPPORTED;

public class KqlPlugin extends Plugin implements SearchPlugin, ExtensiblePlugin {

    @Override
    public Collection<?> createComponents(PluginServices services) {
        return super.createComponents(services);
    }

    @Override
    public List<QuerySpec<?>> getQueries() {
        if (hasKqlQueryFeature()) {
            return List.of(new SearchPlugin.QuerySpec<>(KqlQueryBuilder.NAME, KqlQueryBuilder::new, KqlQueryBuilder::fromXContent));
        }

        return List.of();
    }

    private boolean hasKqlQueryFeature() {
        return new KqlFeatures().getFeatures().contains(KQL_QUERY_SUPPORTED);
    }
}
