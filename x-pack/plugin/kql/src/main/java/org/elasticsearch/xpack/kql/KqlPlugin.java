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

import java.util.List;

public class KqlPlugin extends Plugin implements SearchPlugin, ExtensiblePlugin {
    @Override
    public List<QuerySpec<?>> getQueries() {
        return List.of(new SearchPlugin.QuerySpec<>(KqlQueryBuilder.NAME, KqlQueryBuilder::new, KqlQueryBuilder::fromXContent));
    }
}
