/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;

import java.util.List;
import java.util.stream.Stream;

public class FailingQueryPlugin extends Plugin implements SearchPlugin {
    public FailingQueryPlugin() {}

    @Override
    public List<QuerySpec<?>> getQueries() {
        return List.of(
            new QuerySpec<QueryBuilder>(
                ShardFailingQueryBuilder.NAME,
                ShardFailingQueryBuilder::new,
                ShardFailingQueryBuilder::fromXContent
            )
        );
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return Stream.of(
            new NamedWriteableRegistry.Entry(ShardFailingQueryBuilder.class, ShardFailingQueryBuilder.NAME, ShardFailingQueryBuilder::new)
        ).toList();
    }
}
