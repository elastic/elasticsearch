/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchbusinessrules;

import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.plugins.SearchPlugin.QuerySpec;
import org.elasticsearch.plugins.SearchPlugin.RetrieverSpec;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.searchbusinessrules.retriever.PinnedRetrieverBuilder;

import java.util.List;

import static java.util.Collections.singletonList;

public class SearchBusinessRules extends Plugin implements SearchPlugin, ExtensiblePlugin {

    @Override
    public List<QuerySpec<?>> getQueries() {
        return List.of(new QuerySpec<>(PinnedQueryBuilder.NAME, PinnedQueryBuilder::new, PinnedQueryBuilder::fromXContent));
    }

    @Override
    public List<RetrieverSpec<?>> getRetrievers() {
        return singletonList(new RetrieverSpec<>(new ParseField(PinnedRetrieverBuilder.NAME), PinnedRetrieverBuilder::fromXContent));
    }

}
