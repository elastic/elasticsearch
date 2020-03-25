/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.searchbusinessrules;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;

import java.util.List;

import static java.util.Collections.singletonList;

public class SearchBusinessRules extends Plugin implements SearchPlugin {

    @Override
    public List<QuerySpec<?>> getQueries() {
        return singletonList(new QuerySpec<>(PinnedQueryBuilder.NAME, PinnedQueryBuilder::new, PinnedQueryBuilder::fromXContent));
    }

}
