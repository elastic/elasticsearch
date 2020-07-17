/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.query;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;

import java.util.List;

import static java.util.Collections.singletonList;

public class DeprecatedQueryPlugin extends Plugin implements SearchPlugin {

    public DeprecatedQueryPlugin() {}

    @Override
    public List<QuerySpec<?>> getQueries() {
        return singletonList(new QuerySpec<>("deprecated", DeprecatedQueryBuilder::new, p -> DeprecatedQueryBuilder.fromXContent(p)));
    }
}
