/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.example.customsigheuristic;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.bucket.terms.heuristic.SignificanceHeuristic;

import java.util.List;

import static java.util.Collections.singletonList;

/**
 * Plugin declaring a custom {@link SignificanceHeuristic}.
 */
public class CustomSignificanceHeuristicPlugin extends Plugin implements SearchPlugin {
    @Override
    public List<SignificanceHeuristicSpec<?>> getSignificanceHeuristics() {
        return singletonList(new SignificanceHeuristicSpec<>(SimpleHeuristic.NAME, SimpleHeuristic::new, SimpleHeuristic.PARSER));
    }
}
