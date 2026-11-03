/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.compute.data.DocOrigin;
import org.elasticsearch.search.internal.SearchContext;

/**
 * Names the reader of a shard for the documents of rows that leave a data node.
 */
@FunctionalInterface
public interface DocOriginResolver {
    /**
     * The origin of the rows read through {@code searchContext}. Asking means some of those rows survived the node cut.
     */
    DocOrigin originOf(SearchContext searchContext);

    /**
     * For data node requests that open no fetch contexts. Nothing could load the documents of their rows later, so a
     * planner that sends documents off such a node has a bug.
     */
    DocOriginResolver NONE = searchContext -> {
        throw new IllegalStateException("documents can only leave their node for a fetch, but this request opened no fetch contexts");
    };
}
