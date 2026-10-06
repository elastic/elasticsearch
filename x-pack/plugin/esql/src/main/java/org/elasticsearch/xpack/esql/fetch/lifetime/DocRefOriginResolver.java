/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch.lifetime;

import org.elasticsearch.compute.data.DocRefOrigin;
import org.elasticsearch.search.internal.SearchContext;

/**
 * Names the reader of a shard for the rows that leave a data node as document references.
 */
@FunctionalInterface
public interface DocRefOriginResolver {
    /**
     * The origin of the rows read through {@code searchContext}. Asking means some of those rows survived the node cut.
     */
    DocRefOrigin originOf(SearchContext searchContext);

    /**
     * For data node requests that open no fetch contexts. Nothing could load their rows by reference later, so a planner
     * that makes references there has a bug.
     */
    DocRefOriginResolver NONE = searchContext -> {
        throw new IllegalStateException("document references need fetch contexts, but this request opened none");
    };
}
