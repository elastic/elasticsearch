/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.query;

import org.apache.lucene.search.Query;

/**
 * {@code SearchQuery} is a wrapper class for containing all
 * the information required to perform a single search query
 * as part of a series of multiple queries for features like ranking.
 * It's expected to typically be used as part of a {@link java.util.List}.
 */
public class SearchQueryWrapper {

    private final Query query;

    public SearchQueryWrapper(Query query) {
        this.query = query;
    }

    public Query getQuery() {
        return query;
    }
}
