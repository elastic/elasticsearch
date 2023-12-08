/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search;

import org.elasticsearch.action.search.SearchRequestBuilder;

public enum SearchResponseUtils {
    ;

    public static long getTotalHitsValue(SearchRequestBuilder request) {
        var resp = request.get();
        try {
            return resp.getHits().getTotalHits().value;
        } finally {
            resp.decRef();
        }
    }
}
