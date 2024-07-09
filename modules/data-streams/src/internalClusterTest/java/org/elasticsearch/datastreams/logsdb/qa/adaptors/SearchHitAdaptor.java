/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.datastreams.logsdb.qa.adaptors;

import org.elasticsearch.search.SearchHit;

import java.util.Arrays;
import java.util.Objects;

public class SearchHitAdaptor implements MatcherAdaptor<SearchHit[], Object[]> {

    @SuppressWarnings("unchecked")
    @Override
    public Object[] adapt(SearchHit[] hits) {
        return Arrays.stream(hits).map(searchHit -> Objects.requireNonNull(searchHit.getSourceAsMap())).toArray();
    }
}
