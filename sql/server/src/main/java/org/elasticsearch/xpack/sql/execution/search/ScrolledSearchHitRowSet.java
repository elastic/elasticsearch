/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.execution.search;

import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.sql.execution.search.extractor.HitExtractor;
import org.elasticsearch.xpack.sql.type.Schema;

import java.util.List;

/**
 * "Next" page of results from a scroll search. Distinct from the first page
 * because it no longer has the {@link Schema}. See {@link InitialSearchHitRowSet}
 * for the initial results.
 */
public class ScrolledSearchHitRowSet extends AbstractSearchHitRowSet {
    private final int columnCount;

    public ScrolledSearchHitRowSet(List<HitExtractor> exts, SearchHit[] hits, int limitHits, String scrollId) {
        super(exts, hits, limitHits, scrollId);
        this.columnCount = exts.size();
    }

    @Override
    public int columnCount() {
        return columnCount;
    }
}
