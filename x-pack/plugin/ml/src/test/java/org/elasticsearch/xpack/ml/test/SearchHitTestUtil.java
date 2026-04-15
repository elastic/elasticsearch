/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.test;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import static org.mockito.Mockito.doAnswer;

public final class SearchHitTestUtil {

    private SearchHitTestUtil() {}

    public static void decRefHits(SearchHit[] hits) {
        if (hits == null) {
            return;
        }
        for (SearchHit h : hits) {
            if (h != null) {
                h.decRef();
            }
        }
    }

    public static void stubSearchResponseDecRefsHits(SearchResponse searchResponse, SearchHits searchHits) {
        doAnswer(inv -> {
            searchHits.decRef();
            return true;
        }).when(searchResponse).decRef();
    }
}
