/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.execution.search;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.execution.search.extractor.ConstantExtractor;
import org.elasticsearch.xpack.ql.execution.search.extractor.HitExtractor;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.execution.search.extractor.FieldHitExtractor;

import java.time.ZoneId;
import java.util.BitSet;
import java.util.List;

import static org.elasticsearch.xpack.ql.execution.search.extractor.AbstractFieldHitExtractor.MultiValueSupport.NONE;
import static org.hamcrest.Matchers.containsString;

public class SearchHitRowSetTests extends ESTestCase {

    public void testForEachRowReleasesExtraRefOnSearchHits() {
        SearchHit hit = new SearchHit(1, "doc");
        SearchHits hits = new SearchHits(new SearchHit[] { hit }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.0f);
        SearchResponse response = newSearchResponse(hits);
        try {
            BitSet mask = new BitSet();
            mask.set(0);
            List<HitExtractor> exts = List.of(new ConstantExtractor(1));
            SearchHitRowSet rowSet = new SearchHitRowSet(exts, mask, 10, -1, response);
            assertTrue(hits.hasReferences());
            rowSet.forEachRow(rv -> assertEquals(1, rv.column(0)));
            assertTrue(hits.hasReferences());
        } finally {
            response.decRef();
            hits.decRef();
        }
        assertFalse(hits.hasReferences());
    }

    public void testReleaseSearchHitsIdempotentAfterForEachRow() {
        SearchHit hit = new SearchHit(1, "doc");
        SearchHits hits = new SearchHits(new SearchHit[] { hit }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.0f);
        SearchResponse response = newSearchResponse(hits);
        try {
            BitSet mask = new BitSet();
            mask.set(0);
            SearchHitRowSet rowSet = new SearchHitRowSet(List.of(new ConstantExtractor(1)), mask, 10, -1, response);
            rowSet.forEachRow(rv -> {});
            rowSet.releaseSearchHits();
            rowSet.releaseSearchHits();
        } finally {
            response.decRef();
            hits.decRef();
        }
        assertFalse(hits.hasReferences());
    }

    public void testForEachRowWithEmptyHitsDoesNotThrow() {
        SearchHits hits = SearchHits.EMPTY_WITH_TOTAL_HITS;
        SearchResponse response = newSearchResponse(hits);
        try {
            BitSet mask = new BitSet();
            SearchHitRowSet rowSet = new SearchHitRowSet(List.of(new ConstantExtractor(1)), mask, 10, -1, response);
            rowSet.forEachRow(rv -> fail("expected no rows"));
        } finally {
            response.decRef();
        }
    }

    public void testConstructorFailureDecRefsAcquiredHits() {
        SearchHit hit = new SearchHit(1, "doc");
        SearchHits hits = new SearchHits(new SearchHit[] { hit }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.0f);
        SearchResponse response = newSearchResponse(hits);
        BitSet mask = new BitSet();
        mask.set(0);
        mask.set(1);
        List<HitExtractor> exts = List.of(
            new FieldHitExtractor("hit_a.f1", null, ZoneId.of("UTC"), "hit_a", NONE),
            new FieldHitExtractor("hit_b.f2", null, ZoneId.of("UTC"), "hit_b", NONE)
        );
        SqlIllegalArgumentException ex = expectThrows(
            SqlIllegalArgumentException.class,
            () -> new SearchHitRowSet(exts, mask, 10, -1, response)
        );
        assertThat(ex.getMessage(), containsString("Multi-nested"));
        response.decRef();
        hits.decRef();
        assertFalse(hits.hasReferences());
    }

    private static SearchResponse newSearchResponse(SearchHits hits) {
        return new SearchResponse(
            hits,
            null,
            null,
            false,
            null,
            null,
            0,
            null,
            1,
            1,
            0,
            0L,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );
    }
}
