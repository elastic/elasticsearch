/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.percolator;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.common.lucene.search.function.RandomScoreFunction;
import org.elasticsearch.search.fetch.FetchContext;
import org.elasticsearch.search.fetch.subphase.highlight.SearchHighlightContext;
import org.elasticsearch.test.ESTestCase;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;

public class PercolatorHighlightSubFetchPhaseTests extends ESTestCase {

    public void testHitsExecutionNeeded() {
        PercolateQuery percolateQuery = new PercolateQuery("_name", ctx -> null, Collections.singletonList(new BytesArray("{}")),
            new MatchAllDocsQuery(), Mockito.mock(IndexSearcher.class), null, new MatchAllDocsQuery());
        PercolatorHighlightSubFetchPhase subFetchPhase = new PercolatorHighlightSubFetchPhase(emptyMap());
        FetchContext fetchContext = mock(FetchContext.class);
        Mockito.when(fetchContext.highlight()).thenReturn(new SearchHighlightContext(Collections.emptyList()));
        Mockito.when(fetchContext.query()).thenReturn(new MatchAllDocsQuery());

        assertNull(subFetchPhase.getProcessor(fetchContext));
        Mockito.when(fetchContext.query()).thenReturn(percolateQuery);
        assertNotNull(subFetchPhase.getProcessor(fetchContext));
    }

    public void testLocatePercolatorQuery() {
        PercolateQuery percolateQuery = new PercolateQuery("_name", ctx -> null, Collections.singletonList(new BytesArray("{}")),
            new MatchAllDocsQuery(), Mockito.mock(IndexSearcher.class), null, new MatchAllDocsQuery());
        assertThat(PercolatorHighlightSubFetchPhase.locatePercolatorQuery(new MatchAllDocsQuery()).size(), equalTo(0));
        BooleanQuery.Builder bq = new BooleanQuery.Builder();
        bq.add(new MatchAllDocsQuery(), BooleanClause.Occur.FILTER);
        assertThat(PercolatorHighlightSubFetchPhase.locatePercolatorQuery(bq.build()).size(), equalTo(0));
        bq.add(percolateQuery, BooleanClause.Occur.FILTER);
        assertThat(PercolatorHighlightSubFetchPhase.locatePercolatorQuery(bq.build()).size(), equalTo(1));
        assertThat(PercolatorHighlightSubFetchPhase.locatePercolatorQuery(bq.build()).get(0), sameInstance(percolateQuery));

        ConstantScoreQuery constantScoreQuery = new ConstantScoreQuery(new MatchAllDocsQuery());
        assertThat(PercolatorHighlightSubFetchPhase.locatePercolatorQuery(constantScoreQuery).size(), equalTo(0));
        constantScoreQuery = new ConstantScoreQuery(percolateQuery);
        assertThat(PercolatorHighlightSubFetchPhase.locatePercolatorQuery(constantScoreQuery).size(), equalTo(1));
        assertThat(PercolatorHighlightSubFetchPhase.locatePercolatorQuery(constantScoreQuery).get(0), sameInstance(percolateQuery));

        BoostQuery boostQuery = new BoostQuery(new MatchAllDocsQuery(), 1f);
        assertThat(PercolatorHighlightSubFetchPhase.locatePercolatorQuery(boostQuery).size(), equalTo(0));
        boostQuery = new BoostQuery(percolateQuery, 1f);
        assertThat(PercolatorHighlightSubFetchPhase.locatePercolatorQuery(boostQuery).size(), equalTo(1));
        assertThat(PercolatorHighlightSubFetchPhase.locatePercolatorQuery(boostQuery).get(0), sameInstance(percolateQuery));

        FunctionScoreQuery functionScoreQuery = new FunctionScoreQuery(new MatchAllDocsQuery(), new RandomScoreFunction(0, 0, null));
        assertThat(PercolatorHighlightSubFetchPhase.locatePercolatorQuery(functionScoreQuery).size(), equalTo(0));
        functionScoreQuery = new FunctionScoreQuery(percolateQuery, new RandomScoreFunction(0, 0, null));
        assertThat(PercolatorHighlightSubFetchPhase.locatePercolatorQuery(functionScoreQuery).size(), equalTo(1));
        assertThat(PercolatorHighlightSubFetchPhase.locatePercolatorQuery(functionScoreQuery).get(0), sameInstance(percolateQuery));

        DisjunctionMaxQuery disjunctionMaxQuery = new DisjunctionMaxQuery(Collections.singleton(new MatchAllDocsQuery()), 1f);
        assertThat(PercolatorHighlightSubFetchPhase.locatePercolatorQuery(disjunctionMaxQuery).size(), equalTo(0));
        disjunctionMaxQuery = new DisjunctionMaxQuery(Arrays.asList(percolateQuery, new MatchAllDocsQuery()), 1f);
        assertThat(PercolatorHighlightSubFetchPhase.locatePercolatorQuery(disjunctionMaxQuery).size(), equalTo(1));
        assertThat(PercolatorHighlightSubFetchPhase.locatePercolatorQuery(disjunctionMaxQuery).get(0), sameInstance(percolateQuery));

        PercolateQuery percolateQuery2 = new PercolateQuery("_name", ctx -> null, Collections.singletonList(new BytesArray("{}")),
            new MatchAllDocsQuery(), Mockito.mock(IndexSearcher.class), null, new MatchAllDocsQuery());
        bq = new BooleanQuery.Builder();
        bq.add(new MatchAllDocsQuery(), BooleanClause.Occur.FILTER);
        assertThat(PercolatorHighlightSubFetchPhase.locatePercolatorQuery(bq.build()).size(), equalTo(0));
        bq.add(percolateQuery, BooleanClause.Occur.FILTER);
        bq.add(percolateQuery2, BooleanClause.Occur.FILTER);
        assertThat(PercolatorHighlightSubFetchPhase.locatePercolatorQuery(bq.build()).size(), equalTo(2));
        assertThat(PercolatorHighlightSubFetchPhase.locatePercolatorQuery(bq.build()),
            containsInAnyOrder(sameInstance(percolateQuery), sameInstance(percolateQuery2)));

        assertNotNull(PercolatorHighlightSubFetchPhase.locatePercolatorQuery(null));
        assertThat(PercolatorHighlightSubFetchPhase.locatePercolatorQuery(null).size(), equalTo(0));
    }

}
