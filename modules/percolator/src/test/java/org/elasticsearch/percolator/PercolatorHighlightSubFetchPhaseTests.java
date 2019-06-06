/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
import org.elasticsearch.search.fetch.subphase.highlight.SearchContextHighlight;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.ESTestCase;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class PercolatorHighlightSubFetchPhaseTests extends ESTestCase {

    public void testHitsExecutionNeeded() {
        PercolateQuery percolateQuery = new PercolateQuery("_name", ctx -> null, Collections.singletonList(new BytesArray("{}")),
            new MatchAllDocsQuery(), Mockito.mock(IndexSearcher.class), null, new MatchAllDocsQuery());
        PercolatorHighlightSubFetchPhase subFetchPhase = new PercolatorHighlightSubFetchPhase(emptyMap());
        SearchContext searchContext = Mockito.mock(SearchContext.class);
        Mockito.when(searchContext.highlight()).thenReturn(new SearchContextHighlight(Collections.emptyList()));
        Mockito.when(searchContext.query()).thenReturn(new MatchAllDocsQuery());

        assertThat(subFetchPhase.hitsExecutionNeeded(searchContext), is(false));
        Mockito.when(searchContext.query()).thenReturn(percolateQuery);
        assertThat(subFetchPhase.hitsExecutionNeeded(searchContext), is(true));
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
        assertThat(PercolatorHighlightSubFetchPhase.locatePercolatorQuery(bq.build()).get(0), sameInstance(percolateQuery));
        assertThat(PercolatorHighlightSubFetchPhase.locatePercolatorQuery(bq.build()).get(1), sameInstance(percolateQuery2));
    }

}
