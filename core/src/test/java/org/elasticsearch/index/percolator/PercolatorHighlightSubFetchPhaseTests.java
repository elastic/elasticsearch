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
package org.elasticsearch.index.percolator;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.index.query.PercolatorQuery;
import org.elasticsearch.search.highlight.SearchContextHighlight;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.ESTestCase;
import org.mockito.Mockito;

import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class PercolatorHighlightSubFetchPhaseTests extends ESTestCase {

    public void testHitsExecutionNeeded() {
        PercolatorQuery percolatorQuery = new PercolatorQuery.Builder("", ctx -> null, new BytesArray("{}"),
                Mockito.mock(IndexSearcher.class))
                .build();

        PercolatorHighlightSubFetchPhase subFetchPhase = new PercolatorHighlightSubFetchPhase(null);
        SearchContext searchContext = Mockito.mock(SearchContext.class);
        Mockito.when(searchContext.highlight()).thenReturn(new SearchContextHighlight(Collections.emptyList()));
        Mockito.when(searchContext.query()).thenReturn(new MatchAllDocsQuery());

        assertThat(subFetchPhase.hitsExecutionNeeded(searchContext), is(false));
        IllegalStateException exception = expectThrows(IllegalStateException.class,
                () -> subFetchPhase.hitsExecute(searchContext, null));
        assertThat(exception.getMessage(), equalTo("couldn't locate percolator query"));

        Mockito.when(searchContext.query()).thenReturn(percolatorQuery);
        assertThat(subFetchPhase.hitsExecutionNeeded(searchContext), is(true));
    }

    public void testLocatePercolatorQuery() {
        PercolatorQuery percolatorQuery = new PercolatorQuery.Builder("", ctx -> null, new BytesArray("{}"),
                Mockito.mock(IndexSearcher.class))
                .build();

        assertThat(PercolatorHighlightSubFetchPhase.locatePercolatorQuery(new MatchAllDocsQuery()), nullValue());
        BooleanQuery.Builder bq = new BooleanQuery.Builder();
        bq.add(new MatchAllDocsQuery(), BooleanClause.Occur.FILTER);
        assertThat(PercolatorHighlightSubFetchPhase.locatePercolatorQuery(bq.build()), nullValue());
        bq.add(percolatorQuery, BooleanClause.Occur.FILTER);
        assertThat(PercolatorHighlightSubFetchPhase.locatePercolatorQuery(bq.build()), sameInstance(percolatorQuery));

        ConstantScoreQuery constantScoreQuery = new ConstantScoreQuery(new MatchAllDocsQuery());
        assertThat(PercolatorHighlightSubFetchPhase.locatePercolatorQuery(constantScoreQuery), nullValue());
        constantScoreQuery = new ConstantScoreQuery(percolatorQuery);
        assertThat(PercolatorHighlightSubFetchPhase.locatePercolatorQuery(constantScoreQuery), sameInstance(percolatorQuery));

        BoostQuery boostQuery = new BoostQuery(new MatchAllDocsQuery(), 1f);
        assertThat(PercolatorHighlightSubFetchPhase.locatePercolatorQuery(boostQuery), nullValue());
        boostQuery = new BoostQuery(percolatorQuery, 1f);
        assertThat(PercolatorHighlightSubFetchPhase.locatePercolatorQuery(boostQuery), sameInstance(percolatorQuery));
    }

}
