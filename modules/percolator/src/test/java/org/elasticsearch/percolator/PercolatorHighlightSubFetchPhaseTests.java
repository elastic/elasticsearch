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
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.Highlighters;
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
        PercolateQuery percolateQuery = new PercolateQuery.Builder("", ctx -> null, new BytesArray("{}"),
                Mockito.mock(IndexSearcher.class))
                .build();

        PercolatorHighlightSubFetchPhase subFetchPhase = new PercolatorHighlightSubFetchPhase(Settings.EMPTY,
            new Highlighters(Settings.EMPTY));
        SearchContext searchContext = Mockito.mock(SearchContext.class);
        Mockito.when(searchContext.highlight()).thenReturn(new SearchContextHighlight(Collections.emptyList()));
        Mockito.when(searchContext.query()).thenReturn(new MatchAllDocsQuery());

        assertThat(subFetchPhase.hitsExecutionNeeded(searchContext), is(false));
        Mockito.when(searchContext.query()).thenReturn(percolateQuery);
        assertThat(subFetchPhase.hitsExecutionNeeded(searchContext), is(true));
    }

    public void testLocatePercolatorQuery() {
        PercolateQuery percolateQuery = new PercolateQuery.Builder("", ctx -> null, new BytesArray("{}"),
                Mockito.mock(IndexSearcher.class))
                .build();

        assertThat(PercolatorHighlightSubFetchPhase.locatePercolatorQuery(new MatchAllDocsQuery()), nullValue());
        BooleanQuery.Builder bq = new BooleanQuery.Builder();
        bq.add(new MatchAllDocsQuery(), BooleanClause.Occur.FILTER);
        assertThat(PercolatorHighlightSubFetchPhase.locatePercolatorQuery(bq.build()), nullValue());
        bq.add(percolateQuery, BooleanClause.Occur.FILTER);
        assertThat(PercolatorHighlightSubFetchPhase.locatePercolatorQuery(bq.build()), sameInstance(percolateQuery));

        ConstantScoreQuery constantScoreQuery = new ConstantScoreQuery(new MatchAllDocsQuery());
        assertThat(PercolatorHighlightSubFetchPhase.locatePercolatorQuery(constantScoreQuery), nullValue());
        constantScoreQuery = new ConstantScoreQuery(percolateQuery);
        assertThat(PercolatorHighlightSubFetchPhase.locatePercolatorQuery(constantScoreQuery), sameInstance(percolateQuery));

        BoostQuery boostQuery = new BoostQuery(new MatchAllDocsQuery(), 1f);
        assertThat(PercolatorHighlightSubFetchPhase.locatePercolatorQuery(boostQuery), nullValue());
        boostQuery = new BoostQuery(percolateQuery, 1f);
        assertThat(PercolatorHighlightSubFetchPhase.locatePercolatorQuery(boostQuery), sameInstance(percolateQuery));
    }

}
