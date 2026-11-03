/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.fetch.subphase.FieldAndFormat;
import org.elasticsearch.test.ESSingleNodeTestCase;

import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

/**
 * A pattern from the request that Lucene cannot determinize must be rejected with a 400 when it is compiled in the
 * fetch phase. Failures in the other shard phases go through {@link SearchService#wrapListenerForErrorHandling}.
 */
public class TooComplexPatternSearchTests extends ESSingleNodeTestCase {

    public void testFetchUnmappedFieldPattern() {
        // Two shards so that the fetch phase runs as its own shard request rather than together with the query phase
        client().admin().indices().prepareCreate("idx").setSettings(Settings.builder().put("index.number_of_shards", 2)).get();
        for (int i = 0; i < 4; i++) {
            client().prepareIndex("idx").setSource("kw", "x" + i).get();
        }
        client().admin().indices().prepareRefresh("idx").get();

        FieldAndFormat field = new FieldAndFormat("*" + "0".repeat(50_000) + "*", null, true);
        SearchPhaseExecutionException e = expectThrows(
            SearchPhaseExecutionException.class,
            client().prepareSearch("idx")
                .setQuery(QueryBuilders.matchAllQuery())
                .addFetchField(field)
                .setAllowPartialSearchResults(false)::get
        );

        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.shardFailures(), not(emptyArray()));
        for (ShardSearchFailure failure : e.shardFailures()) {
            assertThat(failure.status(), equalTo(RestStatus.BAD_REQUEST));
            assertThat(ExceptionsHelper.unwrap(failure.getCause(), TooComplexToDeterminizeException.class), notNullValue());
        }
    }
}
