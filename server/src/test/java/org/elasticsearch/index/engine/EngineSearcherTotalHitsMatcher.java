/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.lucene.search.Queries;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.io.IOException;

public final class EngineSearcherTotalHitsMatcher extends TypeSafeMatcher<Engine.Searcher> {

    private final Query query;

    private final int totalHits;
    private int count;

    public EngineSearcherTotalHitsMatcher(Query query, int totalHits) {
        this.query = query;
        this.totalHits = totalHits;
    }

    @Override
    public boolean matchesSafely(Engine.Searcher searcher) {
        try {
            this.count = searcher.count(query);
            return count == totalHits;
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    protected void describeMismatchSafely(Engine.Searcher item, Description mismatchDescription) {
        mismatchDescription.appendText("was ").appendValue(count);
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("total hits of size ").appendValue(totalHits).appendText(" with query ").appendValue(query);
    }

    public static Matcher<Engine.Searcher> engineSearcherTotalHits(Query query, int totalHits) {
        return new EngineSearcherTotalHitsMatcher(query, totalHits);
    }

    public static Matcher<Engine.Searcher> engineSearcherTotalHits(int totalHits) {
        return new EngineSearcherTotalHitsMatcher(Queries.newMatchAllQuery(), totalHits);
    }
}
