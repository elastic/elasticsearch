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
            this.count = (int) searcher.searcher().count(query);
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
