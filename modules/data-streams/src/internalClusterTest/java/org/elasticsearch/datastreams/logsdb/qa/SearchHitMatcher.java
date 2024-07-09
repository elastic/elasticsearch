/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.datastreams.logsdb.qa;

import org.elasticsearch.datastreams.logsdb.qa.exceptions.MatcherException;
import org.elasticsearch.datastreams.logsdb.qa.matchers.Matcher;
import org.elasticsearch.search.SearchHit;

public class SearchHitMatcher extends Matcher<SearchHit> {

    private final Matcher<SearchHit> matcher;

    public SearchHitMatcher(final Matcher<SearchHit> matcher) {
        this.matcher = matcher;
    }
    @Override
    public void match(final SearchHit a, final SearchHit b) throws MatcherException {

        matcher.match(a, b);
    }
}
