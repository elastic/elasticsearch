/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.profile;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.search.profile.query.CollectorResult;
import org.elasticsearch.search.profile.query.QueryProfileShardResult;
import org.elasticsearch.search.profile.query.QueryProfileShardResultTests;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class SearchProfileDfsPhaseResultTests extends AbstractXContentSerializingTestCase<SearchProfileDfsPhaseResult> {

    static SearchProfileDfsPhaseResult createTestItem() {
        return new SearchProfileDfsPhaseResult(
            randomBoolean() ? null : ProfileResultTests.createTestItem(1),
            randomBoolean() ? null : randomList(1, 10, QueryProfileShardResultTests::createTestItem)
        );
    }

    @Override
    protected SearchProfileDfsPhaseResult createTestInstance() {
        return createTestItem();
    }

    @Override
    protected SearchProfileDfsPhaseResult mutateInstance(SearchProfileDfsPhaseResult instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Reader<SearchProfileDfsPhaseResult> instanceReader() {
        return SearchProfileDfsPhaseResult::new;
    }

    @Override
    protected SearchProfileDfsPhaseResult doParseInstance(XContentParser parser) throws IOException {
        return SearchProfileDfsPhaseResult.fromXContent(parser);
    }

    public void testCombineQueryProfileShardResults() {
        assertThat(new SearchProfileDfsPhaseResult(null, null).combineQueryProfileShardResults(), is(nullValue()));

        List<QueryProfileShardResult> resultList = randomList(5, 5, QueryProfileShardResultTests::createTestItem);

        SearchProfileDfsPhaseResult result = new SearchProfileDfsPhaseResult(null, resultList);
        QueryProfileShardResult queryProfileShardResult = result.combineQueryProfileShardResults();
        assertThat(
            queryProfileShardResult.getRewriteTime(),
            equalTo(resultList.stream().mapToLong(QueryProfileShardResult::getRewriteTime).sum())
        );
        assertThat(
            queryProfileShardResult.getCollectorResult().getTime(),
            equalTo(resultList.stream().map(QueryProfileShardResult::getCollectorResult).mapToLong(CollectorResult::getTime).sum())
        );
        assertThat(queryProfileShardResult.getCollectorResult().getProfiledChildren().size(), equalTo(resultList.size()));
        assertThat(
            queryProfileShardResult.getQueryResults().size(),
            equalTo((int) resultList.stream().mapToLong(q -> q.getQueryResults().size()).sum())
        );
    }
}
