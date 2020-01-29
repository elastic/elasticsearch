/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.search;

import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchRequest;
import org.elasticsearch.xpack.core.transform.action.AbstractWireSerializingTransformTestCase;

public class SubmitAsyncSearchRequestTests extends AbstractWireSerializingTransformTestCase<SubmitAsyncSearchRequest> {
    @Override
    protected Writeable.Reader<SubmitAsyncSearchRequest> instanceReader() {
        return SubmitAsyncSearchRequest::new;
    }

    @Override
    protected SubmitAsyncSearchRequest createTestInstance() {
        final SubmitAsyncSearchRequest searchRequest;
        if (randomBoolean()) {
            searchRequest = new SubmitAsyncSearchRequest(generateRandomStringArray(10, 10, false, false));
        }  else {
            searchRequest = new SubmitAsyncSearchRequest();
        }
        if (randomBoolean()) {
            searchRequest.setWaitForCompletion(TimeValue.parseTimeValue(randomPositiveTimeValue(), "wait_for_completion"));
        }
        searchRequest.setCleanOnCompletion(randomBoolean());
        if (randomBoolean()) {
            searchRequest.setKeepAlive(TimeValue.parseTimeValue(randomPositiveTimeValue(), "keep_alive"));
        }
        if (randomBoolean()) {
            searchRequest.getSearchRequest()
                .indicesOptions(IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean()));
        }
        if (randomBoolean()) {
            searchRequest.getSearchRequest()
                .preference(randomAlphaOfLengthBetween(3, 10));
        }
        if (randomBoolean()) {
            searchRequest.getSearchRequest().requestCache(randomBoolean());
        }
        if (randomBoolean()) {
            searchRequest.getSearchRequest().searchType(randomFrom(SearchType.DFS_QUERY_THEN_FETCH, SearchType.QUERY_THEN_FETCH));
        }
        if (randomBoolean()) {
            searchRequest.getSearchRequest().source(randomSearchSourceBuilder());
        }
        return searchRequest;
    }

    protected SearchSourceBuilder randomSearchSourceBuilder() {
        SearchSourceBuilder source = new SearchSourceBuilder();
        if (randomBoolean()) {
            source.query(QueryBuilders.termQuery("foo", "bar"));
        }
        if (randomBoolean()) {
            source.aggregation(AggregationBuilders.max("max").field("field"));
        }
        return source;
    }
}
