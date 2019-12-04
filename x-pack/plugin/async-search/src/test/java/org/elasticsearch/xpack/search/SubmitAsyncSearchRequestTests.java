/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.search;

import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.Writeable;
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
        SubmitAsyncSearchRequest searchRequest = new SubmitAsyncSearchRequest();
        searchRequest.allowPartialSearchResults(randomBoolean());
        if (randomBoolean()) {
            searchRequest.indices(generateRandomStringArray(10, 10, false, false));
        }
        if (randomBoolean()) {
            searchRequest.indicesOptions(IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean()));
        }
        if (randomBoolean()) {
            searchRequest.preference(randomAlphaOfLengthBetween(3, 10));
        }
        if (randomBoolean()) {
            searchRequest.requestCache(randomBoolean());
        }
        if (randomBoolean()) {
            searchRequest.routing(randomAlphaOfLengthBetween(3, 10));
        }
        if (randomBoolean()) {
            searchRequest.searchType(randomFrom(SearchType.DFS_QUERY_THEN_FETCH, SearchType.QUERY_THEN_FETCH));
        }
        if (randomBoolean()) {
            searchRequest.source(randomSearchSourceBuilder());
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

    public void testValidateScroll() {

    }

    public void testValidateSuggestOnly() {

    }

    public void testValidateWaitForCompletion() {

    }
}
