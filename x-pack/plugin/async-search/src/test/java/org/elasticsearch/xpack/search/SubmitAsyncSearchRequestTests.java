/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.search;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchRequest;
import org.elasticsearch.xpack.core.transform.action.AbstractWireSerializingTransformTestCase;

import java.util.Collections;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

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
        } else {
            searchRequest = new SubmitAsyncSearchRequest();
        }
        if (randomBoolean()) {
            searchRequest.setWaitForCompletionTimeout(TimeValue.parseTimeValue(randomPositiveTimeValue(), "wait_for_completion"));
        }
        searchRequest.setKeepOnCompletion(randomBoolean());
        if (randomBoolean()) {
            searchRequest.setKeepAlive(TimeValue.parseTimeValue(randomPositiveTimeValue(), "keep_alive"));
        }
        if (randomBoolean()) {
            searchRequest.getSearchRequest()
                .indicesOptions(IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean()));
        }
        if (randomBoolean()) {
            searchRequest.getSearchRequest().preference(randomAlphaOfLengthBetween(3, 10));
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

    public void testValidateCssMinimizeRoundtrips() {
        SubmitAsyncSearchRequest req = new SubmitAsyncSearchRequest();
        req.getSearchRequest().setCcsMinimizeRoundtrips(true);
        ActionRequestValidationException exc = req.validate();
        assertNotNull(exc);
        assertThat(exc.validationErrors().size(), equalTo(1));
        assertThat(exc.validationErrors().get(0), containsString("[ccs_minimize_roundtrips]"));
    }

    public void testValidateScroll() {
        SubmitAsyncSearchRequest req = new SubmitAsyncSearchRequest();
        req.getSearchRequest().scroll(TimeValue.timeValueMinutes(5));
        ActionRequestValidationException exc = req.validate();
        assertNotNull(exc);
        assertThat(exc.validationErrors().size(), equalTo(2));
        // request_cache is activated by default
        assertThat(exc.validationErrors().get(0), containsString("[request_cache]"));
        assertThat(exc.validationErrors().get(1), containsString("[scroll]"));
    }

    public void testValidateKeepAlive() {
        SubmitAsyncSearchRequest req = new SubmitAsyncSearchRequest();
        req.setKeepAlive(TimeValue.timeValueMillis(randomIntBetween(1, 999)));
        ActionRequestValidationException exc = req.validate();
        assertNotNull(exc);
        assertThat(exc.validationErrors().size(), equalTo(1));
        assertThat(exc.validationErrors().get(0), containsString("[keep_alive]"));
    }

    public void testValidateSuggestOnly() {
        SubmitAsyncSearchRequest req = new SubmitAsyncSearchRequest();
        req.getSearchRequest().source(new SearchSourceBuilder().suggest(new SuggestBuilder()));
        ActionRequestValidationException exc = req.validate();
        assertNotNull(exc);
        assertThat(exc.validationErrors().size(), equalTo(1));
        assertThat(exc.validationErrors().get(0), containsString("suggest"));
    }

    public void testValidatePreFilterShardSize() {
        SubmitAsyncSearchRequest req = new SubmitAsyncSearchRequest();
        req.getSearchRequest().setPreFilterShardSize(randomIntBetween(2, Integer.MAX_VALUE));
        ActionRequestValidationException exc = req.validate();
        assertNotNull(exc);
        assertThat(exc.validationErrors().size(), equalTo(1));
        assertThat(exc.validationErrors().get(0), containsString("[pre_filter_shard_size]"));
    }

    public void testTaskDescription() {
        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(
            new SearchSourceBuilder().query(new MatchAllQueryBuilder()),
            "index"
        );
        Task task = request.createTask(1, "type", "action", null, Collections.emptyMap());
        assertEquals(
            "waitForCompletionTimeout[1s], keepOnCompletion[false] keepAlive[5d], request=indices[index], "
                + "search_type[QUERY_THEN_FETCH], source[{\"query\":{\"match_all\":{\"boost\":1.0}}}]",
            task.getDescription()
        );
    }
}
