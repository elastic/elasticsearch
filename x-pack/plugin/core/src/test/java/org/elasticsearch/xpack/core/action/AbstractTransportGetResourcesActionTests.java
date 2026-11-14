/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.action;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockUtils;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.action.GetFiltersAction;
import org.elasticsearch.xpack.core.ml.job.config.MlFilter;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class AbstractTransportGetResourcesActionTests extends ESTestCase {

    private ThreadPool threadPool;

    @Before
    public void setUpThreadPool() {
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void tearDownThreadPool() {
        terminate(threadPool);
    }

    public void testParseFailureCompletesListenerOnce() {
        var action = new UnparseableResourcesAction(new SearchHitsClient(threadPool, between(2, 5)));
        var responses = new ArrayList<QueryPage<MlFilter>>();
        var failures = new ArrayList<Exception>();

        action.searchResources(
            new GetFiltersAction.Request("_all"),
            TaskId.EMPTY_TASK_ID,
            ActionListener.wrap(responses::add, failures::add)
        );

        assertThat(responses, empty());
        assertThat(failures, contains(instanceOf(IOException.class)));
    }

    /**
     * Responds to every search with {@code hitCount} hits.
     */
    private static class SearchHitsClient extends NoOpClient {
        private final int hitCount;

        SearchHitsClient(ThreadPool threadPool, int hitCount) {
            super(threadPool);
            this.hitCount = hitCount;
        }

        @Override
        @SuppressWarnings("unchecked")
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            assertThat(action, equalTo(TransportSearchAction.TYPE));
            var hits = new SearchHit[hitCount];
            for (int i = 0; i < hitCount; i++) {
                hits[i] = new SearchHit(i);
                hits[i].sourceRef(new BytesArray("{}"));
            }
            var searchHits = new SearchHits(hits, new TotalHits(hitCount, TotalHits.Relation.EQUAL_TO), 1.0f);
            var response = SearchResponseUtils.successfulResponse(searchHits);
            searchHits.decRef(); // transfer ownership to response
            ActionListener.respondAndRelease((ActionListener<SearchResponse>) listener, response);
        }
    }

    /**
     * Fails to parse every resource, as a corrupt document would.
     */
    private static class UnparseableResourcesAction extends AbstractTransportGetResourcesAction<
        MlFilter,
        GetFiltersAction.Request,
        GetFiltersAction.Response> {

        UnparseableResourcesAction(Client client) {
            super(
                GetFiltersAction.NAME,
                MockUtils.setupTransportServiceWithThreadpoolExecutor(),
                new ActionFilters(Set.of()),
                GetFiltersAction.Request::new,
                client,
                NamedXContentRegistry.EMPTY
            );
        }

        @Override
        protected void doExecute(Task task, GetFiltersAction.Request request, ActionListener<GetFiltersAction.Response> listener) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected ParseField getResultsField() {
            return MlFilter.RESULTS_FIELD;
        }

        @Override
        protected String[] getIndices() {
            return new String[] { "test-index" };
        }

        @Override
        protected MlFilter parse(XContentParser parser) throws IOException {
            throw new IOException("unparseable resource");
        }

        @Override
        protected ResourceNotFoundException notFoundException(String resourceId) {
            return new ResourceNotFoundException(resourceId);
        }

        @Override
        protected String executionOrigin() {
            return "test";
        }

        @Override
        protected String extractIdFromResource(MlFilter resource) {
            return resource.getId();
        }
    }
}
