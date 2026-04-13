/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.synonyms;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.FilterClient;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpNodeClient;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.action.synonyms.SynonymsTestUtils.randomSynonymsSet;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class SynonymsManagementAPIServiceTests extends ESTestCase {

    /**
     * Verifies that appending to a non-existent synonym set creates it (CREATED status) and
     * does not issue a delete-by-query.
     */
    public void testAppendToNonExistentSetCreatesIt() throws Exception {
        SynonymRule[] rules = randomSynonymsSet(3);

        try (var threadPool = createThreadPool()) {
            var client = new AppendTestClient(new NoOpNodeClient(threadPool), false, 0);
            var service = new SynonymsManagementAPIService(client, rules.length + 1, PIT_BATCH_SIZE);

            var future = new PlainActionFuture<SynonymsManagementAPIService.SynonymsReloadResult>();
            service.putSynonymsSet("my-set", rules, false, true, future);
            var result = future.actionGet();

            assertThat(result.synonymsOperationResult(), equalTo(SynonymsManagementAPIService.UpdateSynonymsResultStatus.CREATED));
            assertThat("delete-by-query must not be issued on append", client.deleteByQueryIssued.get(), equalTo(false));
            assertThat("bulk insert must be issued", client.bulkRequestCount.get(), equalTo(1));
        }
    }

    /**
     * Verifies that appending to an existing synonym set adds rules without deleting existing ones
     * (UPDATED status).
     */
    public void testAppendToExistingSetUpdatesIt() throws Exception {
        SynonymRule[] rules = randomSynonymsSet(3);
        int existingCount = 5;

        try (var threadPool = createThreadPool()) {
            var client = new AppendTestClient(new NoOpNodeClient(threadPool), true, existingCount);
            var service = new SynonymsManagementAPIService(client, existingCount + rules.length + 1, PIT_BATCH_SIZE);

            var future = new PlainActionFuture<SynonymsManagementAPIService.SynonymsReloadResult>();
            service.putSynonymsSet("my-set", rules, false, true, future);
            var result = future.actionGet();

            assertThat(result.synonymsOperationResult(), equalTo(SynonymsManagementAPIService.UpdateSynonymsResultStatus.UPDATED));
            assertThat("delete-by-query must not be issued on append", client.deleteByQueryIssued.get(), equalTo(false));
            assertThat("bulk insert must be issued", client.bulkRequestCount.get(), equalTo(1));
        }
    }

    /**
     * Verifies that an append that would push the total rule count above the limit is rejected.
     */
    public void testAppendRejectedWhenLimitExceeded() throws Exception {
        int maxRules = 10;
        int existingCount = 8;
        SynonymRule[] rules = randomSynonymsSet(3); // 8 + 3 = 11 > 10

        try (var threadPool = createThreadPool()) {
            var client = new AppendTestClient(new NoOpNodeClient(threadPool), true, existingCount);
            var service = new SynonymsManagementAPIService(client, maxRules, PIT_BATCH_SIZE);

            boolean[] failed = { false };
            Exception[] holder = { null };
            service.putSynonymsSet("my-set", rules, false, true, ActionListener.wrap(r -> fail("expected failure"), e -> {
                failed[0] = true;
                holder[0] = e;
            }));

            assertTrue("expected onFailure", failed[0]);
            assertThat(holder[0], instanceOf(IllegalArgumentException.class));
            assertThat(holder[0].getMessage(), containsString("exceed"));
            assertThat("no bulk insert should be issued", client.bulkRequestCount.get(), equalTo(0));
        }
    }

    // The field is package-private on main; access it directly.
    private static final int PIT_BATCH_SIZE = SynonymsManagementAPIService.PIT_BATCH_SIZE;

    /**
     * A {@link FilterClient} that simulates GET (set existence), SEARCH (rule count), and BULK
     * (insert) responses for append-mode tests. Fails the test if a delete-by-query is attempted.
     */
    private static class AppendTestClient extends FilterClient {
        final AtomicBoolean deleteByQueryIssued = new AtomicBoolean(false);
        final AtomicInteger bulkRequestCount = new AtomicInteger();
        private final boolean setExists;
        private final long existingRuleCount;

        AppendTestClient(NoOpNodeClient in, boolean setExists, long existingRuleCount) {
            super(in);
            this.setExists = setExists;
            this.existingRuleCount = existingRuleCount;
        }

        @Override
        @SuppressWarnings("unchecked")
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            if (request instanceof GetRequest getRequest) {
                var result = new GetResult(
                    getRequest.index(),
                    getRequest.id(),
                    setExists ? 0L : SequenceNumbers.UNASSIGNED_SEQ_NO,
                    setExists ? 1L : SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
                    setExists ? 1 : -1,
                    setExists,
                    null,
                    null,
                    null
                );
                ((ActionListener<GetResponse>) listener).onResponse(new GetResponse(result));
                return;
            }
            if (request instanceof SearchRequest) {
                SearchHits hits = SearchHits.empty(new TotalHits(existingRuleCount, TotalHits.Relation.EQUAL_TO), Float.NaN);
                ((ActionListener<SearchResponse>) listener).onResponse(SearchResponseUtils.successfulResponse(hits));
                return;
            }
            if (request instanceof BulkRequest) {
                bulkRequestCount.incrementAndGet();
                ((ActionListener<BulkResponse>) listener).onResponse(new BulkResponse(new BulkItemResponse[0], 0L));
                return;
            }
            if (DeleteByQueryAction.INSTANCE.equals(action)) {
                deleteByQueryIssued.set(true);
                fail("delete-by-query must not be issued during append");
            }
            super.doExecute(action, request, listener);
        }
    }
}
