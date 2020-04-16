/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.core.eql.action.AsyncEqlSearchResponse;
import org.elasticsearch.xpack.core.eql.action.DeleteAsyncEqlSearchAction;
import org.elasticsearch.xpack.core.eql.action.EqlSearchAction;
import org.elasticsearch.xpack.core.eql.action.EqlSearchRequest;
import org.elasticsearch.xpack.core.eql.action.EqlSearchResponse;
import org.elasticsearch.xpack.core.eql.action.GetAsyncEqlSearchAction;
import org.elasticsearch.xpack.core.eql.action.SubmitAsyncEqlSearchAction;
import org.elasticsearch.xpack.core.eql.action.SubmitAsyncEqlSearchRequest;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.After;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFutureThrows;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class AsyncEqlSearchActionIT extends AbstractEqlBlockingIntegTestCase {

    private final ExecutorService executorService = Executors.newFixedThreadPool(1);

    /**
     * Shutdown the executor so we don't leak threads into other test runs.
     */
    @After
    public void shutdownExec() {
        executorService.shutdown();
    }

    private void prepareIndex() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
            .setMapping("val", "type=integer", "event_type", "type=keyword", "@timestamp", "type=date", "ip", "type=ip")
            .get());
        createIndex("idx_unmapped");

        int numDocs = randomIntBetween(6, 20);

        List<IndexRequestBuilder> builders = new ArrayList<>();

        for (int i = 0; i < numDocs; i++) {
            int fieldValue = randomIntBetween(0, 10);
            builders.add(client().prepareIndex("test").setSource(
                jsonBuilder().startObject()
                    .field("val", fieldValue).field("event_type", "my_event").field("@timestamp", "2020-04-09T12:35:48Z")
                    .endObject()));
        }
        indexRandom(true, builders);
    }


    public void testBlockedSuccess() throws Exception {
        prepareIndex();

        EqlSearchRequest request = new EqlSearchRequest().indices("test").query("my_event where val=1").eventCategoryField("event_type");
        EqlSearchResponse expectedResponse = client().execute(EqlSearchAction.INSTANCE, request).get();

        List<SearchBlockPlugin> plugins = initBlockFactory(true, false);
        SubmitAsyncEqlSearchRequest asyncRequest = new SubmitAsyncEqlSearchRequest(request)
            .setWaitForCompletionTimeout(TimeValue.timeValueMillis(1));
        logger.trace("Starting async search");
        AsyncEqlSearchResponse response = client().execute(SubmitAsyncEqlSearchAction.INSTANCE, asyncRequest).get();
        assertThat(response.isRunning(), is(true));
        assertThat(response.isPartial(), is(true));
        assertThat(response.getId(), notNullValue());
        assertThat(response.getFailure(), nullValue());
        assertThat(response.getEqlSearchResponse(), nullValue());

        logger.trace("Waiting for block to be established");
        awaitForBlockedSearches(plugins, "test");
        logger.trace("Block is established");

        disableBlocks(plugins);
        String id = response.getId();

        assertBusy(() ->
        {
            AsyncEqlSearchResponse finalResponse =
                client().execute(GetAsyncEqlSearchAction.INSTANCE, new GetAsyncEqlSearchAction.Request(id)).get();
            assertThat(finalResponse.getId(), equalTo(id));
            assertThat(finalResponse.isRunning(), is(false));
            assertThat(finalResponse.getFailure(), nullValue());
            assertThat(finalResponse.getEqlSearchResponse().hits().events(), notNullValue());
            assertThat(finalResponse.getEqlSearchResponse(), eqlSearchResponseMatcherEqualTo(expectedResponse));
        });
    }

    public void testBlockedCancellation() throws Exception {
        prepareIndex();

        EqlSearchRequest request = new EqlSearchRequest().indices("test").query("my_event where val=1")
            .eventCategoryField("event_type");

        List<SearchBlockPlugin> plugins = initBlockFactory(true, false);
        SubmitAsyncEqlSearchRequest asyncRequest = new SubmitAsyncEqlSearchRequest(request)
            .setWaitForCompletionTimeout(TimeValue.timeValueMillis(1));
        logger.trace("Starting async search");
        String id = randomAlphaOfLength(10);
        AsyncEqlSearchResponse response = client().filterWithHeader(Collections.singletonMap(Task.X_OPAQUE_ID, id))
            .execute(SubmitAsyncEqlSearchAction.INSTANCE, asyncRequest).get();
        assertThat(response.isRunning(), is(true));
        assertThat(response.isPartial(), is(true));
        assertThat(response.getId(), notNullValue());
        assertThat(response.getFailure(), nullValue());
        assertThat(response.getEqlSearchResponse(), nullValue());

        logger.trace("Waiting for block to be established");
        awaitForBlockedSearches(plugins, "test");
        logger.trace("Block is established");

        String asyncId = response.getId();
        response= client().execute(GetAsyncEqlSearchAction.INSTANCE, new GetAsyncEqlSearchAction.Request(asyncId)).get();
        assertThat(response.isRunning(), is(true));
        assertThat(response.isPartial(), is(true));
        assertThat(response.getId(), notNullValue());
        assertThat(response.getFailure(), nullValue());
        assertThat(response.getEqlSearchResponse(), nullValue());

        if(randomBoolean()) {
            TaskId taskId = cancelTaskWithXOpaqueId(id, EqlSearchAction.NAME);
            assertNotNull(taskId);
        } else {
            assertAcked(client().execute(DeleteAsyncEqlSearchAction.INSTANCE, new DeleteAsyncEqlSearchAction.Request(asyncId)).get());
        }
        disableBlocks(plugins);

        assertBusy(() ->
        {
            // Assert Cleanup
            assertFutureThrows(client().execute(GetAsyncEqlSearchAction.INSTANCE, new GetAsyncEqlSearchAction.Request(asyncId)),
                ResourceNotFoundException.class);
        });
    }

    public static org.hamcrest.Matcher<EqlSearchResponse> eqlSearchResponseMatcherEqualTo(EqlSearchResponse eqlSearchResponse) {
        return new BaseMatcher<>() {

            @Override
            public void describeTo(Description description) {
                description.appendText(Strings.toString(eqlSearchResponse));
            }

            @Override
            public boolean matches(Object o) {
                if (eqlSearchResponse == o) {
                    return true;
                }
                if (o == null || EqlSearchResponse.class != o.getClass()) {
                    return false;
                }
                EqlSearchResponse that = (EqlSearchResponse) o;
                // We don't compare took since it might deffer
                return Objects.equals(eqlSearchResponse.hits(), that.hits())
                    && Objects.equals(eqlSearchResponse.isTimeout(), that.isTimeout());
            }
        };
    }
}
