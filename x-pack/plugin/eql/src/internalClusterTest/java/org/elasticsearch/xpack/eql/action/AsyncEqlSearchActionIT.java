/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;
import org.elasticsearch.xpack.core.async.DeleteAsyncResultAction;
import org.elasticsearch.xpack.core.async.DeleteAsyncResultRequest;
import org.elasticsearch.xpack.core.async.GetAsyncResultRequest;
import org.elasticsearch.xpack.eql.async.StoredAsyncResponse;
import org.elasticsearch.xpack.eql.plugin.EqlAsyncGetResultAction;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.After;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFutureThrows;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class AsyncEqlSearchActionIT extends AbstractEqlBlockingIntegTestCase {

    private final ExecutorService executorService = Executors.newFixedThreadPool(1);

    NamedWriteableRegistry registry = new NamedWriteableRegistry(new SearchModule(Settings.EMPTY,
        Collections.emptyList()).getNamedWriteables());

    /**
     * Shutdown the executor so we don't leak threads into other test runs.
     */
    @After
    public void shutdownExec() {
        executorService.shutdown();
    }

    private void prepareIndex() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
            .setMapping("val", "type=integer", "event_type", "type=keyword", "@timestamp", "type=date", "i", "type=integer")
            .get());
        createIndex("idx_unmapped");

        int numDocs = randomIntBetween(6, 20);

        List<IndexRequestBuilder> builders = new ArrayList<>();

        for (int i = 0; i < numDocs; i++) {
            int fieldValue = randomIntBetween(0, 10);
            builders.add(client().prepareIndex("test").setSource(
                jsonBuilder().startObject()
                    .field("val", fieldValue)
                    .field("event_type", "my_event")
                    .field("@timestamp", "2020-04-09T12:35:48Z")
                    .field("i", i)
                    .endObject()));
        }
        indexRandom(true, builders);
    }

    public void testBasicAsyncExecution() throws Exception {
        prepareIndex();

        boolean success = randomBoolean();
        String query = success ? "my_event where i=1" : "my_event where 10/i=1";
        EqlSearchRequest request = new EqlSearchRequest().indices("test").query(query).eventCategoryField("event_type")
            .waitForCompletionTimeout(TimeValue.timeValueMillis(1));

        List<SearchBlockPlugin> plugins = initBlockFactory(true, false);

        logger.trace("Starting async search");
        EqlSearchResponse response = client().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.isRunning(), is(true));
        assertThat(response.isPartial(), is(true));
        assertThat(response.id(), notNullValue());

        logger.trace("Waiting for block to be established");
        awaitForBlockedSearches(plugins, "test");
        logger.trace("Block is established");

        if (randomBoolean()) {
            // let's timeout first
            GetAsyncResultRequest getResultsRequest = new GetAsyncResultRequest(response.id())
                .setWaitForCompletionTimeout(TimeValue.timeValueMillis(10));
            EqlSearchResponse responseWithTimeout = client().execute(EqlAsyncGetResultAction.INSTANCE, getResultsRequest).get();
            assertThat(responseWithTimeout.isRunning(), is(true));
            assertThat(responseWithTimeout.isPartial(), is(true));
            assertThat(responseWithTimeout.id(), equalTo(response.id()));
        }

        // Now we wait
        GetAsyncResultRequest getResultsRequest = new GetAsyncResultRequest(response.id())
            .setWaitForCompletionTimeout(TimeValue.timeValueSeconds(10));
        ActionFuture<EqlSearchResponse> future = client().execute(EqlAsyncGetResultAction.INSTANCE, getResultsRequest);
        disableBlocks(plugins);
        if (success) {
            response = future.get();
            assertThat(response, notNullValue());
            assertThat(response.hits().events().size(), equalTo(1));
        } else {
            Exception ex = expectThrows(Exception.class, future::actionGet);
            assertThat(ex.getCause().getMessage(), containsString("by zero"));
        }

        AcknowledgedResponse deleteResponse =
            client().execute(DeleteAsyncResultAction.INSTANCE, new DeleteAsyncResultRequest(response.id())).actionGet();
        assertThat(deleteResponse.isAcknowledged(), equalTo(true));
    }

    public void testGoingAsync() throws Exception {
        prepareIndex();

        boolean success = randomBoolean();
        String query = success ? "my_event where i=1" : "my_event where 10/i=1";
        EqlSearchRequest request = new EqlSearchRequest().indices("test").query(query).eventCategoryField("event_type")
            .waitForCompletionTimeout(TimeValue.timeValueMillis(1));

        boolean customKeepAlive = randomBoolean();
        TimeValue keepAliveValue;
        if (customKeepAlive) {
            keepAliveValue = TimeValue.parseTimeValue(randomTimeValue(1, 5, "d"), "test");
            request.keepAlive(keepAliveValue);
        } else {
            keepAliveValue = EqlSearchRequest.DEFAULT_KEEP_ALIVE;
        }

        List<SearchBlockPlugin> plugins = initBlockFactory(true, false);

        String opaqueId = randomAlphaOfLength(10);
        logger.trace("Starting async search");
        EqlSearchResponse response = client().filterWithHeader(Collections.singletonMap(Task.X_OPAQUE_ID, opaqueId))
            .execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.isRunning(), is(true));
        assertThat(response.isPartial(), is(true));
        assertThat(response.id(), notNullValue());

        logger.trace("Waiting for block to be established");
        awaitForBlockedSearches(plugins, "test");
        logger.trace("Block is established");

        String id = response.id();
        TaskId taskId = findTaskWithXOpaqueId(opaqueId, EqlSearchAction.NAME + "[a]");
        assertThat(taskId, notNullValue());

        disableBlocks(plugins);

        assertBusy(() -> assertThat(findTaskWithXOpaqueId(opaqueId, EqlSearchAction.NAME + "[a]"), nullValue()));
        StoredAsyncResponse<EqlSearchResponse> doc = getStoredRecord(id);
        // Make sure that the expiration time is not more than 1 min different from the current time + keep alive
        assertThat(System.currentTimeMillis() + keepAliveValue.getMillis() - doc.getExpirationTime(),
            lessThan(doc.getExpirationTime() + TimeValue.timeValueMinutes(1).getMillis()));
        if (success) {
            assertThat(doc.getException(), nullValue());
            assertThat(doc.getResponse(), notNullValue());
            assertThat(doc.getResponse().hits().events().size(), equalTo(1));
        } else {
            assertThat(doc.getException(), notNullValue());
            assertThat(doc.getResponse(), nullValue());
            assertThat(doc.getException().getCause().getMessage(), containsString("by zero"));
        }
    }

    public void testAsyncCancellation() throws Exception {
        prepareIndex();

        boolean success = randomBoolean();
        String query = success ? "my_event where i=1" : "my_event where 10/i=1";
        EqlSearchRequest request = new EqlSearchRequest().indices("test").query(query).eventCategoryField("event_type")
            .waitForCompletionTimeout(TimeValue.timeValueMillis(1));

        boolean customKeepAlive = randomBoolean();
        final TimeValue keepAliveValue;
        if (customKeepAlive) {
            keepAliveValue = TimeValue.parseTimeValue(randomTimeValue(1, 5, "d"), "test");
            request.keepAlive(keepAliveValue);
        }

        List<SearchBlockPlugin> plugins = initBlockFactory(true, false);

        String opaqueId = randomAlphaOfLength(10);
        logger.trace("Starting async search");
        EqlSearchResponse response = client().filterWithHeader(Collections.singletonMap(Task.X_OPAQUE_ID, opaqueId))
            .execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.isRunning(), is(true));
        assertThat(response.isPartial(), is(true));
        assertThat(response.id(), notNullValue());

        logger.trace("Waiting for block to be established");
        awaitForBlockedSearches(plugins, "test");
        logger.trace("Block is established");

        ActionFuture<AcknowledgedResponse> deleteResponse =
            client().execute(DeleteAsyncResultAction.INSTANCE, new DeleteAsyncResultRequest(response.id()));
        disableBlocks(plugins);
        assertThat(deleteResponse.actionGet().isAcknowledged(), equalTo(true));

        deleteResponse = client().execute(DeleteAsyncResultAction.INSTANCE, new DeleteAsyncResultRequest(response.id()));
        assertFutureThrows(deleteResponse, ResourceNotFoundException.class);
    }

    public void testFinishingBeforeTimeout() throws Exception {
        prepareIndex();

        boolean success = randomBoolean();
        boolean keepOnCompletion = randomBoolean();
        String query = success ? "my_event where i=1" : "my_event where 10/i=1";
        EqlSearchRequest request = new EqlSearchRequest().indices("test").query(query).eventCategoryField("event_type")
            .waitForCompletionTimeout(TimeValue.timeValueSeconds(10));
        if (keepOnCompletion || randomBoolean()) {
            request.keepOnCompletion(keepOnCompletion);
        }

        if (success) {
            EqlSearchResponse response = client().execute(EqlSearchAction.INSTANCE, request).get();
            assertThat(response.isRunning(), is(false));
            assertThat(response.isPartial(), is(false));
            assertThat(response.id(), notNullValue());
            assertThat(response.hits().events().size(), equalTo(1));
            if (keepOnCompletion) {
                StoredAsyncResponse<EqlSearchResponse> doc = getStoredRecord(response.id());
                assertThat(doc, notNullValue());
                assertThat(doc.getException(), nullValue());
                assertThat(doc.getResponse(), notNullValue());
                assertThat(doc.getResponse().hits().events().size(), equalTo(1));
                EqlSearchResponse storedResponse = client().execute(EqlAsyncGetResultAction.INSTANCE,
                    new GetAsyncResultRequest(response.id())).actionGet();
                assertThat(storedResponse, equalTo(response));

                AcknowledgedResponse deleteResponse =
                    client().execute(DeleteAsyncResultAction.INSTANCE, new DeleteAsyncResultRequest(response.id())).actionGet();
                assertThat(deleteResponse.isAcknowledged(), equalTo(true));
            }
        } else {
            Exception ex = expectThrows(Exception.class,
                () -> client().execute(EqlSearchAction.INSTANCE, request).get());
            assertThat(ex.getMessage(), containsString("by zero"));
        }
    }

    public StoredAsyncResponse<EqlSearchResponse> getStoredRecord(String id) throws Exception {
        try {
            GetResponse doc = client().prepareGet(XPackPlugin.ASYNC_RESULTS_INDEX, AsyncExecutionId.decode(id).getDocId()).get();
            if (doc.isExists()) {
                String value = doc.getSource().get("result").toString();
                try (ByteBufferStreamInput buf = new ByteBufferStreamInput(ByteBuffer.wrap(Base64.getDecoder().decode(value)))) {
                    try (StreamInput in = new NamedWriteableAwareStreamInput(buf, registry)) {
                        in.setVersion(Version.readVersion(in));
                        return new StoredAsyncResponse<>(EqlSearchResponse::new, in);
                    }
                }
            }
            return null;
        } catch (IndexNotFoundException | NoShardAvailableActionException ex) {
            return null;
        }
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

    public static class FakePainlessScriptPlugin extends MockScriptPlugin {

        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();
            scripts.put("InternalQlScriptUtils.nullSafeFilter(InternalQlScriptUtils.eq(InternalQlScriptUtils.div(" +
                "params.v0,InternalQlScriptUtils.docValue(doc,params.v1)),params.v2))", FakePainlessScriptPlugin::fail);
            return scripts;
        }

        public static Object fail(Map<String, Object> arg) {
            throw new ArithmeticException("Division by zero");
        }

        public String pluginScriptLang() {
            // Faking painless
            return "painless";
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(FakePainlessScriptPlugin.class);
        return plugins;
    }

}
