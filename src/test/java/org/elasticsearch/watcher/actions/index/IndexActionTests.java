/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.index;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.watcher.actions.email.service.Authentication;
import org.elasticsearch.watcher.actions.email.service.Email;
import org.elasticsearch.watcher.actions.email.service.EmailService;
import org.elasticsearch.watcher.actions.email.service.Profile;
import org.elasticsearch.watcher.execution.TriggeredExecutionContext;
import org.elasticsearch.watcher.execution.WatchExecutionContext;
import org.elasticsearch.watcher.execution.Wid;
import org.elasticsearch.watcher.support.http.HttpClient;
import org.elasticsearch.watcher.support.http.auth.HttpAuthRegistry;
import org.elasticsearch.watcher.support.init.proxy.ClientProxy;
import org.elasticsearch.watcher.support.init.proxy.ScriptServiceProxy;
import org.elasticsearch.watcher.test.WatcherTestUtils;
import org.elasticsearch.watcher.trigger.schedule.ScheduleTriggerEvent;
import org.elasticsearch.watcher.watch.Payload;
import org.elasticsearch.watcher.watch.Watch;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.joda.time.DateTimeZone.UTC;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.mock;

/**
 */
public class IndexActionTests extends ElasticsearchIntegrationTest {

    @Test
    public void testIndexActionExecute() throws Exception {

        IndexAction action = new IndexAction("test-index", "test-type");
        ExecutableIndexAction executable = new ExecutableIndexAction(action, logger, ClientProxy.of(client()));
        final String account = "account1";
        Watch watch = WatcherTestUtils.createTestWatch("testAlert",
                ClientProxy.of(client()),
                ScriptServiceProxy.of(internalCluster().getInstance(ScriptService.class)),
                new HttpClient(ImmutableSettings.EMPTY, mock(HttpAuthRegistry.class)),
                new EmailService() {
                    @Override
                    public EmailService.EmailSent send(Email email, Authentication auth, Profile profile) {
                        return new EmailSent(account, email);
                    }

                    @Override
                    public EmailSent send(Email email, Authentication auth, Profile profile, String accountName) {
                        return new EmailSent(account, email);
                    }
                },
                logger);
        WatchExecutionContext ctx = new TriggeredExecutionContext(watch, new DateTime(), new ScheduleTriggerEvent(watch.id(), new DateTime(), new DateTime()));

        Map<String, Object> payloadMap = new HashMap<>();
        payloadMap.put("test", "foo");
        IndexAction.Result.Executed result = (IndexAction.Result.Executed) executable.execute("_id", ctx, new Payload.Simple(payloadMap));

        assertThat(result.success(), equalTo(true));
        Map<String, Object> responseData = result.response().data();
        assertThat(responseData.get("created"), equalTo((Object)Boolean.TRUE));
        assertThat(responseData.get("version"), equalTo((Object) 1L));
        assertThat(responseData.get("type").toString(), equalTo("test-type"));
        assertThat(responseData.get("index").toString(), equalTo("test-index"));

        refresh(); //Manually refresh to make sure data is available

        SearchResponse sr = client().prepareSearch("test-index")
                .setTypes("test-type")
                .setSource(searchSource().query(matchAllQuery()).buildAsBytes()).get();

        assertThat(sr.getHits().totalHits(), equalTo(1L));
    }

    @Test @Repeat(iterations = 10)
    public void testParser() throws Exception {
        XContentBuilder builder = jsonBuilder();
        builder.startObject()
                .field(IndexAction.Field.INDEX.getPreferredName(), "test-index")
                .field(IndexAction.Field.DOC_TYPE.getPreferredName(), "test-type")
                .endObject();

        IndexActionFactory actionParser = new IndexActionFactory(ImmutableSettings.EMPTY, ClientProxy.of(client()));
        XContentParser parser = JsonXContent.jsonXContent.createParser(builder.bytes());
        parser.nextToken();

        ExecutableIndexAction executable = actionParser.parseExecutable(randomAsciiOfLength(5), randomAsciiOfLength(3), parser);

        assertThat(executable.action().docType, equalTo("test-type"));
        assertThat(executable.action().index, equalTo("test-index"));
    }

    @Test @Repeat(iterations = 10)
    public void testParser_Failure() throws Exception {
        XContentBuilder builder = jsonBuilder();
        boolean useIndex = randomBoolean();
        boolean useType = randomBoolean();
        builder.startObject();
        {
            if (useIndex) {
                builder.field(IndexAction.Field.INDEX.getPreferredName(), "test-index");
            }
            if (useType) {
                builder.field(IndexAction.Field.DOC_TYPE.getPreferredName(), "test-type");
            }
        }
        builder.endObject();
        IndexActionFactory actionParser = new IndexActionFactory(ImmutableSettings.EMPTY, ClientProxy.of(client()));
        XContentParser parser = JsonXContent.jsonXContent.createParser(builder.bytes());
        parser.nextToken();
        try {
            actionParser.parseExecutable(randomAsciiOfLength(4), randomAsciiOfLength(5), parser);
            if (!(useIndex && useType)) {
                fail();
            }
        } catch (IndexActionException iae) {
            assertThat(useIndex && useType, equalTo(false));
        }
    }

    @Test @Repeat(iterations = 30)
    public void testParser_Result() throws Exception {
        boolean success = randomBoolean();
        boolean simulated = randomBoolean();

        XContentBuilder builder = jsonBuilder().startObject()
                .field("success", success);

        Payload.Simple source = null;
        String index = randomAsciiOfLength(5);
        String docType = randomAsciiOfLength(5);

        if (simulated) {
            source = new Payload.Simple(ImmutableMap.<String, Object>builder()
                    .put("data", new HashMap<String, Object>())
                    .put("timestamp", DateTime.now(UTC).toString())
                    .build());
            builder.startObject(IndexAction.Field.SIMULATED_REQUEST.getPreferredName())
                    .field(IndexAction.Field.INDEX.getPreferredName(), index)
                    .field(IndexAction.Field.DOC_TYPE.getPreferredName(), docType)
                    .field(IndexAction.Field.SOURCE.getPreferredName(), source)
                .endObject();

        } else if (success) {
            Map<String,Object> data = new HashMap<>();
            data.put("created", true);
            data.put("id", "0");
            data.put("version", 1);
            data.put("type", "test-type");
            data.put("index", "test-index");
            builder.field(IndexAction.Field.RESPONSE.getPreferredName(), data);

        } else {
            builder.field("reason", "_reason");
        }

        Wid wid = new Wid(randomAsciiOfLength(4), randomLong(), DateTime.now());
        String actionId = randomAsciiOfLength(5);

        XContentParser parser = JsonXContent.jsonXContent.createParser(builder.bytes());
        parser.nextToken();
        IndexAction.Result result = new IndexActionFactory(ImmutableSettings.EMPTY, ClientProxy.of(client()))
                .parseResult(wid, actionId, parser);

        if (simulated){
            assertThat(result, instanceOf(IndexAction.Result.Simulated.class));
            assertThat(((IndexAction.Result.Simulated) result).source(), equalTo((Payload) source));
            assertThat(((IndexAction.Result.Simulated) result).index(), equalTo(index));
            assertThat(((IndexAction.Result.Simulated) result).docType(), equalTo(docType));

        } else {
            assertThat(result.success(), is(success));
            if (success) {
                assertThat(result, instanceOf(IndexAction.Result.Executed.class));
                Map<String, Object> responseData = ((IndexAction.Result.Executed) result).response().data();
                assertThat(responseData.get("created"), equalTo((Object) Boolean.TRUE));
                assertThat(responseData.get("version"), equalTo((Object) 1));
                assertThat(responseData.get("type").toString(), equalTo("test-type"));
                assertThat(responseData.get("index").toString(), equalTo("test-index"));
            } else {
                assertThat(result, instanceOf(IndexAction.Result.Failure.class));
                assertThat(((IndexAction.Result.Failure) result).reason(), is("_reason"));
            }
        }
    }

    @Test
    public void testParser_Result_Simulated_SelfGenerated() throws Exception {
        IndexRequest request = new IndexRequest("testindex").type("testtype");
        XContentBuilder resultBuilder = XContentFactory.jsonBuilder().prettyPrint();
        resultBuilder.startObject();
        resultBuilder.field("data", new HashMap<String, Object>());
        resultBuilder.field("timestamp", new DateTime(UTC));
        resultBuilder.endObject();
        request.source(resultBuilder);
        Payload.Simple requestPayload = new Payload.Simple(request.sourceAsMap());

        String index = randomAsciiOfLength(4);
        String docType = randomAsciiOfLength(5);
        IndexAction.Result.Simulated simulatedResult = new IndexAction.Result.Simulated(index, docType, requestPayload);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        simulatedResult.toXContent(builder, ToXContent.EMPTY_PARAMS);

        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken();

        Wid wid = new Wid(randomAsciiOfLength(4), randomLong(), DateTime.now());
        String actionId = randomAsciiOfLength(5);

        IndexAction.Result result = new IndexActionFactory(ImmutableSettings.EMPTY, ClientProxy.of(client()))
                .parseResult(wid, actionId, parser);

        assertThat(result, instanceOf(IndexAction.Result.Simulated.class));
        assertThat(((IndexAction.Result.Simulated) result).source(), equalTo((Payload) requestPayload));
        assertThat(((IndexAction.Result.Simulated) result).index(), equalTo(index));
        assertThat(((IndexAction.Result.Simulated) result).docType(), equalTo(docType));
    }

}
