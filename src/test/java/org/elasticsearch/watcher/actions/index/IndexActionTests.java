/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.index;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.watcher.actions.Action;
import org.elasticsearch.watcher.actions.Action.Result.Status;
import org.elasticsearch.watcher.actions.email.service.Authentication;
import org.elasticsearch.watcher.actions.email.service.Email;
import org.elasticsearch.watcher.actions.email.service.EmailService;
import org.elasticsearch.watcher.actions.email.service.Profile;
import org.elasticsearch.watcher.execution.TriggeredExecutionContext;
import org.elasticsearch.watcher.execution.WatchExecutionContext;
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

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

/**
 */
public class IndexActionTests extends ElasticsearchIntegrationTest {

    @Test
    public void testIndexActionExecute() throws Exception {

        IndexAction action = new IndexAction("test-index", "test-type");
        ExecutableIndexAction executable = new ExecutableIndexAction(action, logger, ClientProxy.of(client()));
        final String account = "account1";
        Watch watch = WatcherTestUtils.createTestWatch("test_watch",
                ClientProxy.of(client()),
                ScriptServiceProxy.of(internalCluster().getInstance(ScriptService.class)),
                new HttpClient(ImmutableSettings.EMPTY, mock(HttpAuthRegistry.class)).start(),
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
        WatchExecutionContext ctx = new TriggeredExecutionContext(watch, new DateTime(), new ScheduleTriggerEvent(watch.id(), new DateTime(), new DateTime()), TimeValue.timeValueSeconds(5));

        Map<String, Object> payloadMap = new HashMap<>();
        payloadMap.put("test", "foo");
        Action.Result result = executable.execute("_id", ctx, new Payload.Simple(payloadMap));

        assertThat(result.status(), equalTo(Status.SUCCESS));
        assertThat(result, instanceOf(IndexAction.Result.Success.class));
        IndexAction.Result.Success successResult = (IndexAction.Result.Success) result;
        Map<String, Object> responseData = successResult.response().data();
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

}
