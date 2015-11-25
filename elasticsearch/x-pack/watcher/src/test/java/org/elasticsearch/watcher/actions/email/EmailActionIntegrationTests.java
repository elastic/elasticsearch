/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.email;

import org.apache.lucene.util.LuceneTestCase.AwaitsFix;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.watcher.actions.email.service.EmailTemplate;
import org.elasticsearch.watcher.actions.email.service.support.EmailServer;
import org.elasticsearch.watcher.client.WatcherClient;
import org.elasticsearch.watcher.condition.compare.CompareCondition;
import org.elasticsearch.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.watcher.trigger.schedule.IntervalSchedule;
import org.junit.After;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.mail.internet.MimeMessage;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.elasticsearch.watcher.actions.ActionBuilders.emailAction;
import static org.elasticsearch.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.watcher.condition.ConditionBuilders.compareCondition;
import static org.elasticsearch.watcher.input.InputBuilders.searchInput;
import static org.elasticsearch.watcher.test.WatcherTestUtils.newInputSearchRequest;
import static org.elasticsearch.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.equalTo;

@TestLogging("subethamail:TRACE,watcher:TRACE")
@ESIntegTestCase.ClusterScope(scope = SUITE, numClientNodes = 0, transportClientRatio = 0, randomDynamicTemplates = false, numDataNodes = 1)
public class EmailActionIntegrationTests extends AbstractWatcherIntegrationTestCase {
    static final String USERNAME = "_user";
    static final String PASSWORD = "_passwd";

    private EmailServer server;

    @After
    public void cleanup() throws Exception {
        server.stop();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        if(server == null) {
            //Need to construct the Email Server here as this happens before init()
            server = EmailServer.localhost("2500-2600", USERNAME, PASSWORD, logger);
        }
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("watcher.actions.email.service.account.test.smtp.auth", true)
                .put("watcher.actions.email.service.account.test.smtp.user", USERNAME)
                .put("watcher.actions.email.service.account.test.smtp.password", PASSWORD)
                .put("watcher.actions.email.service.account.test.smtp.port", server.port())
                .put("watcher.actions.email.service.account.test.smtp.host", "localhost")
                .build();
    }

    public void testArrayAccess() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        server.addListener(new EmailServer.Listener() {
            @Override
            public void on(MimeMessage message) throws Exception {
                assertThat(message.getSubject(), equalTo("value"));
                latch.countDown();
            }
        });

        WatcherClient watcherClient = watcherClient();
        createIndex("idx");
        // Have a sample document in the index, the watch is going to evaluate
        client().prepareIndex("idx", "type").setSource("field", "value").get();
        refresh();
        SearchRequest searchRequest = newInputSearchRequest("idx").source(searchSource().query(termQuery("field", "value")));
        watcherClient.preparePutWatch("_id")
                .setSource(watchBuilder()
                        .trigger(schedule(interval(5, IntervalSchedule.Interval.Unit.SECONDS)))
                        .input(searchInput(searchRequest))
                        .condition(compareCondition("ctx.payload.hits.total", CompareCondition.Op.GT, 0l))
                        .addAction("_email", emailAction(EmailTemplate.builder().from("_from").to("_to")
                                .subject("{{ctx.payload.hits.hits.0._source.field}}")).setAuthentication(USERNAME, PASSWORD.toCharArray())))
                        .get();

        if (timeWarped()) {
            timeWarp().scheduler().trigger("_id");
            refresh();
        }

        assertWatchWithMinimumPerformedActionsCount("_id", 1);

        if (!latch.await(5, TimeUnit.SECONDS)) {
            fail("waited too long for email to be received");
        }
    }
}
