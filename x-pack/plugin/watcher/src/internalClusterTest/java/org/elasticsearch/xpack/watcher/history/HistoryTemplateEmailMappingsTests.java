/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.history;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.protocol.xpack.watcher.PutWatchResponse;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.xpack.core.watcher.execution.ExecutionState;
import org.elasticsearch.xpack.core.watcher.history.HistoryStoreField;
import org.elasticsearch.xpack.core.watcher.transport.actions.put.PutWatchRequestBuilder;
import org.elasticsearch.xpack.watcher.condition.InternalAlwaysCondition;
import org.elasticsearch.xpack.watcher.notification.email.EmailTemplate;
import org.elasticsearch.xpack.watcher.notification.email.support.EmailServer;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;
import org.junit.After;

import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.emailAction;
import static org.elasticsearch.xpack.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.simpleInput;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * This test makes sure that the email address fields in the watch_record action result are
 * not analyzed so they can be used in aggregations
 */
public class HistoryTemplateEmailMappingsTests extends AbstractWatcherIntegrationTestCase {

    private EmailServer server;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        server = EmailServer.localhost(logger);
    }

    @After
    public void cleanup() throws Exception {
        server.stop();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.notification.email.account.test.smtp.secure_password", EmailServer.PASSWORD);
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))

            // email
            .put("xpack.notification.email.account.test.smtp.auth", true)
            .put("xpack.notification.email.account.test.smtp.user", EmailServer.USERNAME)
            .put("xpack.notification.email.account.test.smtp.port", server.port())
            .put("xpack.notification.email.account.test.smtp.host", "localhost")
            .setSecureSettings(secureSettings)
            .build();
    }

    public void testEmailFields() throws Exception {
        PutWatchResponse putWatchResponse = new PutWatchRequestBuilder(client(), "_id").setSource(
            watchBuilder().trigger(schedule(interval("5s")))
                .input(simpleInput())
                .condition(InternalAlwaysCondition.INSTANCE)
                .addAction(
                    "_email",
                    emailAction(
                        EmailTemplate.builder()
                            .from("from@example.com")
                            .to("to1@example.com", "to2@example.com")
                            .cc("cc1@example.com", "cc2@example.com")
                            .bcc("bcc1@example.com", "bcc2@example.com")
                            .replyTo("rt1@example.com", "rt2@example.com")
                            .subject("_subject")
                            .textBody("_body")
                    )
                )
        ).get();

        assertThat(putWatchResponse.isCreated(), is(true));
        timeWarp().trigger("_id");
        flush();
        refresh();

        // the action should fail as no email server is available
        assertWatchWithMinimumActionsCount("_id", ExecutionState.EXECUTED, 1);

        SearchResponse response = client().prepareSearch(HistoryStoreField.DATA_STREAM + "*")
            .setSource(
                searchSource().aggregation(terms("from").field("result.actions.email.message.from"))
                    .aggregation(terms("to").field("result.actions.email.message.to"))
                    .aggregation(terms("cc").field("result.actions.email.message.cc"))
                    .aggregation(terms("bcc").field("result.actions.email.message.bcc"))
                    .aggregation(terms("reply_to").field("result.actions.email.message.reply_to"))
            )
            .get();

        assertThat(response, notNullValue());
        assertThat(response.getHits().getTotalHits().value, greaterThanOrEqualTo(1L));
        Aggregations aggs = response.getAggregations();
        assertThat(aggs, notNullValue());

        Terms terms = aggs.get("from");
        assertThat(terms, notNullValue());
        assertThat(terms.getBuckets().size(), is(1));
        assertThat(terms.getBucketByKey("from@example.com"), notNullValue());
        assertThat(terms.getBucketByKey("from@example.com").getDocCount(), greaterThanOrEqualTo(1L));

        terms = aggs.get("to");
        assertThat(terms, notNullValue());
        assertThat(terms.getBuckets().size(), is(2));
        assertThat(terms.getBucketByKey("to1@example.com"), notNullValue());
        assertThat(terms.getBucketByKey("to1@example.com").getDocCount(), greaterThanOrEqualTo(1L));
        assertThat(terms.getBucketByKey("to2@example.com"), notNullValue());
        assertThat(terms.getBucketByKey("to2@example.com").getDocCount(), greaterThanOrEqualTo(1L));

        terms = aggs.get("cc");
        assertThat(terms, notNullValue());
        assertThat(terms.getBuckets().size(), is(2));
        assertThat(terms.getBucketByKey("cc1@example.com"), notNullValue());
        assertThat(terms.getBucketByKey("cc1@example.com").getDocCount(), greaterThanOrEqualTo(1L));
        assertThat(terms.getBucketByKey("cc2@example.com"), notNullValue());
        assertThat(terms.getBucketByKey("cc2@example.com").getDocCount(), greaterThanOrEqualTo(1L));

        terms = aggs.get("bcc");
        assertThat(terms, notNullValue());
        assertThat(terms.getBuckets().size(), is(2));
        assertThat(terms.getBucketByKey("bcc1@example.com"), notNullValue());
        assertThat(terms.getBucketByKey("bcc1@example.com").getDocCount(), greaterThanOrEqualTo(1L));
        assertThat(terms.getBucketByKey("bcc2@example.com"), notNullValue());
        assertThat(terms.getBucketByKey("bcc2@example.com").getDocCount(), greaterThanOrEqualTo(1L));

        terms = aggs.get("reply_to");
        assertThat(terms, notNullValue());
        assertThat(terms.getBuckets().size(), is(2));
        assertThat(terms.getBucketByKey("rt1@example.com"), notNullValue());
        assertThat(terms.getBucketByKey("rt1@example.com").getDocCount(), greaterThanOrEqualTo(1L));
        assertThat(terms.getBucketByKey("rt2@example.com"), notNullValue());
        assertThat(terms.getBucketByKey("rt2@example.com").getDocCount(), greaterThanOrEqualTo(1L));
    }
}
