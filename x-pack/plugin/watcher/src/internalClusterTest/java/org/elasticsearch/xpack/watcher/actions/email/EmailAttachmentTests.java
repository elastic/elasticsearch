/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.actions.email;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.xpack.core.watcher.client.WatchSourceBuilder;
import org.elasticsearch.xpack.core.watcher.history.HistoryStoreField;
import org.elasticsearch.xpack.core.watcher.transport.actions.put.PutWatchRequestBuilder;
import org.elasticsearch.xpack.watcher.common.http.HttpRequestTemplate;
import org.elasticsearch.xpack.watcher.common.http.Scheme;
import org.elasticsearch.xpack.watcher.condition.InternalAlwaysCondition;
import org.elasticsearch.xpack.watcher.notification.email.EmailTemplate;
import org.elasticsearch.xpack.watcher.notification.email.attachment.DataAttachment;
import org.elasticsearch.xpack.watcher.notification.email.attachment.EmailAttachmentParser;
import org.elasticsearch.xpack.watcher.notification.email.attachment.EmailAttachments;
import org.elasticsearch.xpack.watcher.notification.email.attachment.HttpRequestAttachment;
import org.elasticsearch.xpack.watcher.notification.email.support.EmailServer;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.xpack.watcher.trigger.schedule.IntervalSchedule;
import org.junit.After;

import javax.mail.BodyPart;
import javax.mail.Multipart;
import javax.mail.Part;
import javax.mail.internet.MimeMessage;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.emailAction;
import static org.elasticsearch.xpack.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.noneInput;
import static org.elasticsearch.xpack.watcher.notification.email.DataAttachment.JSON;
import static org.elasticsearch.xpack.watcher.notification.email.DataAttachment.YAML;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.startsWith;

public class EmailAttachmentTests extends AbstractWatcherIntegrationTestCase {

    private MockWebServer webServer = new MockWebServer();
    private MockResponse mockResponse = new MockResponse().setResponseCode(200)
            .addHeader("Content-Type", "application/foo").setBody("This is the content");
    private EmailServer server;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        webServer.enqueue(mockResponse);
        webServer.start();

        server = EmailServer.localhost(logger);
    }

    @After
    public void cleanup() throws Exception {
        server.stop();
        webServer.close();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.notification.email.account.test.smtp.secure_password", EmailServer.PASSWORD);
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal, otherSettings))
                .put("xpack.notification.email.account.test.smtp.auth", true)
                .put("xpack.notification.email.account.test.smtp.user", EmailServer.USERNAME)
                .put("xpack.notification.email.account.test.smtp.port", server.port())
                .put("xpack.notification.email.account.test.smtp.host", "localhost")
                .setSecureSettings(secureSettings)
                .build();
    }

    public List<String> getAttachments(MimeMessage message) throws Exception {
        Object content = message.getContent();
        if (content instanceof String)
            return null;

        if (content instanceof Multipart) {
            Multipart multipart = (Multipart) content;
            List<String> result = new ArrayList<>();

            for (int i = 0; i < multipart.getCount(); i++) {
                result.addAll(getAttachments(multipart.getBodyPart(i)));
            }
            return result;

        }
        return null;
    }

    private List<String> getAttachments(BodyPart part) throws Exception {
        List<String> result = new ArrayList<>();
        Object content = part.getContent();
        if (content instanceof InputStream || content instanceof String) {
            if (Part.ATTACHMENT.equalsIgnoreCase(part.getDisposition()) || Strings.hasLength(part.getFileName())) {
                result.add(Streams.copyToString(new InputStreamReader(part.getInputStream(), StandardCharsets.UTF_8)));
                return result;
            } else {
                return new ArrayList<>();
            }
        }

        if (content instanceof Multipart) {
            Multipart multipart = (Multipart) content;
            for (int i = 0; i < multipart.getCount(); i++) {
                BodyPart bodyPart = multipart.getBodyPart(i);
                result.addAll(getAttachments(bodyPart));
            }
        }
        return result;
    }

    public void testThatEmailAttachmentsAreSent() throws Exception {
        org.elasticsearch.xpack.watcher.notification.email.DataAttachment dataFormat = randomFrom(JSON, YAML);
        final CountDownLatch latch = new CountDownLatch(1);
        server.addListener(message -> {
            assertThat(message.getSubject(), equalTo("Subject"));
            List<String> attachments = getAttachments(message);
            if (dataFormat == YAML) {
                assertThat(attachments, hasItem(allOf(startsWith("---"), containsString("_test_id"))));
            } else {
                assertThat(attachments, hasItem(allOf(startsWith("{"), containsString("_test_id"))));
            }
            assertThat(attachments, hasItem(containsString("This is the content")));
            latch.countDown();
        });

        createIndex("idx");
        // Have a sample document in the index, the watch is going to evaluate
        client().prepareIndex("idx").setSource("field", "value").get();
        refresh();

        List<EmailAttachmentParser.EmailAttachment> attachments = new ArrayList<>();

        DataAttachment dataAttachment = DataAttachment.builder("my-id").dataAttachment(dataFormat).build();
        attachments.add(dataAttachment);

        HttpRequestTemplate requestTemplate = HttpRequestTemplate.builder("localhost", webServer.getPort())
                .path("/").scheme(Scheme.HTTP).build();
        HttpRequestAttachment httpRequestAttachment = HttpRequestAttachment.builder("other-id")
                .httpRequestTemplate(requestTemplate).build();

        attachments.add(httpRequestAttachment);
        EmailAttachments emailAttachments = new EmailAttachments(attachments);
        XContentBuilder tmpBuilder = jsonBuilder();
        tmpBuilder.startObject();
        emailAttachments.toXContent(tmpBuilder, ToXContent.EMPTY_PARAMS);
        tmpBuilder.endObject();

        EmailTemplate.Builder emailBuilder = EmailTemplate.builder().from("from@example.org").to("to@example.org").subject("Subject");
        WatchSourceBuilder watchSourceBuilder = watchBuilder()
                .trigger(schedule(interval(5, IntervalSchedule.Interval.Unit.SECONDS)))
                .input(noneInput())
                .condition(InternalAlwaysCondition.INSTANCE)
                .addAction("_email", emailAction(emailBuilder).setAuthentication(EmailServer.USERNAME, EmailServer.PASSWORD.toCharArray())
                .setAttachments(emailAttachments));

        new PutWatchRequestBuilder(client())
                .setId("_test_id")
                .setSource(watchSourceBuilder)
                .get();

        timeWarp().trigger("_test_id");
        refresh();

        SearchResponse searchResponse = client().prepareSearch(HistoryStoreField.DATA_STREAM + "*")
                .setQuery(QueryBuilders.termQuery("watch_id", "_test_id"))
                .execute().actionGet();
        assertHitCount(searchResponse, 1);

        if (latch.await(5, TimeUnit.SECONDS) == false) {
            fail("waited too long for email to be received");
        }
    }
}
