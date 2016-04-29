/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.email;

import com.squareup.okhttp.mockwebserver.MockResponse;
import com.squareup.okhttp.mockwebserver.MockWebServer;
import com.squareup.okhttp.mockwebserver.QueueDispatcher;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.notification.email.DataAttachment;
import org.elasticsearch.xpack.notification.email.EmailTemplate;
import org.elasticsearch.xpack.notification.email.attachment.EmailAttachmentParser;
import org.elasticsearch.xpack.notification.email.attachment.EmailAttachments;
import org.elasticsearch.xpack.notification.email.attachment.HttpRequestAttachment;
import org.elasticsearch.xpack.notification.email.support.EmailServer;
import org.elasticsearch.watcher.client.WatchSourceBuilder;
import org.elasticsearch.watcher.client.WatcherClient;
import org.elasticsearch.watcher.condition.compare.CompareCondition;
import org.elasticsearch.watcher.support.http.HttpRequestTemplate;
import org.elasticsearch.watcher.support.http.Scheme;
import org.elasticsearch.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.watcher.trigger.schedule.IntervalSchedule;
import org.junit.After;
import org.junit.Before;

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
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.watcher.actions.ActionBuilders.emailAction;
import static org.elasticsearch.xpack.notification.email.DataAttachment.JSON;
import static org.elasticsearch.xpack.notification.email.DataAttachment.YAML;
import static org.elasticsearch.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.watcher.condition.ConditionBuilders.compareCondition;
import static org.elasticsearch.watcher.input.InputBuilders.searchInput;
import static org.elasticsearch.watcher.test.WatcherTestUtils.newInputSearchRequest;
import static org.elasticsearch.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.startsWith;

public class EmailAttachmentTests extends AbstractWatcherIntegrationTestCase {

    static final String USERNAME = "_user";
    static final String PASSWORD = "_passwd";

    private MockWebServer webServer = new MockWebServer();;
    private EmailServer server;

    @Before
    public void startWebservice() throws Exception {
        QueueDispatcher dispatcher = new QueueDispatcher();
        dispatcher.setFailFast(true);
        webServer.setDispatcher(dispatcher);
        webServer.start(0);
        MockResponse mockResponse = new MockResponse().setResponseCode(200)
                .addHeader("Content-Type", "application/foo").setBody("This is the content");
        webServer.enqueue(mockResponse);
    }

    @After
    public void cleanup() throws Exception {
        server.stop();
        webServer.shutdown();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        if(server == null) {
            //Need to construct the Email Server here as this happens before init()
            server = EmailServer.localhost("2500-2600", USERNAME, PASSWORD, logger);
        }
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("xpack.notification.email.account.test.smtp.auth", true)
                .put("xpack.notification.email.account.test.smtp.user", USERNAME)
                .put("xpack.notification.email.account.test.smtp.password", PASSWORD)
                .put("xpack.notification.email.account.test.smtp.port", server.port())
                .put("xpack.notification.email.account.test.smtp.host", "localhost")
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
        DataAttachment dataFormat = randomFrom(JSON, YAML);
        final CountDownLatch latch = new CountDownLatch(1);
        server.addListener(new EmailServer.Listener() {
            @Override
            public void on(MimeMessage message) throws Exception {
                assertThat(message.getSubject(), equalTo("Subject"));
                List<String> attachments = getAttachments(message);
                if (dataFormat == YAML) {
                    assertThat(attachments, hasItem(allOf(startsWith("---"), containsString("_test_id"))));
                } else {
                    assertThat(attachments, hasItem(allOf(startsWith("{"), containsString("_test_id"))));
                }
                assertThat(attachments, hasItem(containsString("This is the content")));
                latch.countDown();
            }
        });

        WatcherClient watcherClient = watcherClient();
        createIndex("idx");
        // Have a sample document in the index, the watch is going to evaluate
        client().prepareIndex("idx", "type").setSource("field", "value").get();
        refresh();
        SearchRequest searchRequest = newInputSearchRequest("idx").source(searchSource().query(matchAllQuery()));

        List<EmailAttachmentParser.EmailAttachment> attachments = new ArrayList<>();

        org.elasticsearch.xpack.notification.email.attachment.DataAttachment dataAttachment =
                org.elasticsearch.xpack.notification.email.attachment.DataAttachment.builder("my-id").dataAttachment(dataFormat).build();
        attachments.add(dataAttachment);

        HttpRequestTemplate requestTemplate = HttpRequestTemplate.builder("localhost", webServer.getPort())
                .path("/").scheme(Scheme.HTTP).build();
        HttpRequestAttachment httpRequestAttachment = HttpRequestAttachment.builder("other-id")
                .httpRequestTemplate(requestTemplate).build();

        attachments.add(httpRequestAttachment);
        EmailAttachments emailAttachments = new EmailAttachments(attachments);
        XContentBuilder tmpBuilder = jsonBuilder();
        emailAttachments.toXContent(tmpBuilder, ToXContent.EMPTY_PARAMS);
        logger.info("TMP BUILDER {}", tmpBuilder.string());

        EmailTemplate.Builder emailBuilder = EmailTemplate.builder().from("_from").to("_to").subject("Subject");
        WatchSourceBuilder watchSourceBuilder = watchBuilder()
                .trigger(schedule(interval(5, IntervalSchedule.Interval.Unit.SECONDS)))
                .input(searchInput(searchRequest))
                .condition(compareCondition("ctx.payload.hits.total", CompareCondition.Op.GT, 0L))
                .addAction("_email", emailAction(emailBuilder).setAuthentication(USERNAME, PASSWORD.toCharArray())
                .setAttachments(emailAttachments));
        logger.info("TMP WATCHSOURCE {}", watchSourceBuilder.build().getBytes().toUtf8());

        watcherClient.preparePutWatch("_test_id")
                .setSource(watchSourceBuilder)
                .get();

        if (timeWarped()) {
            timeWarp().scheduler().trigger("_test_id");
            refresh();
        }

        assertWatchWithMinimumPerformedActionsCount("_test_id", 1);

        if (!latch.await(5, TimeUnit.SECONDS)) {
            fail("waited too long for email to be received");
        }
    }



}
