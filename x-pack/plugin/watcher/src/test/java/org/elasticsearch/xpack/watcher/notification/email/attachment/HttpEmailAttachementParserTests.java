/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.notification.email.attachment;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.core.watcher.execution.Wid;
import org.elasticsearch.xpack.core.watcher.watch.Payload;
import org.elasticsearch.xpack.watcher.common.http.HttpClient;
import org.elasticsearch.xpack.watcher.common.http.HttpRequest;
import org.elasticsearch.xpack.watcher.common.http.HttpRequestTemplate;
import org.elasticsearch.xpack.watcher.common.http.HttpResponse;
import org.elasticsearch.xpack.watcher.notification.WebhookService;
import org.elasticsearch.xpack.watcher.notification.email.attachment.EmailAttachmentParser.EmailAttachment;
import org.elasticsearch.xpack.watcher.test.MockTextTemplateEngine;
import org.junit.Before;

import java.io.IOException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.watcher.notification.email.attachment.ReportingAttachmentParser.INTERVAL_SETTING;
import static org.elasticsearch.xpack.watcher.notification.email.attachment.ReportingAttachmentParser.REPORT_WARNING_ENABLED_SETTING;
import static org.elasticsearch.xpack.watcher.notification.email.attachment.ReportingAttachmentParser.REPORT_WARNING_TEXT;
import static org.elasticsearch.xpack.watcher.notification.email.attachment.ReportingAttachmentParser.RETRIES_SETTING;
import static org.elasticsearch.xpack.watcher.test.WatcherTestUtils.mockExecutionContextBuilder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.Is.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HttpEmailAttachementParserTests extends ESTestCase {

    private HttpClient httpClient;
    private EmailAttachmentsParser emailAttachmentsParser;
    private Map<String, EmailAttachmentParser<? extends EmailAttachment>> attachmentParsers;

    @Before
    public void init() throws Exception {
        httpClient = mock(HttpClient.class);

        attachmentParsers = new HashMap<>();
        attachmentParsers.put(
            HttpEmailAttachementParser.TYPE,
            new HttpEmailAttachementParser(
                new WebhookService(Settings.EMPTY, httpClient, mockClusterService().getClusterSettings()),
                new MockTextTemplateEngine()
            )
        );
        emailAttachmentsParser = new EmailAttachmentsParser(attachmentParsers);
    }

    public void testSerializationWorks() throws Exception {
        HttpResponse response = new HttpResponse(200, "This is my response".getBytes(UTF_8));
        when(httpClient.execute(any(HttpRequest.class))).thenReturn(response);

        String id = "some-id";
        XContentBuilder builder = jsonBuilder().startObject()
            .startObject(id)
            .startObject(HttpEmailAttachementParser.TYPE)
            .startObject("request")
            .field("scheme", "http")
            .field("host", "test.de")
            .field("port", 80)
            .field("method", "get")
            .field("path", "/foo")
            .startObject("params")
            .endObject()
            .startObject("headers")
            .endObject()
            .endObject();

        boolean configureContentType = randomBoolean();
        if (configureContentType) {
            builder.field("content_type", "application/foo");
        }
        boolean isInline = randomBoolean();
        if (isInline) {
            builder.field("inline", true);
        }
        builder.endObject().endObject().endObject();
        XContentParser parser = createParser(builder);

        EmailAttachments emailAttachments = emailAttachmentsParser.parse(parser);
        assertThat(emailAttachments.getAttachments(), hasSize(1));

        XContentBuilder toXcontentBuilder = jsonBuilder().startObject();
        List<EmailAttachment> attachments = new ArrayList<>(emailAttachments.getAttachments());
        attachments.get(0).toXContent(toXcontentBuilder, ToXContent.EMPTY_PARAMS);
        toXcontentBuilder.endObject();
        assertThat(Strings.toString(toXcontentBuilder), is(Strings.toString(builder)));

        assertThat(attachments.get(0).inline(), is(isInline));
    }

    public void testNonOkHttpCodeThrowsException() throws Exception {
        HttpResponse response = new HttpResponse(403, "This is my response".getBytes(UTF_8));
        when(httpClient.execute(any(HttpRequest.class))).thenReturn(response);

        HttpRequestTemplate requestTemplate = HttpRequestTemplate.builder("localhost", 80).path("foo").build();
        HttpRequestAttachment attachment = new HttpRequestAttachment("someid", requestTemplate, false, null);
        WatchExecutionContext ctx = createWatchExecutionContext();

        ElasticsearchException exception = expectThrows(ElasticsearchException.class, () -> {
            @SuppressWarnings("unchecked")
            EmailAttachmentParser<HttpRequestAttachment> parser = (EmailAttachmentParser<HttpRequestAttachment>) attachmentParsers.get(
                HttpEmailAttachementParser.TYPE
            );
            parser.toAttachment(ctx, new Payload.Simple(), attachment);
        });
        assertThat(
            exception.getMessage(),
            is("Watch[watch1] attachment[someid] HTTP error status host[localhost], port[80], " + "method[GET], path[foo], status[403]")
        );
    }

    public void testEmptyResponseThrowsException() throws Exception {
        HttpResponse response = new HttpResponse(200);
        when(httpClient.execute(any(HttpRequest.class))).thenReturn(response);

        HttpRequestTemplate requestTemplate = HttpRequestTemplate.builder("localhost", 80).path("foo").build();
        HttpRequestAttachment attachment = new HttpRequestAttachment("someid", requestTemplate, false, null);
        WatchExecutionContext ctx = createWatchExecutionContext();

        ElasticsearchException exception = expectThrows(ElasticsearchException.class, () -> {
            @SuppressWarnings("unchecked")
            EmailAttachmentParser<HttpRequestAttachment> parser = (EmailAttachmentParser<HttpRequestAttachment>) attachmentParsers.get(
                HttpEmailAttachementParser.TYPE
            );
            parser.toAttachment(ctx, new Payload.Simple(), attachment);
        });
        assertThat(
            exception.getMessage(),
            is(
                "Watch[watch1] attachment[someid] HTTP empty response body host[localhost], port[80], "
                    + "method[GET], path[foo], status[200]"
            )
        );
    }

    public void testHttpClientThrowsException() throws Exception {
        when(httpClient.execute(any(HttpRequest.class))).thenThrow(new IOException("whatever"));

        HttpRequestTemplate requestTemplate = HttpRequestTemplate.builder("localhost", 80).path("foo").build();
        HttpRequestAttachment attachment = new HttpRequestAttachment("someid", requestTemplate, false, null);
        WatchExecutionContext ctx = createWatchExecutionContext();

        IOException exception = expectThrows(IOException.class, () -> {
            @SuppressWarnings("unchecked")
            EmailAttachmentParser<HttpRequestAttachment> parser = (EmailAttachmentParser<HttpRequestAttachment>) attachmentParsers.get(
                HttpEmailAttachementParser.TYPE
            );
            parser.toAttachment(ctx, new Payload.Simple(), attachment);
        });
        assertThat(exception.getMessage(), is("whatever"));
    }

    private WatchExecutionContext createWatchExecutionContext() {
        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
        Wid wid = new Wid(randomAlphaOfLength(5), now);
        Map<String, Object> metadata = Map.of("_key", "_val");
        return mockExecutionContextBuilder("watch1").wid(wid)
            .payload(new Payload.Simple())
            .time("watch1", now)
            .metadata(metadata)
            .buildMock();
    }

    private ClusterService mockClusterService() {
        ClusterService clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Set.of(INTERVAL_SETTING, RETRIES_SETTING, REPORT_WARNING_ENABLED_SETTING, REPORT_WARNING_TEXT)
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        return clusterService;
    }
}
