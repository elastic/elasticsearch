/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.notification.email.attachment;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.core.watcher.watch.Payload;
import org.elasticsearch.xpack.watcher.common.http.HttpRequest;
import org.elasticsearch.xpack.watcher.common.http.HttpRequestTemplate;
import org.elasticsearch.xpack.watcher.common.http.HttpResponse;
import org.elasticsearch.xpack.watcher.common.text.TextTemplateEngine;
import org.elasticsearch.xpack.watcher.notification.WebhookService;
import org.elasticsearch.xpack.watcher.notification.email.Attachment;
import org.elasticsearch.xpack.watcher.support.Variables;

import java.io.IOException;
import java.util.Map;

public class HttpEmailAttachementParser implements EmailAttachmentParser<HttpRequestAttachment> {

    public interface Fields {
        ParseField INLINE = new ParseField("inline");
        ParseField REQUEST = new ParseField("request");
        ParseField CONTENT_TYPE = new ParseField("content_type");
    }

    public static final String TYPE = "http";
    private final WebhookService webhookService;
    private final TextTemplateEngine templateEngine;

    public HttpEmailAttachementParser(WebhookService webhookService, TextTemplateEngine templateEngine) {
        this.webhookService = webhookService;
        this.templateEngine = templateEngine;
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public HttpRequestAttachment parse(String id, XContentParser parser) throws IOException {
        boolean inline = false;
        String contentType = null;
        HttpRequestTemplate requestTemplate = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (Fields.CONTENT_TYPE.match(currentFieldName, parser.getDeprecationHandler())) {
                contentType = parser.text();
            } else if (Fields.INLINE.match(currentFieldName, parser.getDeprecationHandler())) {
                inline = parser.booleanValue();
            } else if (Fields.REQUEST.match(currentFieldName, parser.getDeprecationHandler())) {
                requestTemplate = HttpRequestTemplate.Parser.parse(parser);
            } else {
                String msg = "Unknown field name [" + currentFieldName + "] in http request attachment configuration";
                throw new ElasticsearchParseException(msg);
            }
        }

        if (requestTemplate != null) {
            return new HttpRequestAttachment(id, requestTemplate, inline, contentType);
        }

        throw new ElasticsearchParseException("Could not parse http request attachment");
    }

    @Override
    public Attachment toAttachment(WatchExecutionContext context, Payload payload, HttpRequestAttachment attachment) throws IOException {
        Map<String, Object> model = Variables.createCtxParamsMap(context, payload);
        HttpRequest httpRequest = attachment.getRequestTemplate().render(templateEngine, model);

        HttpResponse response = webhookService.modifyAndExecuteHttpRequest(httpRequest).v2();
        // check for status 200, only then append attachment
        if (response.status() >= 200 && response.status() < 300) {
            if (response.hasContent()) {
                String contentType = attachment.getContentType();
                String attachmentContentType = Strings.hasLength(contentType) ? contentType : response.contentType();
                return new Attachment.Bytes(
                    attachment.id(),
                    BytesReference.toBytes(response.body()),
                    attachmentContentType,
                    attachment.inline()
                );
            } else {
                throw new ElasticsearchException(
                    "Watch[{}] attachment[{}] HTTP empty response body host[{}], port[{}], " + "method[{}], path[{}], status[{}]",
                    context.watch().id(),
                    attachment.id(),
                    httpRequest.host(),
                    httpRequest.port(),
                    httpRequest.method(),
                    httpRequest.path(),
                    response.status()
                );
            }
        } else {
            throw new ElasticsearchException(
                "Watch[{}] attachment[{}] HTTP error status host[{}], port[{}], " + "method[{}], path[{}], status[{}]",
                context.watch().id(),
                attachment.id(),
                httpRequest.host(),
                httpRequest.port(),
                httpRequest.method(),
                httpRequest.path(),
                response.status()
            );
        }
    }
}
