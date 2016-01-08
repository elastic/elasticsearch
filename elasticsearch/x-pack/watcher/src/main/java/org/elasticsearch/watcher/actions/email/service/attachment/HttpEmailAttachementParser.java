/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.email.service.attachment;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.actions.email.service.Attachment;
import org.elasticsearch.watcher.execution.WatchExecutionContext;
import org.elasticsearch.watcher.support.Variables;
import org.elasticsearch.watcher.support.http.HttpClient;
import org.elasticsearch.watcher.support.http.HttpRequest;
import org.elasticsearch.watcher.support.http.HttpRequestTemplate;
import org.elasticsearch.watcher.support.http.HttpResponse;
import org.elasticsearch.watcher.support.text.TextTemplateEngine;
import org.elasticsearch.watcher.watch.Payload;

import java.io.IOException;
import java.util.Map;

public class HttpEmailAttachementParser implements EmailAttachmentParser<HttpRequestAttachment> {

    public interface Fields {
        ParseField REQUEST = new ParseField("request");
        ParseField CONTENT_TYPE = new ParseField("content_type");
    }

    public static final String TYPE = "http";
    private final HttpClient httpClient;
    private HttpRequestTemplate.Parser requestTemplateParser;
    private final TextTemplateEngine templateEngine;
    private final ESLogger logger;

    @Inject
    public HttpEmailAttachementParser(HttpClient httpClient, HttpRequestTemplate.Parser requestTemplateParser, TextTemplateEngine templateEngine) {
        this.httpClient = httpClient;
        this.requestTemplateParser = requestTemplateParser;
        this.templateEngine = templateEngine;
        this.logger = Loggers.getLogger(getClass());
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public HttpRequestAttachment parse(String id, XContentParser parser) throws IOException {
        String contentType = null;
        HttpRequestTemplate requestTemplate = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (ParseFieldMatcher.STRICT.match(currentFieldName, Fields.CONTENT_TYPE)) {
                contentType = parser.text();
            } else if (ParseFieldMatcher.STRICT.match(currentFieldName, Fields.REQUEST)) {
                requestTemplate = requestTemplateParser.parse(parser);
            } else {
                throw new ElasticsearchParseException("Unknown field name [" + currentFieldName + "] in http request attachment configuration");
            }
        }

        if (requestTemplate != null) {
            return new HttpRequestAttachment(id, requestTemplate, contentType);
        }

        throw new ElasticsearchParseException("Could not parse http request attachment");
    }

    @Override
    public Attachment toAttachment(WatchExecutionContext context, Payload payload, HttpRequestAttachment attachment) {
        Map<String, Object> model = Variables.createCtxModel(context, payload);
        HttpRequest httpRequest = attachment.getRequestTemplate().render(templateEngine, model);

        try {
            HttpResponse response = httpClient.execute(httpRequest);
            // check for status 200, only then append attachment
            if (response.status() >= 200 && response.status() < 300) {
                if (response.hasContent()) {
                    String contentType = attachment.getContentType();
                    String attachmentContentType = Strings.hasLength(contentType) ? contentType : response.contentType();
                    return new Attachment.Bytes(attachment.getId(), response.body().toBytes(), attachmentContentType);
                } else {
                    logger.error("Empty response body: [host[{}], port[{}], method[{}], path[{}]: response status [{}]", httpRequest.host(),
                            httpRequest.port(), httpRequest.method(), httpRequest.path(), response.status());
                }
            } else {
                logger.error("Error getting http response: [host[{}], port[{}], method[{}], path[{}]: response status [{}]", httpRequest.host(),
                        httpRequest.port(), httpRequest.method(), httpRequest.path(), response.status());
            }
        } catch (IOException e) {
            logger.error("Error executing HTTP request: [host[{}], port[{}], method[{}], path[{}]: [{}]", e, httpRequest.port(),
                    httpRequest.method(), httpRequest.path(), e.getMessage());
        }

        return null;
    }
}
