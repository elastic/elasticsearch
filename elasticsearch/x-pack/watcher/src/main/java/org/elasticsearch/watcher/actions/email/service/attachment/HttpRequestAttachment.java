/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.email.service.attachment;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.watcher.support.http.HttpRequestTemplate;

import java.io.IOException;

public class HttpRequestAttachment implements EmailAttachmentParser.EmailAttachment {

    private final HttpRequestTemplate requestTemplate;
    private final String contentType;
    private String id;

    public HttpRequestAttachment(String id, HttpRequestTemplate requestTemplate, @Nullable String contentType) {
        this.id = id;
        this.requestTemplate = requestTemplate;
        this.contentType = contentType;
    }

    public HttpRequestTemplate getRequestTemplate() {
        return requestTemplate;
    }

    public String getContentType() {
        return contentType;
    }

    public String getId() {
        return id;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(id)
                .startObject(HttpEmailAttachementParser.TYPE)
                .field(HttpEmailAttachementParser.Fields.REQUEST.getPreferredName(), requestTemplate, params);
        if (Strings.hasLength(contentType)) {
            builder.field(HttpEmailAttachementParser.Fields.CONTENT_TYPE.getPreferredName(), contentType);
        }
        return builder.endObject().endObject();
    }

    public static Builder builder(String id) {
        return new Builder(id);
    }

    @Override
    public String type() {
        return HttpEmailAttachementParser.TYPE;
    }

    public static class Builder {

        private String id;
        private HttpRequestTemplate httpRequestTemplate;
        private String contentType;

        private Builder(String id) {
            this.id = id;
        }

        public Builder httpRequestTemplate(HttpRequestTemplate httpRequestTemplate) {
            this.httpRequestTemplate = httpRequestTemplate;
            return this;
        }

        public Builder contentType(String contentType) {
            this.contentType = contentType;
            return this;
        }

        public HttpRequestAttachment build() {
            return new HttpRequestAttachment(id, httpRequestTemplate, contentType);
        }

    }
}
