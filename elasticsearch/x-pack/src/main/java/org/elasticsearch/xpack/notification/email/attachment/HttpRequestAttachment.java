/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.notification.email.attachment;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.watcher.support.http.HttpRequestTemplate;

import java.io.IOException;
import java.util.Objects;

public class HttpRequestAttachment implements EmailAttachmentParser.EmailAttachment {

    private final HttpRequestTemplate requestTemplate;
    private final String contentType;
    private final String id;

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

    @Override
    public String id() {
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HttpRequestAttachment otherDataAttachment = (HttpRequestAttachment) o;
        return Objects.equals(id, otherDataAttachment.id) && Objects.equals(requestTemplate, otherDataAttachment.requestTemplate)
                && Objects.equals(contentType, otherDataAttachment.contentType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, requestTemplate, contentType);
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
