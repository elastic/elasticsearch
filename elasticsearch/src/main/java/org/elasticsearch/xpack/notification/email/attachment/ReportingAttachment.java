/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.notification.email.attachment;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.common.http.auth.HttpAuth;

import java.io.IOException;
import java.util.Objects;

public class ReportingAttachment implements EmailAttachmentParser.EmailAttachment {

    private static final ParseField INLINE = new ParseField("inline");
    private static final ParseField AUTH = new ParseField("auth");
    private static final ParseField INTERVAL = new ParseField("interval");
    private static final ParseField RETRIES = new ParseField("retries");
    private static final ParseField URL = new ParseField("url");

    private final boolean inline;
    private final String id;
    private final HttpAuth auth;
    private final String url;
    private final TimeValue interval;
    private final Integer retries;

    public ReportingAttachment(String id, String url, boolean inline) {
        this(id, url, inline, null, null, null);
    }

    public ReportingAttachment(String id, String url, boolean inline, @Nullable TimeValue interval, @Nullable Integer retries,
                               @Nullable HttpAuth auth) {
        this.id = id;
        this.url = url;
        this.retries = retries;
        this.inline = inline;
        this.auth = auth;
        this.interval = interval;
        if (retries != null && retries < 0) {
            throw new IllegalArgumentException("Retries for attachment must be >= 0");
        }
    }

    @Override
    public String type() {
        return ReportingAttachmentParser.TYPE;
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public boolean inline() {
        return inline;
    }

    public HttpAuth auth() {
        return auth;
    }

    public String url() {
        return url;
    }

    public TimeValue interval() {
        return interval;
    }

    public Integer retries() {
        return retries;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(id).startObject(ReportingAttachmentParser.TYPE)
                .field(URL.getPreferredName(), url);

        if (retries != null) {
            builder.field(RETRIES.getPreferredName(), retries);
        }

        if (interval != null) {
            builder.field(INTERVAL.getPreferredName(), interval);
        }

        if (inline) {
            builder.field(INLINE.getPreferredName(), inline);
        }

        if (auth != null) {
            builder.startObject(AUTH.getPreferredName());
            builder.field(auth.type(), auth, params);
            builder.endObject();
        }

        return builder.endObject().endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ReportingAttachment otherAttachment = (ReportingAttachment) o;
        return Objects.equals(id, otherAttachment.id) && Objects.equals(url, otherAttachment.url) &&
               Objects.equals(interval, otherAttachment.interval) && Objects.equals(inline, otherAttachment.inline) &&
               Objects.equals(retries, otherAttachment.retries) && Objects.equals(auth, otherAttachment.auth);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, url, interval, inline, retries, auth);
    }
}
