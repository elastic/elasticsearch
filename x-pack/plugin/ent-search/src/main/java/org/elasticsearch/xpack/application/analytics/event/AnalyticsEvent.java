/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * This class represents Analytics events object meant to be emitted to the event queue.
 *
 * An analytics events is composed of:
 * - A context ({@link AnalyticsContext}) containing basic event meta-data (type, time, collection)
 * - A payload containing a JSON representation of the event.
 *
 * The payload has to be validated. The best way to do it is to create events using the {@link AnalyticsEventParser}.
 */
public class AnalyticsEvent implements Writeable, ToXContentObject {
    private final AnalyticsContext analyticsContext;

    private final XContentType xContentType;

    private final BytesReference payload;

    protected AnalyticsEvent(AnalyticsContext analyticsContext, XContentType xContentType, BytesReference payload) {
        this.analyticsContext = Objects.requireNonNull(analyticsContext);
        this.xContentType = Objects.requireNonNull(xContentType);
        this.payload = Objects.requireNonNull(payload);
    }

    protected AnalyticsEvent(StreamInput in) throws IOException {
        this(new AnalyticsContext(in), in.readEnum(XContentType.class), in.readBytesReference());
    }

    public AnalyticsContext getAnalyticsContext() {
        return analyticsContext;
    }

    public XContentType getxContentType() {
        return xContentType;
    }

    public BytesReference getPayload() {
        return payload;
    }

    public Map<String, Object> getPayloadAsMap() {
        return XContentHelper.convertToMap(payload, true, xContentType).v2();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        analyticsContext.writeTo(out);
        XContentHelper.writeTo(out, xContentType);
        out.writeBytesReference(payload);
    }

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field("@timestamp", analyticsContext.eventTime());

            builder.startObject("event");
            {
                builder.field("action", analyticsContext.eventType());
            }
            builder.endObject();

            builder.startObject("data_stream");
            {
                builder.field("type", "behavioral_analytics");
                builder.field("dataset", "events");
                builder.field("namespace", analyticsContext.eventCollection().getName());

            }
            builder.endObject();

            // Render additional fields from the event payload (session, user, page, ...)
            builder.mapContents(getPayloadAsMap());
        }
        builder.endObject();

        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AnalyticsEvent that = (AnalyticsEvent) o;
        return Objects.equals(analyticsContext, that.analyticsContext)
            && Objects.equals(xContentType, that.xContentType)
            && Objects.equals(payload, that.payload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(analyticsContext, xContentType, payload);
    }
}
