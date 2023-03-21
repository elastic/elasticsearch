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
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.application.analytics.event.parser.InteractionEvent;
import org.elasticsearch.xpack.application.analytics.event.parser.PageViewEvent;
import org.elasticsearch.xpack.application.analytics.event.parser.SearchEvent;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class AnalyticsEvent implements Writeable, ToXContentObject {
    private final AnalyticsContext analyticsContext;

    private final Map<String, Object> payload;

    public AnalyticsEvent(AnalyticsContext analyticsContext, Map<String, Object> payload) {
        this.analyticsContext = analyticsContext;
        this.payload = payload;
    }

    public AnalyticsEvent(StreamInput in) throws IOException {
        this(new AnalyticsContext(in), XContentHelper.convertToMap(in.readBytesReference(), true, XContentType.JSON).v2());
    }

    public static AnalyticsEvent fromPayload(AnalyticsContext context, XContentType xContentType, BytesReference payload)
        throws IOException {
        XContentParser parser = xContentType.xContent().createParser(XContentParserConfiguration.EMPTY, payload.streamInput());
        AnalyticsEventType eventType = context.eventType();

        if (eventType == AnalyticsEventType.PAGEVIEW) return PageViewEvent.parse(parser, context);

        if (eventType == AnalyticsEventType.SEARCH) return SearchEvent.parse(parser, context);

        if (eventType == AnalyticsEventType.INTERACTION) return InteractionEvent.parse(parser, context);

        throw new IllegalArgumentException(org.elasticsearch.core.Strings.format("%s is not a supported event type", eventType));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        analyticsContext.writeTo(out);
        out.writeBytesReference(BytesReference.bytes(JsonXContent.contentBuilder().map(payload)));
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
            builder.mapContents(payload);
        }
        builder.endObject();

        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AnalyticsEvent that = (AnalyticsEvent) o;
        return Objects.equals(analyticsContext, that.analyticsContext) && Objects.equals(payload, that.payload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(analyticsContext, payload);
    }
}
