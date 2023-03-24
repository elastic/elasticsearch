/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.xcontent.ContextParser;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.application.analytics.action.PostAnalyticsEventAction;

import java.io.IOException;
import java.util.Map;

/**
 * A util class that can be used to parse {@link AnalyticsEvent} from a payload (e.g HTTP request POST body) or input stream.
 */
public class AnalyticsEventFactory {
    private static final Map<AnalyticsEvent.Type, ContextParser<AnalyticsEvent.Context, AnalyticsEvent>> EVENT_PARSERS = MapBuilder.<
        AnalyticsEvent.Type,
        ContextParser<AnalyticsEvent.Context, AnalyticsEvent>>newMapBuilder()
        .put(AnalyticsEvent.Type.PAGEVIEW, AnalyticsEventPageView::fromXContent)
        .put(AnalyticsEvent.Type.SEARCH, AnalyticsEventSearch::fromXContent)
        .put(AnalyticsEvent.Type.SEARCH_CLICK, AnalyticsEventSearchClick::fromXContent)
        .immutableMap();

    private static final Map<AnalyticsEvent.Type, Writeable.Reader<AnalyticsEvent>> EVENT_READERS = MapBuilder.<
        AnalyticsEvent.Type,
        Writeable.Reader<AnalyticsEvent>>newMapBuilder()
        .put(AnalyticsEvent.Type.PAGEVIEW, AnalyticsEventPageView::new)
        .put(AnalyticsEvent.Type.SEARCH, AnalyticsEventSearch::new)
        .put(AnalyticsEvent.Type.SEARCH_CLICK, AnalyticsEventSearchClick::new)
        .immutableMap();

    public AnalyticsEventFactory() {}

    public AnalyticsEvent fromRequest(PostAnalyticsEventAction.Request request) throws IOException {
        return fromPayload(request, request.xContentType(), request.payload());
    }

    public AnalyticsEvent fromStreamInput(AnalyticsEvent.Type eventType, StreamInput in) throws IOException {
        if (EVENT_READERS.containsKey(eventType)) {
            return EVENT_READERS.get(eventType).read(in);
        }

        throw new IllegalArgumentException(LoggerMessageFormat.format("{} is not a supported event type", eventType));
    }

    /**
     * Instantiate an {@link AnalyticsEvent} object from an Analytics context and a payload (e.g. HTTP POST request body).
     *
     * @param context Analytics context
     * @param xContentType Type of the payload
     * @param payload Payload
     *
     * @return Parsed event ({@link AnalyticsEvent})
     */
    public AnalyticsEvent fromPayload(AnalyticsEvent.Context context, XContentType xContentType, BytesReference payload)
        throws IOException {
        XContentParser parser = xContentType.xContent().createParser(XContentParserConfiguration.EMPTY, payload.streamInput());
        AnalyticsEvent.Type eventType = context.eventType();

        if (EVENT_PARSERS.containsKey(eventType)) {
            return EVENT_PARSERS.get(eventType).parse(parser, context);
        }

        throw new IllegalArgumentException(LoggerMessageFormat.format("{} is not a supported event type", eventType));
    }
}
