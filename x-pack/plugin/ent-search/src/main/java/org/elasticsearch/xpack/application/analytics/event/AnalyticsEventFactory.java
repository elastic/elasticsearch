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
 * A utility class for parsing {@link AnalyticsEvent} objects from payloads (such as HTTP POST request bodies) or input streams.
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

    /**
     * Creates an {@link AnalyticsEvent} object from a {@link PostAnalyticsEventAction.Request} object.
     *
     * @param request the {@link PostAnalyticsEventAction.Request} object
     *
     * @return the parsed {@link AnalyticsEvent} object
     *
     * @throws IOException if an I/O error occurs while parsing the event
     */
    public AnalyticsEvent fromRequest(PostAnalyticsEventAction.Request request) throws IOException {
        return fromPayload(request, request.xContentType(), request.payload());
    }

    /**
     * Creates an {@link AnalyticsEvent} object from an input stream.
     *
     * @param eventType the type of the analytics event to create
     * @param in the input stream
     *
     * @return the parsed {@link AnalyticsEvent} object
     *
     * @throws IOException if an I/O error occurs while parsing the event
     * @throws IllegalArgumentException if the specified event type is not supported
     */

    public AnalyticsEvent fromStreamInput(AnalyticsEvent.Type eventType, StreamInput in) throws IOException {
        if (EVENT_READERS.containsKey(eventType)) {
            return EVENT_READERS.get(eventType).read(in);
        }

        throw new IllegalArgumentException(LoggerMessageFormat.format("{} is not a supported event type", eventType));
    }

    /**
     * Creates an {@link AnalyticsEvent} object from an Analytics context and a payload (e.g. HTTP POST request body).
     *
     * @param context the analytics context
     * @param xContentType the type of the payload
     * @param payload the payload as a {@link BytesReference} object
     *
     * @return Parsed event ({@link AnalyticsEvent})
     */
    public AnalyticsEvent fromPayload(AnalyticsEvent.Context context, XContentType xContentType, BytesReference payload)
        throws IOException {
        try (XContentParser parser = xContentType.xContent().createParser(XContentParserConfiguration.EMPTY, payload.streamInput())) {
            AnalyticsEvent.Type eventType = context.eventType();

            if (EVENT_PARSERS.containsKey(eventType)) {
                return EVENT_PARSERS.get(eventType).parse(parser, context);
            }

            throw new IllegalArgumentException(LoggerMessageFormat.format("{} is not a supported event type", eventType));
        }
    }
}
