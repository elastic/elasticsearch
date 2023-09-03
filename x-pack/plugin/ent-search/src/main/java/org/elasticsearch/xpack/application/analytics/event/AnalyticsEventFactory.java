/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xcontent.ContextParser;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.application.analytics.action.PostAnalyticsEventAction;
import org.elasticsearch.xpack.application.analytics.event.parser.event.PageViewAnalyticsEvent;
import org.elasticsearch.xpack.application.analytics.event.parser.event.SearchAnalyticsEvent;
import org.elasticsearch.xpack.application.analytics.event.parser.event.SearchClickAnalyticsEvent;

import java.io.IOException;
import java.util.Map;

import static java.util.Map.entry;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEvent.Type.PAGE_VIEW;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEvent.Type.SEARCH;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEvent.Type.SEARCH_CLICK;

/**
 * A utility class for parsing {@link AnalyticsEvent} objects from payloads (such as HTTP POST request bodies) or input streams.
 */
public class AnalyticsEventFactory {

    public static final AnalyticsEventFactory INSTANCE = new AnalyticsEventFactory();

    private static final Map<AnalyticsEvent.Type, ContextParser<AnalyticsEvent.Context, AnalyticsEvent>> EVENT_PARSERS = Map.ofEntries(
        entry(PAGE_VIEW, PageViewAnalyticsEvent::fromXContent),
        entry(SEARCH, SearchAnalyticsEvent::fromXContent),
        entry(SEARCH_CLICK, SearchClickAnalyticsEvent::fromXContent)
    );

    private AnalyticsEventFactory() {

    }

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
     * Creates an {@link AnalyticsEvent} object from an Analytics context and a payload (e.g. HTTP POST request body).
     *
     * @param context      the analytics context
     * @param xContentType the type of the payload
     * @param payload      the payload as a {@link BytesReference} object
     * @return Parsed event ({@link AnalyticsEvent})
     */
    public AnalyticsEvent fromPayload(AnalyticsEvent.Context context, XContentType xContentType, BytesReference payload)
        throws IOException {
        try (XContentParser parser = xContentType.xContent().createParser(XContentParserConfiguration.EMPTY, payload.streamInput())) {
            AnalyticsEvent.Type eventType = context.eventType();

            if (EVENT_PARSERS.containsKey(eventType)) {
                return EVENT_PARSERS.get(eventType).parse(parser, context);
            }

            throw new IllegalArgumentException(Strings.format("[%s] is not a supported event type", eventType));
        }
    }
}
