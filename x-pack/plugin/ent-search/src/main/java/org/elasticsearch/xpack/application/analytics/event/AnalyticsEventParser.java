/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

/**
 * A util class that can be used to parse {@link AnalyticsEvent} from a payload (e.g HTTP request POST body).
 *
 * Following event type are supported:
 * - "pageview"
 * - "search"
 * - "interaction"
 */
public class AnalyticsEventParser {
    /*
     * Event top-level fields.
     */
    private static final ParseField INTERACTION_FIELD = new ParseField("interaction");
    private static final ParseField PAGE_FIELD = new ParseField("page");
    private static final ParseField SEARCH_FIELD = new ParseField("search");
    private static final ParseField SESSION_FIELD = new ParseField("session");
    private static final ParseField USER_FIELD = new ParseField("user");

    /*
     * Subfields of the "interaction" field.
     */
    private static final ParseField INTERACTION_TYPE_FIELD = new ParseField("type");

    /*
     * Subfields of the "page" field.
     */
    private static final ParseField PAGE_URL_FIELD = new ParseField("url");
    private static final ParseField PAGE_TITLE_FIELD = new ParseField("title");
    private static final ParseField PAGE_REFERRER_FIELD = new ParseField("referrer");

    /*
     * Subfields of the "query" field.
     */
    private static final ParseField SEARCH_QUERY_FIELD = new ParseField("query");

    /*
     * Subfields of the "session" field.
     */
    private static final ParseField SESSION_ID_FIELD = new ParseField("id");

    /*
     * Subfields of the "user" field.
     */
    private static final ParseField USER_ID_FIELD = new ParseField("id");

    /*
     * Parser for event with the type "interaction".
     */
    private static final ConstructingObjectParser<AnalyticsEventBuilder, AnalyticsContext> INTERACTION_EVENT_PARSER =
        new ConstructingObjectParser<>(
            "interaction_event",
            false,
            (params, context) -> new AnalyticsEventBuilder(context).withField(SESSION_FIELD.getPreferredName(), params[0])
                .withField(USER_FIELD.getPreferredName(), params[1])
                .withField(INTERACTION_FIELD.getPreferredName(), params[2])
                .withField(PAGE_FIELD.getPreferredName(), params[3])
                .withField(SEARCH_FIELD.getPreferredName(), params[4])
        );

    /*
     * Parser for event with the type "pageview".
     */
    private static final ConstructingObjectParser<AnalyticsEventBuilder, AnalyticsContext> PAGEVIEW_EVENT_PARSER =
        new ConstructingObjectParser<>(
            "pageview_event",
            false,
            (params, context) -> new AnalyticsEventBuilder(context).withField(SESSION_FIELD.getPreferredName(), params[0])
                .withField(USER_FIELD.getPreferredName(), params[1])
                .withField(PAGE_FIELD.getPreferredName(), params[2])
        );

    /*
     * Parser for event with the type "search".
     */
    private static final ConstructingObjectParser<AnalyticsEventBuilder, AnalyticsContext> SEARCH_EVENT_PARSER =
        new ConstructingObjectParser<>(
            "search_event",
            false,
            (params, context) -> new AnalyticsEventBuilder(context).withField(SESSION_FIELD.getPreferredName(), params[0])
                .withField(USER_FIELD.getPreferredName(), params[1])
                .withField(SEARCH_FIELD.getPreferredName(), params[2])
        );

    /*
     * Parser for "interaction" event field".
     */
    private static final ConstructingObjectParser<Map<String, Object>, Void> INTERACTION_DATA_PARSER = new ConstructingObjectParser<>(
        "interaction",
        false,
        (params, context) -> MapBuilder.<String, Object>newMapBuilder().put(INTERACTION_TYPE_FIELD.getPreferredName(), params[0]).map()
    );

    /*
     * Parser for "page" event field".
     */
    private static final ConstructingObjectParser<Map<String, Object>, Void> PAGE_DATA_PARSER = new ConstructingObjectParser<>(
        "page",
        false,
        (params, context) -> {
            MapBuilder<String, Object> payloadBuilder = MapBuilder.newMapBuilder();

            payloadBuilder.put(PAGE_URL_FIELD.getPreferredName(), params[0]);

            if (Strings.isNullOrEmpty((String) params[1]) == false) {
                payloadBuilder.put(PAGE_TITLE_FIELD.getPreferredName(), params[0]);
            }

            if (Strings.isNullOrEmpty((String) params[2]) == false) {
                payloadBuilder.put(PAGE_REFERRER_FIELD.getPreferredName(), params[0]);
            }

            return payloadBuilder.map();
        }
    );

    /*
     * Parser for "search" event field".
     */
    private static final ConstructingObjectParser<Map<String, Object>, Void> SEARCH_DATA_PARSER = new ConstructingObjectParser<>(
        "search",
        false,
        (params, context) -> MapBuilder.<String, Object>newMapBuilder().put(SEARCH_QUERY_FIELD.getPreferredName(), params[0]).map()
    );

    /*
     * Parser for "session" event field".
     */
    private static final ConstructingObjectParser<Map<String, Object>, Void> SESSION_DATA_PARSER = new ConstructingObjectParser<>(
        "session",
        false,
        (params, context) -> MapBuilder.<String, Object>newMapBuilder().put(SESSION_ID_FIELD.getPreferredName(), params[0]).map()
    );

    /*
     * Parser for "user" event field".
     */
    private static final ConstructingObjectParser<Map<String, Object>, Void> USER_DATA_PARSER = new ConstructingObjectParser<>(
        "user",
        false,
        (params, context) -> MapBuilder.<String, Object>newMapBuilder().put(USER_ID_FIELD.getPreferredName(), params[0]).map()
    );

    static {
        // Declare subfields for the "interaction" event parser.
        INTERACTION_EVENT_PARSER.declareObject(
            ConstructingObjectParser.constructorArg(),
            (p, c) -> SESSION_DATA_PARSER.parse(p, null),
            SESSION_FIELD
        );
        INTERACTION_EVENT_PARSER.declareObject(
            ConstructingObjectParser.constructorArg(),
            (p, c) -> USER_DATA_PARSER.parse(p, null),
            USER_FIELD
        );
        INTERACTION_EVENT_PARSER.declareObject(
            ConstructingObjectParser.constructorArg(),
            (p, c) -> INTERACTION_DATA_PARSER.parse(p, null),
            INTERACTION_FIELD
        );
        INTERACTION_EVENT_PARSER.declareObject(
            ConstructingObjectParser.constructorArg(),
            (p, c) -> PAGE_DATA_PARSER.parse(p, null),
            PAGE_FIELD
        );
        INTERACTION_EVENT_PARSER.declareObject(
            ConstructingObjectParser.constructorArg(),
            (p, c) -> SEARCH_DATA_PARSER.parse(p, null),
            SEARCH_FIELD
        );

        // Declare subfields for the "pageview" event parser.
        PAGEVIEW_EVENT_PARSER.declareObject(
            ConstructingObjectParser.constructorArg(),
            (p, c) -> SESSION_DATA_PARSER.parse(p, null),
            SESSION_FIELD
        );
        PAGEVIEW_EVENT_PARSER.declareObject(
            ConstructingObjectParser.constructorArg(),
            (p, c) -> USER_DATA_PARSER.parse(p, null),
            USER_FIELD
        );
        PAGEVIEW_EVENT_PARSER.declareObject(
            ConstructingObjectParser.constructorArg(),
            (p, c) -> PAGE_DATA_PARSER.parse(p, null),
            PAGE_FIELD
        );

        // Declare subfields for the "search" event parser.
        SEARCH_EVENT_PARSER.declareObject(
            ConstructingObjectParser.constructorArg(),
            (p, c) -> SESSION_DATA_PARSER.parse(p, null),
            SESSION_FIELD
        );
        SEARCH_EVENT_PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> USER_DATA_PARSER.parse(p, null), USER_FIELD);
        SEARCH_EVENT_PARSER.declareObject(
            ConstructingObjectParser.constructorArg(),
            (p, c) -> SEARCH_DATA_PARSER.parse(p, null),
            SEARCH_FIELD
        );

        // Declare subfields for the "interaction" field parser.
        INTERACTION_DATA_PARSER.declareString(
            ConstructingObjectParser.constructorArg(),
            (s) -> InteractionType.valueOf(s.toUpperCase(Locale.ROOT)),
            INTERACTION_TYPE_FIELD
        );

        // Declare subfields for the "page" field parser.
        PAGE_DATA_PARSER.declareString(ConstructingObjectParser.constructorArg(), PAGE_URL_FIELD);
        PAGE_DATA_PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), PAGE_TITLE_FIELD);
        PAGE_DATA_PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), PAGE_REFERRER_FIELD);

        // Declare subfields for the "search" field parser.
        SEARCH_DATA_PARSER.declareString(ConstructingObjectParser.constructorArg(), SEARCH_QUERY_FIELD);

        // Declare subfields for the "session" field parser.
        SESSION_DATA_PARSER.declareString(ConstructingObjectParser.constructorArg(), SESSION_ID_FIELD);

        // Declare subfields for the "user" field parser.
        USER_DATA_PARSER.declareString(ConstructingObjectParser.constructorArg(), USER_ID_FIELD);
    }

    /**
     * Instantiate ann {@link AnalyticsEvent} object from an Analytics context and a payload (e.g. HTTP request post).
     *
     * @param context Analytics context
     * @param xContentType Type of the payload
     * @param payload Payload
     *
     * @return Parsed event ({@link AnalyticsEvent})
     */
    public static AnalyticsEvent fromPayload(AnalyticsContext context, XContentType xContentType, BytesReference payload)
        throws IOException {
        XContentParser parser = xContentType.xContent().createParser(XContentParserConfiguration.EMPTY, payload.streamInput());
        return getEventParser(context.eventType()).parse(parser, context).build();
    }

    /**
     * Return the right event parser for an event type.
     *
     * @param eventType {@link AnalyticsEventType}
     *
     * @return {@link ConstructingObjectParser<AnalyticsEventBuilder, AnalyticsContext>}
     */
    private static ConstructingObjectParser<AnalyticsEventBuilder, AnalyticsContext> getEventParser(AnalyticsEventType eventType) {

        if (eventType == AnalyticsEventType.PAGEVIEW) return PAGEVIEW_EVENT_PARSER;
        if (eventType == AnalyticsEventType.SEARCH) return SEARCH_EVENT_PARSER;
        if (eventType == AnalyticsEventType.INTERACTION) return INTERACTION_EVENT_PARSER;

        throw new IllegalArgumentException(org.elasticsearch.core.Strings.format("%s is not a supported event type", eventType));
    }

    /**
     * Used internally by the parser to build the {@link AnalyticsEvent} object.
     */
    static class AnalyticsEventBuilder {
        private final AnalyticsContext context;

        private final MapBuilder<String, Object> payloadBuilder = MapBuilder.newMapBuilder();

        AnalyticsEventBuilder(AnalyticsContext context) {
            this.context = context;
        }

        AnalyticsEventBuilder withField(String name, Object value) {
            payloadBuilder.put(name, value);
            return this;
        }

        AnalyticsEvent build() throws IOException {
            XContentBuilder xContentBuilder = JsonXContent.contentBuilder().map(payloadBuilder.map());
            return new AnalyticsEvent(context, XContentType.JSON, BytesReference.bytes(xContentBuilder));
        }
    }
}
