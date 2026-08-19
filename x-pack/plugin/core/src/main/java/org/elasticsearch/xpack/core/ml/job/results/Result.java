/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.job.results;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.common.time.TimeUtils;

import java.io.IOException;
import java.time.Instant;

/**
 * A wrapper for concrete result objects plus meta information.
 * Also contains common attributes for results.
 */
public class Result<T> {

    /**
     * Serialisation fields
     */
    public static final ParseField TYPE = new ParseField("result");
    public static final ParseField RESULT_TYPE = new ParseField("result_type");
    public static final ParseField TIMESTAMP = new ParseField("timestamp");
    public static final ParseField IS_INTERIM = new ParseField("is_interim");
    public static final ParseField EVENT = new ParseField("event");
    public static final ParseField INGESTED = new ParseField("ingested");

    /**
     * Parses the ECS {@code event.ingested} nested object from an XContentParser positioned at the start of the {@code event} object.
     * Returns the value of the nested {@code ingested} field, or {@code null} if it is absent.
     */
    public static Instant parseEventIngested(XContentParser parser) throws IOException {
        Instant ingested = null;
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            if (parser.currentToken() == XContentParser.Token.FIELD_NAME && INGESTED.getPreferredName().equals(parser.currentName())) {
                parser.nextToken();
                ingested = TimeUtils.parseTimeFieldToInstant(parser, INGESTED.getPreferredName());
            } else {
                parser.skipChildren();
            }
        }
        return ingested;
    }

    @Nullable
    public final String index;
    @Nullable
    public final T result;

    public Result(String index, T result) {
        this.index = index;
        this.result = result;
    }
}
