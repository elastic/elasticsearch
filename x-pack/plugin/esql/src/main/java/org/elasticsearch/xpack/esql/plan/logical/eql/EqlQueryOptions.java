/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.eql;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.List;

/**
 * Optional tuning knobs for the {@code EQL} source command's trailing {@code WITH { ... }} map (e.g.
 * {@code WITH {"tiebreaker_field": "serial_event_id"}}). Each field maps to the matching
 * {@link org.elasticsearch.xpack.eql.action.EqlSearchRequest} setting; a {@code null} field leaves the EQL default
 * ({@code @timestamp}, {@code event.category}, no tiebreaker). A tiebreaker gives deterministic ordering for
 * {@code head} / {@code tail} / {@code sequence} / {@code sample} when events share a timestamp.
 */
public record EqlQueryOptions(@Nullable String tiebreakerField, @Nullable String timestampField, @Nullable String eventCategoryField) {

    public static final String TIEBREAKER_FIELD_OPTION = "tiebreaker_field";
    public static final String TIMESTAMP_FIELD_OPTION = "timestamp_field";
    public static final String EVENT_CATEGORY_FIELD_OPTION = "event_category_field";

    /** The option keys accepted in the {@code WITH} map, sorted for stable error messages. */
    public static final List<String> VALID_OPTION_NAMES = List.of(
        EVENT_CATEGORY_FIELD_OPTION,
        TIEBREAKER_FIELD_OPTION,
        TIMESTAMP_FIELD_OPTION
    );

    /** All EQL server defaults (no overrides). */
    public static final EqlQueryOptions DEFAULTS = new EqlQueryOptions(null, null, null);

    public EqlQueryOptions(StreamInput in) throws IOException {
        this(in.readOptionalString(), in.readOptionalString(), in.readOptionalString());
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(tiebreakerField);
        out.writeOptionalString(timestampField);
        out.writeOptionalString(eventCategoryField);
    }
}
