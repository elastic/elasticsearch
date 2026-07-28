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
 * Optional tuning knobs for the {@code EQL} source command, supplied via the trailing
 * {@code WITH { ... }} map (e.g. {@code EQL idx | "..." WITH {"tiebreaker_field": "serial_event_id"}}).
 * <p>
 * Each field maps directly to the equivalent {@link org.elasticsearch.xpack.eql.action.EqlSearchRequest} setting. A
 * field left {@code null} (i.e. the corresponding key omitted from the {@code WITH} map) leaves the EQL default in
 * place ({@code @timestamp} for the timestamp, {@code event.category} for the category, and no tiebreaker). Note the
 * parser rejects an explicit {@code null} value in the map. Supplying a {@code tiebreaker_field} is the way to get
 * deterministic ordering for {@code head} / {@code tail} / {@code sequence} / {@code sample} queries when events
 * share a timestamp.
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
