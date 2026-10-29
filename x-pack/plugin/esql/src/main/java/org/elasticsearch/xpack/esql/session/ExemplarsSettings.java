/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;

import java.io.IOException;
import java.util.Map;

/**
 * The value of the {@code exemplars} query setting: whether the query returns exemplars instead of its result and, optionally, how many
 * at most and whether to identify the series they belong to. {@code SET exemplars=true} enables it without a limit of its own, so the
 * default limit of a regular query applies to the exemplars; {@code SET exemplars={"limit": n}} enables it and returns at most
 * {@code n}; {@code SET exemplars={"include_timeseries": true}} adds the {@code _timeseries} column holding the dimensions of each
 * exemplar's series.
 */
public record ExemplarsSettings(boolean enabled, @Nullable Integer limit, boolean includeTimeseries) implements Writeable {

    public static final ExemplarsSettings DISABLED = new ExemplarsSettings(false, null, false);
    public static final ExemplarsSettings ENABLED = new ExemplarsSettings(true, null, false);

    private static final String LIMIT = "limit";
    private static final String INCLUDE_TIMESERIES = "include_timeseries";

    /**
     * The setting is written with the configuration of every query, not only of an exemplars one, so an ungated field here fails all
     * of them against a peer that predates it. The development capability of the setting does not cover its wire format.
     */
    private static final TransportVersion EXEMPLARS_INCLUDE_TIMESERIES = TransportVersion.fromName("esql_exemplars_include_timeseries");

    public ExemplarsSettings {
        if (limit != null && limit <= 0) {
            throw new IllegalArgumentException("Exemplars configuration [" + LIMIT + "] must be positive, got [" + limit + "]");
        }
    }

    public ExemplarsSettings(StreamInput in) throws IOException {
        this(
            in.readBoolean(),
            in.readOptionalVInt(),
            in.getTransportVersion().supports(EXEMPLARS_INCLUDE_TIMESERIES) ? in.readBoolean() : false
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(enabled);
        out.writeOptionalVInt(limit);
        if (out.getTransportVersion().supports(EXEMPLARS_INCLUDE_TIMESERIES)) {
            out.writeBoolean(includeTimeseries);
        }
    }

    /** Reads the setting from a {@code SET}: a boolean, or a map with an optional {@code limit} and {@code include_timeseries}. */
    public static ExemplarsSettings parse(Expression expression) {
        return switch (expression) {
            case Literal literal when literal.value() instanceof Boolean value -> value ? ENABLED : DISABLED;
            case MapExpression map -> parse(map);
            default -> throw new IllegalArgumentException("Invalid exemplars configuration [" + expression + "]");
        };
    }

    private static ExemplarsSettings parse(MapExpression expression) {
        Map<String, Object> map;
        try {
            map = expression.toFoldedMap(FoldContext.small());
        } catch (IllegalStateException e) {
            throw new IllegalArgumentException("Exemplars configuration must be a constant value [" + expression + "]");
        }
        Integer limit = null;
        boolean includeTimeseries = false;
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            switch (entry.getKey()) {
                case LIMIT -> {
                    if (entry.getValue() instanceof Integer value) {
                        limit = value;
                    } else {
                        throw new IllegalArgumentException("Exemplars configuration [" + LIMIT + "] must be an integer value");
                    }
                }
                case INCLUDE_TIMESERIES -> {
                    if (entry.getValue() instanceof Boolean value) {
                        includeTimeseries = value;
                    } else {
                        throw new IllegalArgumentException("Exemplars configuration [" + INCLUDE_TIMESERIES + "] must be a boolean value");
                    }
                }
                default -> throw new IllegalArgumentException("Exemplars configuration contains unknown key [" + entry.getKey() + "]");
            }
        }
        return new ExemplarsSettings(true, limit, includeTimeseries);
    }

    /** Reads the setting from JSON, in the same two shapes {@link #parse(Expression)} accepts. */
    public static ExemplarsSettings fromXContent(XContentParser parser) throws IOException {
        if (parser.currentToken() == XContentParser.Token.VALUE_BOOLEAN) {
            return parser.booleanValue() ? ENABLED : DISABLED;
        }
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            throw new IllegalArgumentException(
                "Exemplars configuration must be a boolean or an object, got [" + parser.currentToken() + "]"
            );
        }
        Integer limit = null;
        boolean includeTimeseries = false;
        for (XContentParser.Token token = parser.nextToken(); token != XContentParser.Token.END_OBJECT; token = parser.nextToken()) {
            if (token != XContentParser.Token.FIELD_NAME) {
                throw new IllegalArgumentException("Exemplars configuration is malformed at [" + token + "]");
            }
            String key = parser.currentName();
            parser.nextToken();
            switch (key) {
                case LIMIT -> limit = parser.intValue();
                case INCLUDE_TIMESERIES -> includeTimeseries = parser.booleanValue();
                default -> throw new IllegalArgumentException("Exemplars configuration contains unknown key [" + key + "]");
            }
        }
        return new ExemplarsSettings(true, limit, includeTimeseries);
    }
}
