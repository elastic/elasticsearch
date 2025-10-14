/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Foldables;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;
import org.elasticsearch.xpack.esql.parser.ParsingException;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Stores a map of query settings.
 * <p>
 *     The key is the setting definition. It must be one of the static constants in this class.
 * </p>
 * <p>
 *     The value is the literal value of the setting.
 * </p>
 */
public record QuerySettings(Map<QuerySettingDef<?>, Literal> settings) implements Writeable {
    // TODO check cluster state and see if project routing is allowed
    // see https://github.com/elastic/elasticsearch/pull/134446
    // PROJECT_ROUTING(..., state -> state.getRemoteClusterNames().crossProjectEnabled());
    public static final QuerySettingDef<String> PROJECT_ROUTING = new QuerySettingDef<>(
        "project_routing",
        DataType.KEYWORD,
        true,
        false,
        true,
        "A project routing literal, "
            + "used to define which projects to route the query to. "
            + "Only supported if Cross-Project Search is enabled.",
        (value) -> Foldables.stringLiteralValueOf(value, "Unexpected value"),
        null
    );

    public static final QuerySettingDef<ZoneId> TIME_ZONE = new QuerySettingDef<>(
        "time_zone",
        DataType.KEYWORD,
        false,
        true,
        true,
        "The default timezone to be used in the query, by the functions and commands that require it. Defaults to UTC",
        (value) -> {
            String timeZone = Foldables.stringLiteralValueOf(value, "Unexpected value");
            try {
                return ZoneId.of(timeZone);
            } catch (Exception exc) {
                throw new IllegalArgumentException("Invalid time zone [" + timeZone + "]");
            }
        },
        ZoneOffset.UTC
    );

    public static final QuerySettings EMPTY = new QuerySettings(Map.of());

    public static final Map<String, QuerySettingDef<?>> SETTINGS_BY_NAME = Stream.of(PROJECT_ROUTING, TIME_ZONE)
        .collect(Collectors.toMap(QuerySettingDef::name, Function.identity()));

    public static QuerySettings from(EsqlStatement statement) {
        return new QuerySettings(
            statement.settings().stream()
                .collect(Collectors.toMap(s -> SETTINGS_BY_NAME.get(s.name()), s -> (Literal) s.value()))
        );
    }

    public static void validate(EsqlStatement statement, RemoteClusterService clusterService) {
        for (QuerySetting setting : statement.settings()) {
            QuerySettingDef<?> def = SETTINGS_BY_NAME.get(setting.name());
            if (def == null) {
                throw new ParsingException(setting.source(), "Unknown setting [" + setting.name() + "]");
            }

            if (setting.value().dataType() != def.type()) {
                throw new ParsingException(setting.source(), "Setting [" + setting.name() + "] must be of type " + def.type());
            }

            Literal literal;
            if (setting.value() instanceof Literal l) {
                literal = l;
            } else {
                throw new ParsingException(setting.source(), "Setting [" + setting.name() + "] must have a literal value");
            }

            String error = def.validator().validate(literal, clusterService);
            if (error != null) {
                throw new ParsingException("Error validating setting [" + setting.name() + "]: " + error);
            }
        }
    }

    public QuerySettings(StreamInput in) throws IOException {
        this(
            in.readMap(i -> SETTINGS_BY_NAME.get(i.readString()), i -> (Literal) i.readNamedWriteable(Expression.class))
        );
    }

    public <T> T get(QuerySettingDef<T> def) {
        var value = settings.get(def);
        return def.parse(value);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        new PlanStreamOutput(out ,null)
            .writeMap(settings, (o, k) -> o.writeString(k.name()), StreamOutput::writeNamedWriteable);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QuerySettings that = (QuerySettings) o;
        return Objects.equals(settings, that.settings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(settings);
    }

    @Override
    public String toString() {
        return "QuerySettings{"
            + "settings="
            + settings
            + '}';
    }

    /**
     * Definition of a query setting.
     *
     * @param name The name to be used when setting it in the query. E.g. {@code SET name=value}
     * @param type The allowed datatype of the setting.
     * @param serverlessOnly
     * @param preview
     * @param snapshotOnly
     * @param description The user-facing description of the setting.
     * @param validator A validation function to check the setting value.
     *                  Defaults to calling the {@link #parser} and returning the error message of any exception it throws.
     * @param parser A function to parse the setting value into the final object.
     * @param defaultValue A default value to be used when the setting is not set.
     * @param <T> The type of the setting value.
     */
    public record QuerySettingDef<T>(
        String name,
        DataType type,
        boolean serverlessOnly,
        boolean preview,
        boolean snapshotOnly,
        String description,
        Validator validator,
        Parser<T> parser,
        T defaultValue
    ) {
        /**
         * Constructor with a default validator that delegates to the parser.
         */
        public QuerySettingDef(
            String name,
            DataType type,
            boolean serverlessOnly,
            boolean preview,
            boolean snapshotOnly,
            String description,
            Parser<T> parser,
            T defaultValue
        ) {
            this(name, type, serverlessOnly, preview, snapshotOnly, description, (value, rcs) -> {
                try {
                    parser.parse(value);
                    return null;
                } catch (Exception exc) {
                    return exc.getMessage();
                }
            }, parser, defaultValue);
        }

        public T parse(@Nullable Literal value) {
            if (value == null) {
                return defaultValue;
            }
            return parser.parse(value);
        }

        @Override
        public String toString() {
            return "QuerySettingDef{"
                + "name="
                + name
                + ", type="
                + type
                + ", serverlessOnly="
                + serverlessOnly
                + ", preview="
                + preview
                + ", snapshotOnly="
                + snapshotOnly
                + ", description="
                + description
                + '}';
        }

        @FunctionalInterface
        public interface Validator {
            /**
             * Validates the setting value and returns the error message if there's an error, or null otherwise.
             */
            @Nullable
            String validate(Literal value, RemoteClusterService clusterService);
        }

        @FunctionalInterface
        public interface Parser<T> {
            /**
             * Parses an already validated literal.
             */
            T parse(Literal value);
        }
    }
}
