/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Foldables;
import org.elasticsearch.xpack.esql.parser.ParsingException;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class QuerySettings {
    public static final QuerySettingDef<String> PROJECT_ROUTING = new QuerySettingDef<>(
        "project_routing",
        DataType.KEYWORD,
        true,
        false,
        true,
        "A project routing expression, "
            + "used to define which projects to route the query to. "
            + "Only supported if Cross-Project Search is enabled.",
        // TODO enable this when CPS is ready and we move this to tech preview
        // (value, ctx) -> ctx.crossProjectEnabled() ? null : "not enabled",
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

    public static final Map<String, QuerySettingDef<?>> SETTINGS_BY_NAME = Stream.of(PROJECT_ROUTING, TIME_ZONE)
        .collect(Collectors.toMap(QuerySettingDef::name, Function.identity()));

    public static void validate(EsqlStatement statement, SettingsValidationContext ctx) {
        for (QuerySetting setting : statement.settings()) {
            QuerySettingDef<?> def = SETTINGS_BY_NAME.get(setting.name());
            if (def == null) {
                throw new ParsingException(setting.source(), "Unknown setting [" + setting.name() + "]");
            }

            if (def.snapshotOnly && ctx.isSnapshot() == false) {
                throw new ParsingException(setting.source(), "Setting [" + setting.name() + "] is only available in snapshot builds");
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

            String error = def.validator().validate(literal, ctx);
            if (error != null) {
                throw new ParsingException("Error validating setting [" + setting.name() + "]: " + error);
            }
        }
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

        @FunctionalInterface
        public interface Validator {
            /**
             * Validates the setting value and returns the error message if there's an error, or null otherwise.
             */
            @Nullable
            String validate(Literal value, SettingsValidationContext ctx);
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
