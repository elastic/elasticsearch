/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.analysis.UnmappedResolution;
import org.elasticsearch.xpack.esql.approximation.ApproximationSettings;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Foldables;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.MapParam;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.parser.ParsingException;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class QuerySettings {

    @Param(
        name = "project_routing",
        type = { "keyword" },
        description = "A project routing expression, "
            + "used to define which projects to route the query to. "
            + "Only supported if Cross-Project Search is enabled."
        // TODO add a link to CPS docs when available
    )
    @Example(file = "from", tag = "project-routing", description = "Routes the query to the specified project.")
    public static final QuerySettingDef<String> PROJECT_ROUTING = new QuerySettingDef<>(
        "project_routing",
        DataType.KEYWORD,
        true,
        true,
        false,
        (value, ctx) -> ctx.crossProjectEnabled() ? null : "cross-project search not enabled",
        (value) -> Foldables.stringLiteralValueOf(value, "Unexpected value"),
        null
    );

    @Param(
        name = "time_zone",
        type = { "keyword" },
        since = "9.4+",
        description = "The default timezone to be used in the query. Defaults to UTC, and overrides the `time_zone` request parameter. "
            + "See [timezones](/reference/query-languages/esql/esql-rest.md#esql-timezones)."
    )
    @Example(file = "tbucket", tag = "set-timezone-example")
    public static final QuerySettingDef<ZoneId> TIME_ZONE = new QuerySettingDef<>(
        "time_zone",
        DataType.KEYWORD,
        false,
        false,
        false,
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

    @Param(
        name = "unmapped_fields",
        type = { "keyword" },
        since = "9.3.0",
        description = "Defines how unmapped fields are treated. Possible values are: "
            + "\"FAIL\" (default) - fails the query if unmapped fields are present; "
            + "\"NULLIFY\" - treats unmapped fields as null values. "
        // + "\"LOAD\" - attempts to load the fields from the source." Commented out since LOAD is currently only under snapshot.
    )
    @Example(file = "unmapped-nullify", tag = "unmapped-nullify-simple-keep", description = "Make the field null if it is unmapped.")
    public static final QuerySettingDef<UnmappedResolution> UNMAPPED_FIELDS = new QuerySettingDef<>(
        "unmapped_fields",
        DataType.KEYWORD,
        false,
        true,
        false,
        (value) -> {
            String resolution = Foldables.stringLiteralValueOf(value, "Unexpected value");
            try {
                UnmappedResolution res = UnmappedResolution.valueOf(resolution.toUpperCase(Locale.ROOT));
                if (res == UnmappedResolution.LOAD && EsqlCapabilities.Cap.OPTIONAL_FIELDS.isEnabled() == false) {
                    throw new IllegalArgumentException("'LOAD' is only supported in snapshot builds");
                }
                return res;
            } catch (Exception exc) {
                var values = EsqlCapabilities.Cap.OPTIONAL_FIELDS.isEnabled()
                    ? UnmappedResolution.values()
                    : Arrays.stream(UnmappedResolution.values()).filter(e -> e != UnmappedResolution.LOAD).toArray();

                throw new IllegalArgumentException(
                    "Invalid unmapped_fields resolution [" + value + "], must be one of " + Arrays.toString(values)
                );
            }
        },
        UnmappedResolution.FAIL
    );

    @Param(name = "approximation", type = { "boolean", "map_param" }, description = "TODO - add description here")
    @MapParam(
        name = "approximation",
        params = {
            @MapParam.MapParamEntry(name = "num_rows", type = { "integer" }, description = "Number of rows."),
            @MapParam.MapParamEntry(name = "confidence_level", type = { "double" }, description = "Confidence level.") }
    )
    // TODO add examples when approximate is implemented, eg.
    // @Example(file = "approximate", tag = "approximate-with-boolean")
    // @Example(file = "approximate", tag = "approximate-with-map")
    public static final QuerySettingDef<ApproximationSettings> APPROXIMATION = new QuerySettingDef<>(
        "approximation",
        null,
        false,
        false,
        true,
        ApproximationSettings::parse,
        null
    );

    public static final Map<String, QuerySettingDef<?>> SETTINGS_BY_NAME = Stream.of(
        UNMAPPED_FIELDS,
        PROJECT_ROUTING,
        TIME_ZONE,
        APPROXIMATION
    ).collect(Collectors.toMap(QuerySettingDef::name, Function.identity()));

    public static void validate(EsqlStatement statement, SettingsValidationContext ctx) {
        for (QuerySetting setting : statement.settings()) {
            QuerySettingDef<?> def = SETTINGS_BY_NAME.get(setting.name());
            if (def == null) {
                throw new ParsingException(setting.source(), "Unknown setting [" + setting.name() + "]");
            }

            if (def.snapshotOnly && ctx.isSnapshot() == false) {
                throw new ParsingException(setting.source(), "Setting [" + setting.name() + "] is only available in snapshot builds");
            }

            // def.type() can be null if the expression is not foldable, eg. see MapExpression for approximate
            if (def.type() != null && setting.value().dataType() != def.type()) {
                throw new ParsingException(setting.source(), "Setting [" + setting.name() + "] must be of type " + def.type());
            }

            if (def.type() != null && setting.value().foldable() == false) {
                throw new ParsingException(setting.source(), "Setting [" + setting.name() + "] must be a constant");
            }

            String error;
            try {
                error = def.validator().validate(setting.value(), ctx);
            } catch (Exception e) {
                throw new ParsingException("Error validating setting [" + setting.name() + "]: " + e.getMessage());
            }
            if (error != null) {
                throw new ParsingException("Error validating setting [" + setting.name() + "]: " + error);
            }
        }
    }

    /**
     * Definition of a query setting.
     *
     * @param name The name to be used when setting it in the query. E.g. {@code SET name=value}
     * @param type The allowed datatype of the setting. Used for validation and documentation. Use null to skip datatype validation.
     * @param serverlessOnly
     * @param preview
     * @param snapshotOnly
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
            Parser<T> parser,
            T defaultValue
        ) {
            this(name, type, serverlessOnly, preview, snapshotOnly, (value, rcs) -> {
                try {
                    parser.parse(value);
                    return null;
                } catch (Exception exc) {
                    return exc.getMessage();
                }
            }, parser, defaultValue);
        }

        public T parse(@Nullable Expression value) {
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
            String validate(Expression value, SettingsValidationContext ctx);
        }

        @FunctionalInterface
        public interface Parser<T> {
            /**
             * Parses an already validated literal.
             */
            T parse(Expression value);
        }
    }
}
