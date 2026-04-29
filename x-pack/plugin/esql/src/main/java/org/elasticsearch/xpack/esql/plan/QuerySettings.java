/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan;

import org.elasticsearch.core.Nullable;
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

    @Param(name = "project_routing", type = { "keyword" }, description = """
        Limits the scope of a [cross-project search (CPS)](/reference/query-languages/esql/esql-cross-serverless-projects.md) to \
        specific projects before query execution, based on a \
        [Lucene query expression](docs-content://explore-analyze/cross-project-search/cross-project-search-project-routing.md) \
        evaluated against project tags. Excluded projects are not queried, which can reduce cost and latency. \
        """)
    @Example(file = "from", tag = "project-routing", description = "Route a query to a specific project by alias:")
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

    @Param(name = "unmapped_fields", type = { "keyword" }, since = "9.3.0", description = """
        Determines how unmapped fields are treated. Possible values are:

        - `DEFAULT` : Standard ESQL queries fail when referencing unmapped fields, while other query types (e.g. PromQL)
        may treat them differently.
        - `NULLIFY` : Treats unmapped fields as null values.
        - `LOAD` : Loads unmapped fields from the stored [`_source`](/reference/elasticsearch/mapping-reference/mapping-source-field.md)
        with type `keyword`. Or nullifies them if absent from `_source`. {applies_to}`stack: preview 9.4`

        An `unmapped field` is a field referenced in a query that does not exist in the mapping of the index being queried. When querying
        multiple indices, a field is considered `partially unmapped` if it exists in the mapping of some indices but not others.

        Special notes about the `LOAD` option:
        - `PromQL`, `FORK`, `LOOKUP JOIN`, subqueries, views, and full-text search functions are not yet supported anywhere in the query.
        - Referencing subfields of `flattened` parents is not supported.
        - Referencing partially unmapped non-keyword fields must be inside a cast or a conversion function (e.g. `::TYPE` or `TO_TYPE`),
        unless referenced in a `KEEP` or `DROP`.
        """)
    @Example(file = "unmapped-nullify", tag = "unmapped-nullify-simple-keep", description = """
        Field `unmapped_message` is not mapped; it doesn't appear in the mapping of index `partial_mapping_sample_data`. It appears,
        however, in the stored `_source` of all documents in this index.

        The `NULLIFY` option will treat this field as `null`.
        """)
    @Example(file = "unmapped-load", tag = "unmapped-load-sample", description = """
        Field `unmapped_message` is not mapped; it doesn't appear in the mapping of index `partial_mapping_sample_data`. It appears,
        however, in the stored `_source` of all documents in this index.

        The `LOAD` option will load this field from `_source` and treat it like a `keyword` type field.
        """)
    public static final QuerySettingDef<UnmappedResolution> UNMAPPED_FIELDS = new QuerySettingDef<>(
        "unmapped_fields",
        DataType.KEYWORD,
        false,
        true,
        false,
        (value) -> {
            String resolution = Foldables.stringLiteralValueOf(value, "Unexpected value");
            try {
                return UnmappedResolution.valueOf(resolution.toUpperCase(Locale.ROOT));
            } catch (Exception exc) {
                throw new IllegalArgumentException(
                    "Invalid unmapped_fields resolution [" + value + "], must be one of " + Arrays.toString(UnmappedResolution.values())
                );
            }
        },
        UnmappedResolution.DEFAULT
    );

    @Param(
        name = "approximation",
        type = { "boolean", "map_param" },
        since = "9.4.0",
        description = "Enables [query approximation](/reference/query-languages/esql/esql-query-approximation.md) if possible for the "
            + "query. A boolean value `false` (default) disables query approximation and `true` enables it with "
            + "default settings. Map values enable query approximation with custom settings."
    )
    @MapParam(
        name = "approximation",
        params = {
            @MapParam.MapParamEntry(
                name = "rows",
                type = { "integer" },
                description = "Number of sampled rows used for approximating the query. "
                    + "Must be at least 10,000. Null uses the system default."
            ),
            @MapParam.MapParamEntry(
                name = "confidence_level",
                type = { "double" },
                description = "Confidence level of the computed confidence intervals. "
                    + "Default is 0.90. Null disables computing confidence intervals."
            ) }
    )
    @Example(file = "approximation", tag = "approximationBooleanForDocs", description = "Approximate the sum using default settings.")
    @Example(file = "approximation", tag = "approximationMapForDocs", description = "Approximate the median based on 10,000 rows.")
    public static final QuerySettingDef<ApproximationSettings> APPROXIMATION = new QuerySettingDef<>(
        "approximation",
        null,
        false,
        true,
        false,
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
