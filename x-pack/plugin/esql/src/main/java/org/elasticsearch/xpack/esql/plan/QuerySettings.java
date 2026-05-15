/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan;

import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.esql.analysis.UnmappedResolution;
import org.elasticsearch.xpack.esql.approximation.ApproximationSettings;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Foldables;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.MapParam;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.parser.ParsingException;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
    ).withRequestParameter(XContentParser::text).withAdditionalBinding(RequestBodyBinding.atRoot("project_routing"));

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
    ).withRequestParameter(p -> ZoneId.of(p.text())).withAdditionalBinding(RequestBodyBinding.atRoot("time_zone"));

    @Param(name = "unmapped_fields", type = { "keyword" }, since = "9.3.0", description = """
        Determines how unmapped fields are treated. Possible values are:

        - `DEFAULT` : Standard ESQL queries fail when referencing unmapped fields.
        - `NULLIFY` : Treats unmapped fields as null values.
        - `LOAD` : Loads unmapped fields from the stored [`_source`](/reference/elasticsearch/mapping-reference/mapping-source-field.md)
        with type `keyword`. Or nullifies them if absent from `_source`. {applies_to}`stack: preview 9.4`

        An `unmapped field` is a field referenced in a query that does not exist in the mapping of the index being queried. When querying
        multiple indices, a field is considered `partially unmapped` if it exists in the mapping of some indices but not others.

        [`PROMQL`](/reference/query-languages/esql/commands/promql.md) queries have their own specific semantics for unmapped fields.

        Special notes about the `LOAD` option:
        - `FORK`, `LOOKUP JOIN`, subqueries, views, and full-text search functions are not yet supported anywhere in the query.
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
    ).withRequestParameter(ApproximationSettings::fromXContent)
        .withAdditionalBinding(RequestBodyBinding.atRoot("approximation"))
        // Approximation merges field-by-field rather than last-wins, so a query SET that only sets `confidence_level`
        // does not erase a request-supplied `rows` value.
        .withCombiner((lower, higher) -> new ApproximationSettings.Builder(false).merge(lower).merge(higher).build());

    public static final Map<String, QuerySettingDef<?>> SETTINGS_BY_NAME = Stream.of(
        UNMAPPED_FIELDS,
        PROJECT_ROUTING,
        TIME_ZONE,
        APPROXIMATION
    ).collect(Collectors.toMap(QuerySettingDef::name, Function.identity()));

    /**
     * Resolves the effective value of every registered setting by folding the precedence-ordered sources
     * latest-wins per setting:
     *
     * <pre>
     *     registry default  &lt;  request parameter  &lt;  query SET
     * </pre>
     *
     * The fold uses each setting's {@link QuerySettingDef#combiner()}; the default is "higher precedence wins"
     * but settings like {@code approximation} use a custom field-level merge.
     *
     * <p>The returned {@link EffectiveSettings} additionally reports which SET keys had at least one source
     * contribute a value, intended for telemetry.
     *
     * <p>Validation of query SET values runs separately via {@link #validate(EsqlStatement, SettingsValidationContext)}
     * before this resolver is called. Body-supplied values are validated at parse time by the per-setting
     * {@link QuerySettingDef#jsonValueParser()}. Context-dependent runtime checks (e.g. {@code crossProjectEnabled})
     * continue to live in the consumer code paths.
     *
     * @param requestParams body-supplied values keyed by registry definition, as accumulated on {@code EsqlQueryRequest}
     * @param statement     parsed statement carrying the in-query SETs (may be {@code null})
     * @param ctx           validation context — currently unused by this resolver but reserved for future use
     */
    public static EffectiveSettings resolve(
        Map<QuerySettingDef<?>, Object> requestParams,
        @Nullable EsqlStatement statement,
        SettingsValidationContext ctx
    ) {
        Map<QuerySettingDef<?>, Object> resolved = new java.util.HashMap<>();
        java.util.Set<String> consumed = new java.util.HashSet<>();
        for (QuerySettingDef<?> def : SETTINGS_BY_NAME.values()) {
            resolveSingle(def, requestParams, statement, resolved, consumed);
        }
        return new EffectiveSettings(resolved, consumed);
    }

    @SuppressWarnings("unchecked")
    private static <T> void resolveSingle(
        QuerySettingDef<T> def,
        Map<QuerySettingDef<?>, Object> requestParams,
        @Nullable EsqlStatement statement,
        Map<QuerySettingDef<?>, Object> resolved,
        java.util.Set<String> consumed
    ) {
        T value = def.defaultValue();

        if (requestParams.containsKey(def)) {
            T requestValue = (T) requestParams.get(def);
            if (requestValue != null) {
                value = def.combiner().combine(value, requestValue);
                consumed.add(def.name());
            }
        }

        if (statement != null && statement.settings() != null) {
            Expression querySetExpression = statement.setting(def.name());
            if (querySetExpression != null) {
                T querySetValue = def.parse(querySetExpression);
                value = def.combiner().combine(value, querySetValue);
                consumed.add(def.name());
            }
        }

        if (value != null) {
            resolved.put(def, value);
        }
    }

    public static void validate(EsqlStatement statement, SettingsValidationContext ctx) {
        for (QuerySetting setting : statement.settings()) {
            QuerySettingDef<?> def = SETTINGS_BY_NAME.get(setting.name());
            if (def == null) {
                // Unknown SET keys in a query string are forgiven: emit a deprecation header and skip. The user is typing
                // and an unknown key may be a typo or a setting that does not exist on this version. Strict rejection
                // remains for the body surface (settings.{}), which is tooling-facing.
                HeaderWarning.addWarning("Unknown ES|QL setting [" + setting.name() + "] — ignored");
                continue;
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
                throw new ParsingException(setting.source(), "Error validating setting [" + setting.name() + "]: " + e.getMessage());
            }
            if (error != null) {
                throw new ParsingException(setting.source(), "Error validating setting [" + setting.name() + "]: " + error);
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
     * @param requestParameterExposed Whether the setting can also be supplied via the {@code _query} request body. SET-only by default;
     *                                opt in with {@link #withRequestParameter(JsonValueParser)}.
     * @param requestBodyName Optional override for the name used inside the body {@code settings} object and on every additional
     *                        binding that did not specify its own name. {@code null} means use the SET key {@link #name}.
     * @param additionalBindings Zero or more extra request-body locations that alias this setting. Permanent first-class alternates
     *                           to {@code settings.<name>}, not deprecated.
     * @param jsonValueParser Reads the typed value from an {@link XContentParser} positioned at this setting's value. Required when
     *                        {@code requestParameterExposed = true}; must be {@code null} when SET-only.
     * @param combiner Combines a lower-precedence and a higher-precedence value into the resolved value. Defaults to
     *                 last-wins; a setting can override (e.g., {@code approximation} uses field-level merge).
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
        T defaultValue,
        boolean requestParameterExposed,
        @Nullable String requestBodyName,
        List<RequestBodyBinding> additionalBindings,
        @Nullable JsonValueParser<T> jsonValueParser,
        Combiner<T> combiner
    ) {

        public QuerySettingDef {
            if (requestParameterExposed == false) {
                if (requestBodyName != null) {
                    throw new IllegalArgumentException("requestBodyName requires requestParameterExposed=true for setting [" + name + "]");
                }
                if (additionalBindings.isEmpty() == false) {
                    throw new IllegalArgumentException(
                        "additionalBindings require requestParameterExposed=true for setting [" + name + "]"
                    );
                }
                if (jsonValueParser != null) {
                    throw new IllegalArgumentException("jsonValueParser requires requestParameterExposed=true for setting [" + name + "]");
                }
            } else if (jsonValueParser == null) {
                throw new IllegalArgumentException(
                    "jsonValueParser is required when requestParameterExposed=true for setting [" + name + "]"
                );
            }
            if (combiner == null) {
                throw new IllegalArgumentException("combiner is required for setting [" + name + "]");
            }
            additionalBindings = List.copyOf(additionalBindings);
        }

        /**
         * Convenience constructor: defaults to SET-only ({@code requestParameterExposed = false}).
         * Opt in to body exposure with {@link #withRequestParameter(JsonValueParser)}.
         */
        public QuerySettingDef(
            String name,
            DataType type,
            boolean serverlessOnly,
            boolean preview,
            boolean snapshotOnly,
            Validator validator,
            Parser<T> parser,
            T defaultValue
        ) {
            this(
                name,
                type,
                serverlessOnly,
                preview,
                snapshotOnly,
                validator,
                parser,
                defaultValue,
                false,
                null,
                List.of(),
                null,
                lastWins()
            );
        }

        /**
         * Convenience constructor with a default validator that delegates to the parser. SET-only.
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
            }, parser, defaultValue, false, null, List.of(), null, lastWins());
        }

        /**
         * Returns a copy of this definition with {@code requestParameterExposed = true} and the supplied JSON value parser.
         * The setting becomes reachable from the request body at {@code settings.<name>}.
         */
        public QuerySettingDef<T> withRequestParameter(JsonValueParser<T> jsonValueParser) {
            return new QuerySettingDef<>(
                name,
                type,
                serverlessOnly,
                preview,
                snapshotOnly,
                validator,
                parser,
                defaultValue,
                true,
                requestBodyName,
                additionalBindings,
                jsonValueParser,
                combiner
            );
        }

        /**
         * Returns a copy of this definition with {@code requestParameterExposed = true}, a JSON value parser, and the
         * body parameter renamed. The override applies uniformly to {@code settings.<paramName>} and to every additional
         * binding that did not specify its own name.
         */
        public QuerySettingDef<T> withRequestParameter(String paramName, JsonValueParser<T> jsonValueParser) {
            return new QuerySettingDef<>(
                name,
                type,
                serverlessOnly,
                preview,
                snapshotOnly,
                validator,
                parser,
                defaultValue,
                true,
                paramName,
                additionalBindings,
                jsonValueParser,
                combiner
            );
        }

        /**
         * Returns a copy of this definition with an additional request-body binding.
         * Requires {@link #withRequestParameter(JsonValueParser)} to have been called first.
         */
        public QuerySettingDef<T> withAdditionalBinding(RequestBodyBinding binding) {
            if (requestParameterExposed == false) {
                throw new IllegalStateException(
                    "withAdditionalBinding requires withRequestParameter(...) first for setting [" + name + "]"
                );
            }
            List<RequestBodyBinding> bindings = new ArrayList<>(additionalBindings);
            bindings.add(binding);
            return new QuerySettingDef<>(
                name,
                type,
                serverlessOnly,
                preview,
                snapshotOnly,
                validator,
                parser,
                defaultValue,
                requestParameterExposed,
                requestBodyName,
                bindings,
                jsonValueParser,
                combiner
            );
        }

        /**
         * Returns a copy of this definition with a custom combiner. Use when last-wins is not the right merge rule.
         */
        public QuerySettingDef<T> withCombiner(Combiner<T> combiner) {
            return new QuerySettingDef<>(
                name,
                type,
                serverlessOnly,
                preview,
                snapshotOnly,
                validator,
                parser,
                defaultValue,
                requestParameterExposed,
                requestBodyName,
                additionalBindings,
                jsonValueParser,
                combiner
            );
        }

        public T parse(@Nullable Expression value) {
            if (value == null) {
                return defaultValue;
            }
            return parser.parse(value);
        }

        /**
         * The name used to address this setting on the body surface (inside {@code settings.{}} and as the default for
         * additional bindings). Equal to {@link #name} unless an override was provided via
         * {@link #withRequestParameter(String, JsonValueParser)}.
         */
        public String canonicalRequestBodyName() {
            return requestBodyName != null ? requestBodyName : name;
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

        /**
         * Parses a setting value out of a request body {@link XContentParser} positioned on the value token.
         */
        @FunctionalInterface
        public interface JsonValueParser<T> {
            T parse(XContentParser parser) throws IOException;
        }

        /**
         * Combines a lower-precedence value with a higher-precedence value into a single resolved value. Both inputs may
         * be {@code null} (meaning "not supplied"). The default ({@link #lastWins()}) returns the higher-precedence value
         * if non-null, otherwise the lower-precedence value.
         */
        @FunctionalInterface
        public interface Combiner<T> {
            T combine(@Nullable T lower, @Nullable T higher);
        }

        /** Default combiner: higher precedence wins, unless null. */
        public static <T> Combiner<T> lastWins() {
            return (lower, higher) -> higher != null ? higher : lower;
        }
    }

    /**
     * Location of a setting in the {@code _query} request body, outside the canonical {@code settings.{}} block.
     * {@code parentPath} is a dotted JSON path to the parent object ({@code ""} = top level). {@code name} is the field name
     * inside that parent. Permanent first-class alternates to the canonical path; not deprecated.
     */
    public record RequestBodyBinding(String parentPath, String name) {

        public RequestBodyBinding {
            if (parentPath == null) {
                throw new IllegalArgumentException("parentPath must not be null (use \"\" for root)");
            }
            if (name == null || name.isEmpty()) {
                throw new IllegalArgumentException("name must not be null or empty");
            }
        }

        /** Convenience: a binding directly at the top level of the request body. */
        public static RequestBodyBinding atRoot(String name) {
            return new RequestBodyBinding("", name);
        }

        /** True if this binding sits at the top level of the request body. */
        public boolean isAtRoot() {
            return parentPath.isEmpty();
        }
    }
}
