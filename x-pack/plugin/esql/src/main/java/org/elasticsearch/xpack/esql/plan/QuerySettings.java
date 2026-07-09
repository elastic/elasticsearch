/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan;

import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.analysis.UnmappedResolution;
import org.elasticsearch.xpack.esql.approximation.ApproximationSettings;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.MapParam;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.parser.ParsingException;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * The catalog of registered ES|QL query settings.
 *
 * <p>Each entry is one fluent declaration. {@link QuerySettingDef} carries the schema and the read API;
 * this class is a list of constants and two utility methods ({@link #validate} for the in-query SET
 * pass, {@link #resolve} for the merge step that produces an {@link ResolvedSettings}).
 *
 * <h2>Adding a new setting</h2>
 *
 * <pre>{@code
 *   public static final QuerySettingDef<String> MY_SETTING = QuerySettingDef
 *       .string("my_setting")
 *       .withDefault("foo")
 *       .withRequestBody()       // accept under settings.{my_setting}
 *       .build();
 * }</pre>
 *
 * Then add the constant to {@link #ALL} to register it. Read anywhere via
 * {@code MY_SETTING.get(resolvedSettings)}.
 */
public final class QuerySettings {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(QuerySettings.class);

    @Param(name = "project_routing", type = { "keyword" }, description = """
        Limits the scope of a [cross-project search (CPS)](/reference/query-languages/esql/esql-cross-serverless-projects.md) to \
        specific projects before query execution, based on a \
        [Lucene query expression](docs-content://explore-analyze/cross-project-search/cross-project-search-project-routing.md) \
        evaluated against project tags. Excluded projects are not queried, which can reduce cost and latency. \
        """)
    @Example(file = "from", tag = "project-routing", description = "Route a query to a specific project by alias:")
    public static final QuerySettingDef<String> PROJECT_ROUTING = QuerySettingDef.string("project_routing")
        .withServerlessOnly()
        .withPreview()
        .withValidator((value, ctx) -> ctx.crossProjectEnabled() ? null : "cross-project search not enabled")
        .withRequestBody()
        .withAliasAtRoot()
        .build();

    @Param(
        name = "time_zone",
        type = { "keyword" },
        since = "9.4+",
        description = "The default timezone to be used in the query. Defaults to UTC, and overrides the `time_zone` request parameter. "
            + "See [timezones](/reference/query-languages/esql/esql-rest.md#esql-timezones)."
    )
    @Example(file = "tbucket", tag = "set-timezone-example")
    public static final QuerySettingDef<ZoneId> TIME_ZONE = QuerySettingDef.string("time_zone", QuerySettings::parseZoneId)
        .withDefault(ZoneOffset.UTC)
        .withRequestBody()
        .withAliasAtRoot()
        .canonicalize(ZoneId::normalized)
        .build();

    @Param(name = "unmapped_fields", type = { "keyword" }, since = "9.3.0", description = """
        Determines how unmapped fields are treated.
        For a conceptual overview and use cases, refer to [Unmapped fields](/reference/query-languages/esql/esql-unmapped-fields.md).

        Possible values are:

        - `DEFAULT` : Standard ESQL queries fail when referencing unmapped fields.
        - `NULLIFY` : Treats unmapped fields as null values.
        - `LOAD` : Loads unmapped fields from the stored [`_source`](/reference/elasticsearch/mapping-reference/mapping-source-field.md)
        with type `keyword`. Or nullifies them if absent from `_source`. {applies_to}`stack: preview 9.4`

        [`PROMQL`](/reference/query-languages/esql/commands/promql.md) queries have their own specific semantics for unmapped fields.

        Special notes about the `LOAD` option:
        - `FORK`, `LOOKUP JOIN`, subqueries, and views are not yet supported anywhere in the query.
        - Referencing subfields of `flattened` parents is not supported.
        - [Full-text search functions](/reference/query-languages/esql/functions-operators/search-functions.md) are supported.
          {applies_to}`stack: preview 9.5`
          - Full-text search functions are not supported anywhere in the query. {applies_to}`stack: preview =9.4`
        - [`KNN`](/reference/query-languages/esql/functions-operators/dense-vector-functions/knn.md) on partially unmapped
          `dense_vector` fields is not yet supported.
        - Partially unmapped non-`keyword` fields can be used in expressions. If the field is mapped to a single type and there's an
          available conversion from `keyword` to that type, the implicit conversion is applied. If there's no available conversion,
          and an explicit one has not been provided by the user, values remain typed where mapped and are `null` for rows from
          indices where the field is unmapped. {applies_to}`stack: preview 9.5`
          - Partially unmapped non-`keyword` fields must be referenced inside a cast or conversion function (e.g. `::TYPE` or `TO_TYPE`),
            unless referenced in `KEEP` or `DROP`. {applies_to}`stack: preview =9.4`
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
    public static final QuerySettingDef<UnmappedResolution> UNMAPPED_FIELDS = QuerySettingDef.string(
        "unmapped_fields",
        QuerySettings::parseUnmappedResolution
    ).withDefault(UnmappedResolution.DEFAULT).withPreview().build();

    @Param(
        name = "column_metadata",
        type = { "boolean" },
        since = "9.5.0",
        description = "When enabled, column metadata is added to the `_query` response as additional `_meta` properties."
            + " Defaults to `false`. Currently, only `_meta.bucket` is added for columns corresponding to the `BUCKET` function"
            + " and contains bucket interval and unit for queries where it can be determined."
    )
    public static final QuerySettingDef<Boolean> COLUMN_METADATA = QuerySettingDef.bool("column_metadata")
        .withDefault(Boolean.FALSE)
        .withPreview()
        .withRequestBody()
        .build();

    @Param(
        name = "approximation",
        type = { "boolean", "map_param" },
        since = "9.5+, preview =9.4",
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
    public static final QuerySettingDef<ApproximationSettings> APPROXIMATION = QuerySettingDef.object(
        "approximation",
        ApproximationSettings::fromXContent,
        ApproximationSettings::parse
    )
        .withRequestBody()
        .withAliasAtRoot()
        .withReconciler((previous, current) -> new ApproximationSettings.Builder(false).merge(previous).merge(current).build())
        .streamFormat((out, value) -> value.writeTo(out), ApproximationSettings::new)
        .build();

    /**
     * The canonical, explicitly-enumerated set of all query settings. This is the single source of truth — the
     * request parser, the resolver, and telemetry all iterate this list. Add a new setting's constant here when
     * you declare it. Referencing this field initializes the class, so there is no load-order hazard.
     */
    public static final List<QuerySettingDef<?>> ALL = List.of(APPROXIMATION, COLUMN_METADATA, PROJECT_ROUTING, TIME_ZONE, UNMAPPED_FIELDS);

    private static final Map<String, QuerySettingDef<?>> BY_NAME = byName(ALL);

    // Package-private + parameterized so the duplicate-name guard is unit-testable without a JVM-global registry.
    static Map<String, QuerySettingDef<?>> byName(List<QuerySettingDef<?>> all) {
        Map<String, QuerySettingDef<?>> map = new HashMap<>();
        for (QuerySettingDef<?> def : all) {
            if (map.putIfAbsent(def.name(), def) != null) {
                throw new IllegalStateException("Duplicate query setting [" + def.name() + "]");
            }
        }
        return Map.copyOf(map);
    }

    /** All declared settings. */
    public static List<QuerySettingDef<?>> all() {
        return ALL;
    }

    /** The setting with this name, or {@code null} if no such setting is declared. */
    @Nullable
    public static QuerySettingDef<?> lookup(String name) {
        return BY_NAME.get(name);
    }

    private QuerySettings() {}

    private static ZoneId parseZoneId(String tz) {
        try {
            // Normalize so a fixed-offset zone (e.g. "UTC", "+00:00", "Z") collapses to its ZoneOffset. TIME_ZONE's
            // canonicalizer normalizes again on every write into a resolved view; this call normalizes the value at
            // the request level, before resolution, so the top-level-vs-settings{} duplicate check compares canonical
            // forms ("UTC" and "Z" are the same zone). Non-fixed zones (e.g. "Europe/Madrid") are returned unchanged.
            return ZoneId.of(tz).normalized();
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid time zone [" + tz + "]");
        }
    }

    private static UnmappedResolution parseUnmappedResolution(String value) {
        try {
            return UnmappedResolution.valueOf(value.toUpperCase(Locale.ROOT));
        } catch (Exception e) {
            throw new IllegalArgumentException(
                "Invalid unmapped_fields resolution [" + value + "], must be one of " + Arrays.toString(UnmappedResolution.values())
            );
        }
    }

    /**
     * Validates the in-query SETs. An unknown key is a typo the user can act on, so it fails loudly with a
     * {@link ParsingException} — same as the request-body surface. A known-but-deprecated key is accepted with
     * a deprecation warning (see {@link #warnIfDeprecated}). Type and availability failures also throw early.
     */
    public static void validate(EsqlStatement statement, SettingsValidationContext ctx) {
        if (statement.settings() == null) {
            return;
        }
        for (QuerySetting setting : statement.settings()) {
            QuerySettingDef<?> def = lookup(setting.name());
            if (def == null) {
                throw new ParsingException(setting.source(), "Unknown setting [" + setting.name() + "]");
            }
            warnIfDeprecated(def);
            if (def.snapshotOnly() && ctx.isSnapshot() == false) {
                throw new ParsingException(setting.source(), "Setting [" + setting.name() + "] is only available in snapshot builds");
            }
            if (def.type() != null && setting.value().dataType() != def.type()) {
                throw new ParsingException(setting.source(), "Setting [" + setting.name() + "] must be of type " + def.type());
            }
            if (def.type() != null && setting.value().foldable() == false) {
                throw new ParsingException(setting.source(), "Setting [" + setting.name() + "] must be a constant");
            }
            runTypedValidator(def, setting, ctx);
        }
    }

    /**
     * Emits a deprecation warning if {@code def} is deprecated. Called from every settings surface (in-query
     * {@code SET}, the request body, and the legacy root aliases) so a deprecated setting warns wherever it is
     * supplied, while still being resolved and applied. Routed through {@link DeprecationLogger} (not a bare
     * response header) so it also lands in the throttled operator-facing deprecation trail, matching how the rest
     * of the module deprecates user-supplied knobs (e.g. the datasource {@code auth} aliases).
     */
    public static void warnIfDeprecated(QuerySettingDef<?> def) {
        if (def.deprecationMessage() != null) {
            deprecationLogger.warn(
                DeprecationCategory.API,
                "esql_setting_" + def.name(),
                "Setting [{}] is deprecated: {}",
                def.name(),
                def.deprecationMessage()
            );
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static void runTypedValidator(QuerySettingDef def, QuerySetting setting, SettingsValidationContext ctx) {
        Object parsed;
        try {
            parsed = def.readFromExpression(setting.value());
        } catch (Exception e) {
            throw new ParsingException(setting.source(), "Error validating setting [" + setting.name() + "]: " + e.getMessage());
        }
        String error;
        try {
            error = def.runValidator(parsed, ctx);
        } catch (Exception e) {
            throw new ParsingException(setting.source(), "Error validating setting [" + setting.name() + "]: " + e.getMessage());
        }
        if (error != null) {
            throw new ParsingException(setting.source(), "Error validating setting [" + setting.name() + "]: " + error);
        }
    }

    /**
     * Folds {@code registry default < request body < in-query SET} into a single {@link ResolvedSettings},
     * applying each setting's {@link QuerySettingDef#reconciler()} at every step.
     */
    public static ResolvedSettings resolve(
        Map<QuerySettingDef<?>, Object> requestParams,
        @Nullable EsqlStatement statement,
        SettingsValidationContext ctx
    ) {
        Map<QuerySettingDef<?>, Object> resolved = new HashMap<>();
        for (QuerySettingDef<?> def : all()) {
            resolveSingle(def, requestParams, statement, ctx, resolved);
        }
        return new ResolvedSettings(resolved);
    }

    @SuppressWarnings("unchecked")
    private static <T> void resolveSingle(
        QuerySettingDef<T> def,
        Map<QuerySettingDef<?>, Object> requestParams,
        @Nullable EsqlStatement statement,
        SettingsValidationContext ctx,
        Map<QuerySettingDef<?>, Object> resolved
    ) {
        T value = def.defaultValue();
        boolean userSupplied = false;

        if (requestParams.containsKey(def)) {
            T requestValue = (T) requestParams.get(def);
            if (requestValue != null) {
                value = def.reconciler().reconcile(value, requestValue);
                userSupplied = true;
            }
        }

        if (statement != null) {
            Expression querySetExpression = statement.setting(def.name());
            if (querySetExpression != null) {
                T querySetValue = def.readFromExpression(querySetExpression);
                value = def.reconciler().reconcile(value, querySetValue);
                userSupplied = true;
            }
        }

        // Body-supplied snapshot-only settings bypass the parse-time gate in validate() (which only sees SET).
        // SET-supplied ones can't reach here in non-snapshot — validate() rejected them with a ParsingException.
        if (def.snapshotOnly() && ctx.isSnapshot() == false && userSupplied) {
            throw new VerificationException("Setting [" + def.name() + "] is only available in snapshot builds");
        }

        if (value != null) {
            // Validate only a value the user actually supplied — a registry default must never fail a query, and an
            // environment-gated validator (e.g. project_routing's cross-project check) would otherwise reject every
            // query in the wrong environment. Wrap a throwing validator as a 400, matching the SET path (runTypedValidator).
            if (userSupplied) {
                String error;
                try {
                    error = def.runValidator(value, ctx);
                } catch (Exception e) {
                    throw new VerificationException("Error validating setting [" + def.name() + "]: " + e.getMessage());
                }
                if (error != null) {
                    throw new VerificationException("Error validating setting [" + def.name() + "]: " + error);
                }
            }
            resolved.put(def, def.canonicalize(value));
        }
    }

    /**
     * The registered settings whose availability matches the supplied snapshot/serverless environment.
     */
    public static List<QuerySettingDef<?>> applicableIn(boolean isSnapshot, boolean isServerless) {
        List<QuerySettingDef<?>> out = new ArrayList<>();
        for (QuerySettingDef<?> def : all()) {
            if (def.snapshotOnly() && isSnapshot == false) {
                continue;
            }
            if (def.serverlessOnly() && isServerless == false) {
                continue;
            }
            out.add(def);
        }
        return out;
    }
}
