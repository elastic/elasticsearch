/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan;

import org.elasticsearch.common.logging.HeaderWarning;
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
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

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
 *       .withRequestBody();      // accept under settings.{my_setting}
 * }</pre>
 *
 * Read anywhere via {@code MY_SETTING.get(envelope)}.
 */
public final class QuerySettings {

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
        .build();

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
    public static final QuerySettingDef<UnmappedResolution> UNMAPPED_FIELDS = QuerySettingDef.string(
        "unmapped_fields",
        QuerySettings::parseUnmappedResolution
    ).withDefault(UnmappedResolution.DEFAULT).withPreview().build();

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
    public static final QuerySettingDef<ApproximationSettings> APPROXIMATION = QuerySettingDef.object(
        "approximation",
        ApproximationSettings::fromXContent,
        ApproximationSettings::parse
    )
        .withPreview()
        .withRequestBody()
        .withAliasAtRoot()
        .withReconciler((previous, current) -> new ApproximationSettings.Builder(false).merge(previous).merge(current).build())
        .withStreamFormat((out, value) -> value.writeTo(out), ApproximationSettings::new)
        .build();

    private QuerySettings() {}

    private static ZoneId parseZoneId(String tz) {
        try {
            return ZoneId.of(tz);
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
     * Validates the in-query SETs. Unknown keys emit a deprecation header and are skipped (the in-query
     * surface is forgiving of typos); other failures throw {@link ParsingException} early.
     */
    public static void validate(EsqlStatement statement, SettingsValidationContext ctx) {
        if (statement.settings() == null) {
            return;
        }
        for (QuerySetting setting : statement.settings()) {
            QuerySettingDef<?> def = QuerySettingDef.lookup(setting.name());
            if (def == null) {
                HeaderWarning.addWarning("Unknown ES|QL setting [" + setting.name() + "] — ignored");
                continue;
            }
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
        Set<String> consumed = new HashSet<>();
        for (QuerySettingDef<?> def : QuerySettingDef.all()) {
            resolveSingle(def, requestParams, statement, ctx, resolved, consumed);
        }
        return new ResolvedSettings(resolved, consumed);
    }

    @SuppressWarnings("unchecked")
    private static <T> void resolveSingle(
        QuerySettingDef<T> def,
        Map<QuerySettingDef<?>, Object> requestParams,
        @Nullable EsqlStatement statement,
        SettingsValidationContext ctx,
        Map<QuerySettingDef<?>, Object> resolved,
        Set<String> consumed
    ) {
        T value = def.defaultValue();
        boolean userSupplied = false;

        if (requestParams.containsKey(def)) {
            T requestValue = (T) requestParams.get(def);
            if (requestValue != null) {
                value = def.reconciler().reconcile(value, requestValue);
                consumed.add(def.name());
                userSupplied = true;
            }
        }

        if (statement != null) {
            Expression querySetExpression = statement.setting(def.name());
            if (querySetExpression != null) {
                T querySetValue = def.readFromExpression(querySetExpression);
                value = def.reconciler().reconcile(value, querySetValue);
                consumed.add(def.name());
                userSupplied = true;
            }
        }

        // Body-supplied snapshot-only settings bypass the parse-time gate in validate() (which only sees SET).
        // SET-supplied ones can't reach here in non-snapshot — validate() rejected them with a ParsingException.
        if (def.snapshotOnly() && ctx.isSnapshot() == false && userSupplied) {
            throw new VerificationException("Setting [" + def.name() + "] is only available in snapshot builds");
        }

        if (value != null) {
            String error = def.runValidator(value, ctx);
            if (error != null) {
                throw new VerificationException("Error validating setting [" + def.name() + "]: " + error);
            }
            resolved.put(def, value);
        }
    }

    /**
     * The registered settings whose names match the supplied snapshot/serverless environment.
     */
    public static List<QuerySettingDef<?>> applicableIn(boolean isSnapshot, boolean isServerless) {
        List<QuerySettingDef<?>> out = new ArrayList<>();
        for (QuerySettingDef<?> def : QuerySettingDef.all()) {
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
