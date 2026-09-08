/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan;

import org.elasticsearch.Build;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
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
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/**
 * The catalog of registered ES|QL query settings.
 *
 * <p>Each entry is one fluent declaration. {@link QuerySettingDef} carries the schema and the read API;
 * this class is a list of constants plus the entry points that use them — {@link #validate} for the in-query SET
 * pass, {@link #resolve} for the merge that produces a {@link ResolvedSettings}, and the cluster-setting
 * registration and warning.
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

    private static final Logger logger = LogManager.getLogger(QuerySettings.class);

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
            + "See [timezones](/reference/query-languages/esql/esql-rest.md#esql-timezones).\n\n"
            + "The default itself is configurable. If a query does not specify a timezone, the "
            + "`esql.query.settings.time_zone` cluster setting supplies it. If that cluster setting is not configured "
            + "either, the timezone is UTC. "
            + "{applies_to}`{\"stack\": \"ga 9.6+\", \"serverless\": \"unavailable\"}`"
    )
    @Example(file = "tbucket", tag = "set-timezone-example")
    public static final QuerySettingDef<ZoneId> TIME_ZONE = QuerySettingDef.string("time_zone", QuerySettings::parseZoneId)
        .withDefault(ZoneOffset.UTC)
        .withClusterDefault()
        .withRequestBody()
        .withAliasAtRoot()
        .canonicalize(ZoneId::normalized)
        .build();

    // LOAD_ALL is deliberately absent from this description: it is snapshot-only, and there is no mechanism to hold
    // docs back for a snapshot-only value of an already-released setting. Document it once it ships.
    @Param(name = "unmapped_fields", type = { "keyword" }, since = "preview 9.3-9.4, ga 9.5+", description = """
        Determines how unmapped fields are treated.
        For a conceptual overview and use cases, including performance considerations, refer to
        [Unmapped fields](/reference/query-languages/esql/esql-unmapped-fields.md).

        Possible values are:

        - `DEFAULT` : Standard ESQL queries fail when referencing unmapped fields.
        - `NULLIFY` : Treats referenced unmapped fields as null values. Fully unmapped fields that are never mentioned do not
          appear in the output.
        - `LOAD` : Loads referenced fully unmapped fields from the stored
          [`_source`](/reference/elasticsearch/mapping-reference/mapping-source-field.md) with type `keyword`. Or nullifies them if
          absent from `_source`. Also loads partially mapped fields from `_source` where they are unmapped.
        {applies_to}`stack: preview =9.4, ga 9.5+`

        [`PROMQL`](/reference/query-languages/esql/commands/promql.md) queries have their own specific semantics for unmapped fields.

        Special notes about the `LOAD` option:
        - [`PROMQL`](/reference/query-languages/esql/commands/promql.md) is not supported with `LOAD`.
        - Referencing subfields of `flattened` parents is not supported.
        - [Full-text search functions](/reference/query-languages/esql/functions-operators/search-functions.md) are supported,
          although unmapped fields cannot be loaded without an explicit invocation of `to_text`.
          {applies_to}`stack: ga 9.5+`
          - Full-text search functions are not supported anywhere in the query. {applies_to}`stack: preview =9.4`
        - Partially unmapped non-`keyword` fields can be used in expressions. If the field is mapped to a single type and there's an
          available conversion from `keyword` to that type, the implicit conversion is applied. If there's no available conversion
          (for example `text`, `aggregate_metric_double`, or `dense_vector`), and an explicit one has not been provided by the user,
          values retain the mapped type but are `null` for rows from indices where the field is unmapped.
          {applies_to}`stack: ga 9.5+`
          - Partially unmapped non-`keyword` fields must be referenced inside a cast or conversion function (e.g. `::TYPE` or
            `TO_TYPE`), unless referenced in `KEEP` or `DROP`. {applies_to}`stack: preview =9.4`

        The default itself is configurable. If a query does not specify a value, the
        `esql.query.settings.unmapped_fields` cluster setting supplies it. If that cluster setting is not configured
        either, the value is `DEFAULT`.
        {applies_to}`{"stack": "ga 9.6+", "serverless": "unavailable"}`
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
    )
        .withValidator(
            (value, ctx) -> value == UnmappedResolution.LOAD_ALL && ctx.isSnapshot() == false
                ? "unmapped_fields value [LOAD_ALL] requires a snapshot build"
                : null
        )
        .withDefault(UnmappedResolution.DEFAULT)
        .withClusterDefault()
        .build();

    @Param(
        name = "column_metadata",
        type = { "boolean" },
        since = "9.5.0",
        description = "When enabled, column metadata is added to the `_query` response as additional `_meta` properties."
            + " Defaults to `false`. Currently, only `_meta.bucket` is added for columns corresponding to the `BUCKET` function"
            + " and contains bucket interval and unit for queries where it can be determined.\n\n"
            + "The default itself is configurable. If a query does not specify a value, the "
            + "`esql.query.settings.column_metadata` cluster setting supplies it. If that cluster setting is not "
            + "configured either, the value is `false`. "
            + "{applies_to}`{\"stack\": \"ga 9.6+\", \"serverless\": \"unavailable\"}`"
    )
    public static final QuerySettingDef<Boolean> COLUMN_METADATA = QuerySettingDef.bool("column_metadata")
        .withDefault(Boolean.FALSE)
        .withClusterDefault()
        .withPreview()
        .withRequestBody()
        .build();

    @Param(
        name = "approximation",
        type = { "boolean", "map_param" },
        since = "9.5+, preview =9.4",
        description = "Enables [query approximation](/reference/query-languages/esql/esql-query-approximation.md) if possible for the "
            + "query. A boolean value `false` (default) disables query approximation and `true` enables it with "
            + "default settings. Map values enable query approximation with custom settings.\n\n"
            + "The default itself is configurable. If a query does not specify a value, the "
            + "`esql.query.settings.approximation` cluster setting supplies it. If that cluster setting is not "
            + "configured either, query approximation is off. Enabling it cluster-wide requires an Enterprise "
            + "license; without one the cluster default does not apply and queries run exactly. "
            + "{applies_to}`{\"stack\": \"ga 9.6+\", \"serverless\": \"unavailable\"}`"
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
        .withClusterDefault("false")
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

    /**
     * The cluster settings derived from the registry — what {@code EsqlPlugin.getSettings()} registers, and what
     * {@link #watchClusterDefaults} watches. A setting without an operator default contributes nothing, so its key
     * stays unknown and a typo is rejected. Public only because the plugin is in another package.
     */
    public static List<Setting<?>> clusterSettings() {
        List<Setting<?>> out = new ArrayList<>();
        for (QuerySettingDef<?> def : all()) {
            Setting<?> clusterSetting = def.clusterSetting();
            if (clusterSetting != null) {
                out.add(clusterSetting);
            }
        }
        return out;
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
            throw new IllegalArgumentException(invalidUnmappedResolutionMessage(value, Build.current().isSnapshot()));
        }
    }

    /**
     * Parsing runs before the snapshot-only validator of {@link #UNMAPPED_FIELDS}, so this message is what a user of a production build
     * sees for a typo. It must not advertise {@link UnmappedResolution#LOAD_ALL}, which that build rejects.
     */
    static String invalidUnmappedResolutionMessage(String value, boolean snapshotBuild) {
        List<UnmappedResolution> available = Arrays.stream(UnmappedResolution.values())
            .filter(resolution -> snapshotBuild || resolution.loadsAllUnmappedFields() == false)
            .toList();
        return "Invalid unmapped_fields resolution [" + value + "], must be one of " + available;
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
     * Folds {@code registry default < cluster < request body < in-query SET} into a single {@link ResolvedSettings},
     * applying each setting's {@link QuerySettingDef#reconciler()} at every step. The chain is ordered by whose
     * decision a value is: the product's, the operator's, the calling application's, the query author's.
     *
     * @param clusterState the cluster-state settings, {@link Settings#EMPTY} for a caller with no cluster context
     * @param nodeSettings this node's settings, which carry any {@code elasticsearch.yml} value
     */
    public static ResolvedSettings resolve(
        Settings clusterState,
        Settings nodeSettings,
        Map<QuerySettingDef<?>, Object> requestParams,
        @Nullable EsqlStatement statement,
        SettingsValidationContext ctx
    ) {
        return resolve(all(), clusterState, nodeSettings, requestParams, statement, ctx);
    }

    /**
     * Log any operator default this node cannot use. Resolution falls back to the built-in default for such a value
     * rather than failing queries, so without this the operator would have no signal at all.
     *
     * @param effectiveSettings the settings to read operator values from — node and cluster-state already merged and
     *     filtered to these keys on the settings-update path, or cluster-state alone on the license-listener path
     * @param nodeSettings the {@code elasticsearch.yml} layer, or {@link Settings#EMPTY} when {@code effectiveSettings}
     *     already contains it. Both arms read the same view, so they cannot report different sets.
     * @param approximationLicensed whether approximation is licensed; supplied as a predicate rather than by importing
     *     the license checker, so this package keeps knowing only about settings. Must not record feature usage.
     */
    public static void warnUnusableClusterDefaults(
        Settings effectiveSettings,
        Settings nodeSettings,
        BooleanSupplier approximationLicensed
    ) {
        for (QuerySettingDef<?> def : all()) {
            String error = def.clusterValueError(effectiveSettings, nodeSettings);
            if (error != null) {
                logger.warn(
                    "Cluster setting [{}{}] is configured but not usable on this cluster and is being ignored; "
                        + "queries fall back to the built-in default. Reason: {}",
                    QuerySettingDef.CLUSTER_SETTING_PREFIX,
                    def.name(),
                    error
                );
            }
        }
        // The license is a second way an operator default becomes unusable, and it is invisible to clusterValueError:
        // the value is valid, it is the entitlement that comes and goes. Checked here so both the settings-update path
        // and the license-transition path report the same set of unusable defaults through one implementation.
        if (approximationLicensed.getAsBoolean() == false
            && ApproximationSettings.isOn(APPROXIMATION.effectiveDefault(effectiveSettings, nodeSettings))) {
            logger.warn(
                "Cluster setting [{}{}] is configured but this cluster's license does not permit approximation; "
                    + "queries that did not ask for it run exactly. A query that asks for it explicitly still fails.",
                QuerySettingDef.CLUSTER_SETTING_PREFIX,
                APPROXIMATION.name()
            );
        }
    }

    // Parameterized over the registry so the fold is testable against a purpose-built setting, as byName(List) is.
    static ResolvedSettings resolve(
        List<QuerySettingDef<?>> defs,
        Settings clusterState,
        Settings nodeSettings,
        Map<QuerySettingDef<?>, Object> requestParams,
        @Nullable EsqlStatement statement,
        SettingsValidationContext ctx
    ) {
        Map<QuerySettingDef<?>, Object> resolved = new HashMap<>();
        for (QuerySettingDef<?> def : defs) {
            resolveSingle(def, clusterState, nodeSettings, requestParams, statement, ctx, resolved);
        }
        return new ResolvedSettings(resolved);
    }

    /**
     * Register {@link #warnUnusableClusterDefaults} on the settings-update path. A value {@code elasticsearch.yml}
     * cannot parse or that fails its validator already stops the node starting, and cluster state does not exist yet
     * when components are constructed, so this covers the updates that happen afterwards.
     * <p>
     * Pair it with {@link #watchApproximationLicense}: between them the two registrations cover both ways an operator
     * default becomes unusable — the value changing, and the entitlement changing underneath it.
     */
    public static void watchClusterDefaults(ClusterSettings clusterSettings, BooleanSupplier approximationLicensed) {
        clusterSettings.addSettingsUpdateConsumer(
            updated -> warnUnusableClusterDefaults(updated, Settings.EMPTY, approximationLicensed),
            QuerySettings.clusterSettings()
        );
    }

    /**
     * Warn the operator when a cluster-wide {@code approximation} default stops applying because the license no
     * longer permits it.
     * <p>
     * The settings-update registration cannot cover this on its own: it runs only when a setting is updated, and a
     * license expiring updates no setting. Both paths call {@link #warnUnusableClusterDefaults}, so they report the
     * same set of unusable defaults rather than two drifting subsets. The per-query drop site cannot log it either:
     * it runs on every query, and a misconfigured cluster would flood the log. A license listener fires once per
     * transition, which is the granularity the operator needs, and costs the query path nothing.
     * <p>
     * The license is supplied as a predicate rather than by importing the checker, so this package keeps knowing
     * only about settings.
     */
    public static void watchApproximationLicense(
        @Nullable XPackLicenseState licenseState,
        BooleanSupplier approximationLicensed,
        Supplier<Settings> clusterStateSettings,
        Settings nodeSettings
    ) {
        if (licenseState == null) {
            // XPackPlugin publishes the shared license state through a SetOnce, so a harness that builds this plugin
            // without XPackPlugin sees null. That is test-only: PlanExecutor captures the same state eagerly for its
            // query-time license checks, so a null in a real node would fail every query long before this mattered.
            // No license state means no license transitions, so there is nothing to report.
            return;
        }
        licenseState.addListener(() -> { warnUnusableClusterDefaults(clusterStateSettings.get(), nodeSettings, approximationLicensed); });
    }

    /** Resolve with no operator defaults in play. */
    public static ResolvedSettings resolve(
        Map<QuerySettingDef<?>, Object> requestParams,
        @Nullable EsqlStatement statement,
        SettingsValidationContext ctx
    ) {
        return resolve(Settings.EMPTY, Settings.EMPTY, requestParams, statement, ctx);
    }

    @SuppressWarnings("unchecked")
    private static <T> void resolveSingle(
        QuerySettingDef<T> def,
        Settings clusterState,
        Settings nodeSettings,
        Map<QuerySettingDef<?>, Object> requestParams,
        @Nullable EsqlStatement statement,
        SettingsValidationContext ctx,
        Map<QuerySettingDef<?>, Object> resolved
    ) {
        // Never userSupplied: an operator's value was checked where the operator could see the failure, and must
        // not be revalidated under the query-time context.
        T value = def.effectiveDefault(clusterState, nodeSettings);
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
