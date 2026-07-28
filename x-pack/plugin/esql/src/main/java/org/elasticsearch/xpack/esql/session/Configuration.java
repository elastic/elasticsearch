/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.compute.data.BlockStreamInput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.Column;
import org.elasticsearch.xpack.esql.approximation.ApproximationSettings;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.plan.QuerySettingDef;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.plan.ResolvedSettings;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.unit.ByteSizeUnit.KB;

public class Configuration implements Writeable {

    public static final int QUERY_COMPRESS_THRESHOLD_CHARS = KB.toIntBytes(5);

    private static final TransportVersion TIMESERIES_DEFAULT_LIMIT = TransportVersion.fromName("timeseries_default_limit");

    private static final TransportVersion ESQL_SUPPORT_PARTIAL_RESULTS = TransportVersion.fromName("esql_support_partial_results");

    private static final TransportVersion QUERY_APPROXIMATION = TransportVersion.fromName("esql_query_approximation");

    /**
     * Transport version for view queries support. This is needed to correctly serialize/deserialize
     * Source objects that originated from views (which have positions relative to the view query,
     * not the main query).
     */
    public static final TransportVersion ESQL_VIEW_QUERIES = TransportVersion.fromName("esql_view_queries");

    private static final TransportVersion ESQL_EXPLAIN_ONLY = TransportVersion.fromName("esql_explain_only");

    private static final TransportVersion ESQL_RESOLVED_SETTINGS = TransportVersion.fromName("esql_resolved_settings");

    /**
     * Reserved transport version id from the GROK watchdog work (#152170), which was reverted before release.
     * Intentionally unused: the id is boxed in between released version markers, so it cannot be removed without
     * breaking transport-version id density. This reference keeps the definition from becoming orphaned. Do not
     * serialize anything against it — the feature was reverted and mixed clusters must not exchange a grok field.
     */
    @SuppressWarnings("unused")
    private static final TransportVersion ESQL_GROK_WATCHDOG = TransportVersion.fromName("esql_grok_watchdog");

    private final String clusterName;
    private final String username;
    private final Instant now;

    private final QueryPragmas pragmas;

    private final int resultTruncationMaxSizeRegular;
    private final int resultTruncationDefaultSizeRegular;
    private final int resultTruncationMaxSizeTimeseries;
    private final int resultTruncationDefaultSizeTimeseries;

    private final Locale locale;

    private final String query;

    private final boolean profile;
    private final boolean allowPartialResults;
    private final boolean explainOnly;

    private final Map<String, Map<String, Column>> tables;
    private final long queryStartTimeNanos;

    /**
     * The resolved view of every {@link org.elasticsearch.xpack.esql.plan.QuerySettingDef} for this
     * query — registry default {@code <} request body {@code <} in-query {@code SET}. Travels across
     * the wire to data nodes, so every node and driver that holds a Configuration also holds the
     * full resolved settings view.
     */
    private final ResolvedSettings resolvedSettings;

    /**
     * Map of view names to their query strings. Used during deserialization to reconstruct
     * Source objects that originated from views.
     */
    private final Map<String, String> viewQueries;

    public Configuration(
        Instant now,
        Locale locale,
        String username,
        String clusterName,
        QueryPragmas pragmas,
        int resultTruncationMaxSizeRegular,
        int resultTruncationDefaultSizeRegular,
        @Nullable String query,
        boolean profile,
        Map<String, Map<String, Column>> tables,
        long queryStartTimeNanos,
        boolean allowPartialResults,
        int resultTruncationMaxSizeTimeseries,
        int resultTruncationDefaultSizeTimeseries,
        ResolvedSettings resolvedSettings,
        Map<String, String> viewQueries
    ) {
        this(
            now,
            locale,
            username,
            clusterName,
            pragmas,
            resultTruncationMaxSizeRegular,
            resultTruncationDefaultSizeRegular,
            query,
            profile,
            tables,
            queryStartTimeNanos,
            allowPartialResults,
            resultTruncationMaxSizeTimeseries,
            resultTruncationDefaultSizeTimeseries,
            resolvedSettings,
            viewQueries,
            false
        );
    }

    /**
     * Canonical constructor — every field is a parameter (the {@code explainOnly} flag is the only difference
     * from the 16-arg constructor above). {@link ConfigurationBuilder#build()} calls this directly, so any new
     * field added here must also be added to {@link ConfigurationBuilder}.
     */
    public Configuration(
        Instant now,
        Locale locale,
        String username,
        String clusterName,
        QueryPragmas pragmas,
        int resultTruncationMaxSizeRegular,
        int resultTruncationDefaultSizeRegular,
        @Nullable String query,
        boolean profile,
        Map<String, Map<String, Column>> tables,
        long queryStartTimeNanos,
        boolean allowPartialResults,
        int resultTruncationMaxSizeTimeseries,
        int resultTruncationDefaultSizeTimeseries,
        ResolvedSettings resolvedSettings,
        Map<String, String> viewQueries,
        boolean explainOnly
    ) {
        this.now = now;
        this.username = username;
        this.clusterName = clusterName;
        this.locale = locale;
        this.pragmas = pragmas;
        this.resultTruncationMaxSizeRegular = resultTruncationMaxSizeRegular;
        this.resultTruncationDefaultSizeRegular = resultTruncationDefaultSizeRegular;
        this.resultTruncationMaxSizeTimeseries = resultTruncationMaxSizeTimeseries;
        this.resultTruncationDefaultSizeTimeseries = resultTruncationDefaultSizeTimeseries;
        this.query = query != null ? query : "";
        this.profile = profile;
        this.tables = tables;
        assert tables != null;
        this.queryStartTimeNanos = queryStartTimeNanos;
        this.allowPartialResults = allowPartialResults;
        this.resolvedSettings = resolvedSettings != null ? resolvedSettings : ResolvedSettings.EMPTY;
        this.viewQueries = viewQueries;
        assert viewQueries != null;
        this.explainOnly = explainOnly;
    }

    public Configuration(BlockStreamInput in) throws IOException {
        // Settings cross the wire generically in the resolvedSettings block (read near the end). A peer that
        // predates that block instead sends time_zone and approximation in their original positional slots — the
        // zone first, the approximation later — which we read here and fold back into a ResolvedSettings. These two
        // legacy reads (and their writeTo counterparts) are the only setting-specific wire handling left; they are
        // touched only when an older peer is on the other end, and go away entirely once the minimum transport
        // version reaches esql_resolved_settings.
        boolean readLegacySettings = in.getTransportVersion().supports(ESQL_RESOLVED_SETTINGS) == false;
        ZoneId zi = readLegacySettings ? in.readZoneId() : null;
        this.now = Instant.ofEpochSecond(in.readVLong(), in.readVInt());
        this.username = in.readOptionalString();
        this.clusterName = in.readOptionalString();
        locale = Locale.forLanguageTag(in.readString());
        this.pragmas = new QueryPragmas(in);
        this.resultTruncationMaxSizeRegular = in.readVInt();
        this.resultTruncationDefaultSizeRegular = in.readVInt();
        this.query = readQuery(in);
        this.profile = in.readBoolean();
        this.tables = in.readImmutableMap(i1 -> i1.readImmutableMap(i2 -> new Column((BlockStreamInput) i2)));
        this.queryStartTimeNanos = in.readLong();
        if (in.getTransportVersion().supports(ESQL_SUPPORT_PARTIAL_RESULTS)) {
            this.allowPartialResults = in.readBoolean();
        } else {
            this.allowPartialResults = false;
        }
        if (in.getTransportVersion().supports(TIMESERIES_DEFAULT_LIMIT)) {
            this.resultTruncationMaxSizeTimeseries = in.readVInt();
            this.resultTruncationDefaultSizeTimeseries = in.readVInt();
        } else {
            this.resultTruncationMaxSizeTimeseries = this.resultTruncationMaxSizeRegular;
            this.resultTruncationDefaultSizeTimeseries = this.resultTruncationDefaultSizeRegular;
        }
        ApproximationSettings legacyApproximation = null;
        if (readLegacySettings && in.getTransportVersion().supports(QUERY_APPROXIMATION)) {
            legacyApproximation = in.readOptionalWriteable(ApproximationSettings::new);
        }
        if (in.getTransportVersion().supports(ESQL_VIEW_QUERIES)) {
            this.viewQueries = in.readImmutableMap(StreamInput::readString);
        } else {
            this.viewQueries = Map.of();
        }
        if (in.getTransportVersion().supports(ESQL_EXPLAIN_ONLY)) {
            this.explainOnly = in.readBoolean();
        } else {
            this.explainOnly = false;
        }
        if (readLegacySettings) {
            // project_routing is intentionally not synthesized here — data nodes never had it on the wire.
            this.resolvedSettings = synthesizeResolvedFromLegacy(zi, legacyApproximation);
        } else {
            this.resolvedSettings = new ResolvedSettings(in);
        }
    }

    private static ResolvedSettings synthesizeResolvedFromLegacy(ZoneId zoneId, @Nullable ApproximationSettings approximation) {
        ResolvedSettings result = ResolvedSettings.EMPTY;
        if (zoneId != null) {
            // withOverride canonicalizes (TIME_ZONE normalizes), so no explicit .normalized() is needed here.
            result = result.withOverride(QuerySettings.TIME_ZONE, zoneId);
        }
        if (approximation != null) {
            result = result.withOverride(QuerySettings.APPROXIMATION, approximation);
        }
        return result;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // Peers that understand the generic resolvedSettings block (written near the end) receive settings only
        // through it. Older peers instead expect time_zone and approximation in their original positional slots, so
        // for them — and only them — we derive those two values from resolvedSettings and write the legacy slots.
        // Exactly one of {legacy slots, resolvedSettings block} is written, so there is no redundant double-write.
        boolean writeLegacySettings = out.getTransportVersion().supports(ESQL_RESOLVED_SETTINGS) == false;
        if (writeLegacySettings) {
            out.writeZoneId(QuerySettings.TIME_ZONE.get(resolvedSettings));
        }
        out.writeVLong(now.getEpochSecond());
        out.writeVInt(now.getNano());
        out.writeOptionalString(username);    // TODO this one is always null
        out.writeOptionalString(clusterName); // TODO this one is never null so maybe not optional
        out.writeString(locale.toLanguageTag());
        pragmas.writeTo(out);
        out.writeVInt(resultTruncationMaxSizeRegular);
        out.writeVInt(resultTruncationDefaultSizeRegular);
        writeQuery(out, query);
        out.writeBoolean(profile);
        out.writeMap(tables, (o1, columns) -> o1.writeMap(columns, StreamOutput::writeWriteable));
        out.writeLong(queryStartTimeNanos);
        if (out.getTransportVersion().supports(ESQL_SUPPORT_PARTIAL_RESULTS)) {
            out.writeBoolean(allowPartialResults);
        }
        if (out.getTransportVersion().supports(TIMESERIES_DEFAULT_LIMIT)) {
            out.writeVInt(resultTruncationMaxSizeTimeseries);
            out.writeVInt(resultTruncationDefaultSizeTimeseries);
        }
        if (writeLegacySettings && out.getTransportVersion().supports(QUERY_APPROXIMATION)) {
            out.writeOptionalWriteable(QuerySettings.APPROXIMATION.get(resolvedSettings));
        }
        if (out.getTransportVersion().supports(ESQL_VIEW_QUERIES)) {
            out.writeMap(viewQueries, StreamOutput::writeString);
        }
        if (out.getTransportVersion().supports(ESQL_EXPLAIN_ONLY)) {
            out.writeBoolean(explainOnly);
        }
        if (writeLegacySettings == false) {
            resolvedSettings.writeTo(out);
        }
    }

    /**
     * The resolved view of every {@link org.elasticsearch.xpack.esql.plan.QuerySettingDef} for this
     * query. Reads of any SET-mirror knob (time_zone, project_routing, approximation, unmapped_fields,
     * any future setting) go through this — e.g. {@code QuerySettings.TIME_ZONE.get(configuration.resolvedSettings())}.
     */
    public ResolvedSettings resolvedSettings() {
        return resolvedSettings;
    }

    public Instant now() {
        return now;
    }

    public String clusterName() {
        return clusterName;
    }

    public String username() {
        return username;
    }

    public QueryPragmas pragmas() {
        return pragmas;
    }

    public int resultTruncationMaxSize(boolean isTimeseries) {
        if (isTimeseries) {
            return resultTruncationMaxSizeTimeseries;
        }
        return resultTruncationMaxSizeRegular;
    }

    public int resultTruncationDefaultSize(boolean isTimeseries) {
        if (isTimeseries) {
            return resultTruncationDefaultSizeTimeseries;
        }
        return resultTruncationDefaultSizeRegular;
    }

    public Locale locale() {
        return locale;
    }

    public String query() {
        return query;
    }

    /**
     * Returns the current time in milliseconds from the time epoch for the execution of this request.
     * It ensures consistency by using the same value on all nodes involved in the search request.
     */
    public long absoluteStartedTimeInMillis() {
        return now.toEpochMilli();
    }

    /**
     * @return Start time of the ESQL query in nanos
     */
    public long queryStartTimeNanos() {
        return queryStartTimeNanos;
    }

    /**
     * Create a new {@link FoldContext} with the limit configured in the {@link QueryPragmas}.
     */
    public FoldContext newFoldContext() {
        return new FoldContext(pragmas.foldLimit().getBytes());
    }

    /**
     * Tables specified in the request.
     */
    public Map<String, Map<String, Column>> tables() {
        return tables;
    }

    public Configuration withoutTables() {
        return new ConfigurationBuilder(this).tables(Map.of()).build();
    }

    /**
     * Enable profiling, sacrificing performance to return information about
     * what operations are taking the most time.
     */
    public boolean profile() {
        return profile;
    }

    /**
     * Whether this request can return partial results instead of failing fast on failures
     */
    public boolean allowPartialResults() {
        return allowPartialResults;
    }

    /**
     * Whether this is an explain-only request. This flag is propagated to data nodes
     * and could be used for future optimization to skip actual computation.
     * Currently, the EXPLAIN command executes the query normally with profile=true.
     */
    public boolean explainOnly() {
        return explainOnly;
    }

    /**
     * Returns a new Configuration with profile and explainOnly enabled.
     * Used for EXPLAIN queries that need to capture plan information.
     */
    public Configuration withExplainOnly() {
        return new ConfigurationBuilder(this).profile(true).explainOnly(true).build();
    }

    /**
     * Returns the map of view names to their query strings.
     */
    public Map<String, String> viewQueries() {
        return viewQueries;
    }

    /**
     * Returns a new Configuration with one {@link QuerySettingDef} value overridden. Generic — caller
     * names the setting via the {@link QuerySettingDef} constant.
     */
    public <T> Configuration withSetting(QuerySettingDef<T> def, T value) {
        return new ConfigurationBuilder(this).setting(def, value).build();
    }

    /**
     * Returns a new Configuration with the given view queries added.
     */
    public Configuration withViewQueries(Map<String, String> viewQueries) {
        return new ConfigurationBuilder(this).viewQueries(viewQueries).build();
    }

    private static void writeQuery(StreamOutput out, String query) throws IOException {
        if (query.length() > QUERY_COMPRESS_THRESHOLD_CHARS) { // compare on chars to avoid UTF-8 encoding unless actually required
            out.writeBoolean(true);
            var bytesArray = new BytesArray(query.getBytes(StandardCharsets.UTF_8));
            var bytesRef = CompressorFactory.COMPRESSOR.compress(bytesArray);
            out.writeByteArray(bytesRef.array());
        } else {
            out.writeBoolean(false);
            out.writeString(query);
        }
    }

    private static String readQuery(StreamInput in) throws IOException {
        boolean compressed = in.readBoolean();
        if (compressed) {
            byte[] bytes = in.readByteArray();
            var bytesRef = CompressorFactory.uncompress(new BytesArray(bytes));
            return new String(bytesRef.array(), StandardCharsets.UTF_8);
        } else {
            return in.readString();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Configuration that = (Configuration) o;
        return Objects.equals(now, that.now)
            && Objects.equals(username, that.username)
            && Objects.equals(clusterName, that.clusterName)
            && resultTruncationMaxSizeRegular == that.resultTruncationMaxSizeRegular
            && resultTruncationDefaultSizeRegular == that.resultTruncationDefaultSizeRegular
            && resultTruncationMaxSizeTimeseries == that.resultTruncationMaxSizeTimeseries
            && resultTruncationDefaultSizeTimeseries == that.resultTruncationDefaultSizeTimeseries
            && Objects.equals(pragmas, that.pragmas)
            && Objects.equals(locale, that.locale)
            && Objects.equals(that.query, query)
            && profile == that.profile
            && tables.equals(that.tables)
            && allowPartialResults == that.allowPartialResults
            && Objects.equals(resolvedSettings, that.resolvedSettings)
            && viewQueries.equals(that.viewQueries)
            && explainOnly == that.explainOnly;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            now,
            username,
            clusterName,
            pragmas,
            resultTruncationMaxSizeRegular,
            resultTruncationDefaultSizeRegular,
            locale,
            query,
            profile,
            tables,
            allowPartialResults,
            resultTruncationMaxSizeTimeseries,
            resultTruncationDefaultSizeTimeseries,
            resolvedSettings,
            viewQueries,
            explainOnly
        );
    }

    @Override
    public String toString() {
        return "EsqlConfiguration{"
            + "pragmas="
            + pragmas
            + ", resultTruncationMaxSize="
            + "[regular="
            + resultTruncationMaxSize(false)
            + ",timeseries="
            + resultTruncationMaxSize(true)
            + "]"
            + ", resultTruncationDefaultSize="
            + "[regular="
            + resultTruncationDefaultSize(false)
            + ",timeseries="
            + resultTruncationDefaultSize(true)
            + "]"
            + ", resolvedSettings="
            + resolvedSettings
            + ", locale="
            + locale
            + ", query='"
            + query
            + '\''
            + ", profile="
            + profile
            + ", tables="
            + tables
            + ", allowPartialResults="
            + allowPartialResults
            + ", explainOnly="
            + explainOnly
            + '}';
    }

    /**
     * Reads a {@link Configuration} that doesn't contain any {@link Configuration#tables()}.
     */
    public static Configuration readWithoutTables(StreamInput in) throws IOException {
        BlockStreamInput blockStreamInput = new BlockStreamInput(in, null);
        return new Configuration(blockStreamInput);
    }
}
