/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.compute.data.BlockStreamInput;
import org.elasticsearch.xpack.esql.Column;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.unit.ByteSizeUnit.KB;

public class Configuration implements Writeable {

    public static final int QUERY_COMPRESS_THRESHOLD_CHARS = KB.toIntBytes(5);

    private final String clusterName;
    private final String username;
    private final ZonedDateTime now;
    private final ZoneId zoneId;

    private final QueryPragmas pragmas;

    private final int resultTruncationMaxSize;
    private final int resultTruncationDefaultSize;

    private final Locale locale;

    private final String query;

    private final boolean profile;
    private final boolean allowPartialResults;

    private final Map<String, Map<String, Column>> tables;
    private final long queryStartTimeNanos;

    public Configuration(
        ZoneId zi,
        Locale locale,
        String username,
        String clusterName,
        QueryPragmas pragmas,
        int resultTruncationMaxSize,
        int resultTruncationDefaultSize,
        String query,
        boolean profile,
        Map<String, Map<String, Column>> tables,
        long queryStartTimeNanos,
        boolean allowPartialResults
    ) {
        this.zoneId = zi.normalized();
        this.now = ZonedDateTime.now(Clock.tick(Clock.system(zoneId), Duration.ofNanos(1)));
        this.username = username;
        this.clusterName = clusterName;
        this.locale = locale;
        this.pragmas = pragmas;
        this.resultTruncationMaxSize = resultTruncationMaxSize;
        this.resultTruncationDefaultSize = resultTruncationDefaultSize;
        this.query = query;
        this.profile = profile;
        this.tables = tables;
        assert tables != null;
        this.queryStartTimeNanos = queryStartTimeNanos;
        this.allowPartialResults = allowPartialResults;
    }

    public Configuration(BlockStreamInput in) throws IOException {
        this.zoneId = in.readZoneId();
        this.now = Instant.ofEpochSecond(in.readVLong(), in.readVInt()).atZone(zoneId);
        this.username = in.readOptionalString();
        this.clusterName = in.readOptionalString();
        locale = Locale.forLanguageTag(in.readString());
        this.pragmas = new QueryPragmas(in);
        this.resultTruncationMaxSize = in.readVInt();
        this.resultTruncationDefaultSize = in.readVInt();
        this.query = readQuery(in);
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
            this.profile = in.readBoolean();
        } else {
            this.profile = false;
        }
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
            this.tables = in.readImmutableMap(i1 -> i1.readImmutableMap(i2 -> new Column((BlockStreamInput) i2)));
        } else {
            this.tables = Map.of();
        }
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
            this.queryStartTimeNanos = in.readLong();
        } else {
            this.queryStartTimeNanos = -1;
        }
        if (in.getTransportVersion().onOrAfter(TransportVersions.ESQL_SUPPORT_PARTIAL_RESULTS)
            || in.getTransportVersion().isPatchFrom(TransportVersions.ESQL_SUPPORT_PARTIAL_RESULTS_BACKPORT_8_19)) {
            this.allowPartialResults = in.readBoolean();
        } else {
            this.allowPartialResults = false;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeZoneId(zoneId);
        var instant = now.toInstant();
        out.writeVLong(instant.getEpochSecond());
        out.writeVInt(instant.getNano());
        out.writeOptionalString(username);    // TODO this one is always null
        out.writeOptionalString(clusterName); // TODO this one is never null so maybe not optional
        out.writeString(locale.toLanguageTag());
        pragmas.writeTo(out);
        out.writeVInt(resultTruncationMaxSize);
        out.writeVInt(resultTruncationDefaultSize);
        writeQuery(out, query);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
            out.writeBoolean(profile);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
            out.writeMap(tables, (o1, columns) -> o1.writeMap(columns, StreamOutput::writeWriteable));
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
            out.writeLong(queryStartTimeNanos);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.ESQL_SUPPORT_PARTIAL_RESULTS)
            || out.getTransportVersion().isPatchFrom(TransportVersions.ESQL_SUPPORT_PARTIAL_RESULTS_BACKPORT_8_19)) {
            out.writeBoolean(allowPartialResults);
        }
    }

    public ZoneId zoneId() {
        return zoneId;
    }

    public ZonedDateTime now() {
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

    public int resultTruncationMaxSize() {
        return resultTruncationMaxSize;
    }

    public int resultTruncationDefaultSize() {
        return resultTruncationDefaultSize;
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
        return now.toInstant().toEpochMilli();
    }

    /**
     * @return Start time of the ESQL query in nanos
     */
    public long getQueryStartTimeNanos() {
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
        return Objects.equals(zoneId, that.zoneId)
            && Objects.equals(now, that.now)
            && Objects.equals(username, that.username)
            && Objects.equals(clusterName, that.clusterName)
            && resultTruncationMaxSize == that.resultTruncationMaxSize
            && resultTruncationDefaultSize == that.resultTruncationDefaultSize
            && Objects.equals(pragmas, that.pragmas)
            && Objects.equals(locale, that.locale)
            && Objects.equals(that.query, query)
            && profile == that.profile
            && tables.equals(that.tables)
            && allowPartialResults == that.allowPartialResults;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            zoneId,
            now,
            username,
            clusterName,
            pragmas,
            resultTruncationMaxSize,
            resultTruncationDefaultSize,
            locale,
            query,
            profile,
            tables,
            allowPartialResults
        );
    }

    @Override
    public String toString() {
        return "EsqlConfiguration{"
            + "pragmas="
            + pragmas
            + ", resultTruncationMaxSize="
            + resultTruncationMaxSize
            + ", resultTruncationDefaultSize="
            + resultTruncationDefaultSize
            + ", locale="
            + locale
            + ", query='"
            + query
            + '\''
            + ", profile="
            + profile
            + ", tables="
            + tables
            + "allow_partial_result="
            + allowPartialResults
            + '}';
    }

}
