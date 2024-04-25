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
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.ql.session.Configuration;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Locale;
import java.util.Objects;

import static org.elasticsearch.common.unit.ByteSizeUnit.KB;

public class EsqlConfiguration extends Configuration implements Writeable {

    static final int QUERY_COMPRESS_THRESHOLD_CHARS = KB.toIntBytes(5);

    private final QueryPragmas pragmas;

    private final int resultTruncationMaxSize;
    private final int resultTruncationDefaultSize;

    private final Locale locale;

    private final String query;

    private final boolean profile;

    public EsqlConfiguration(
        ZoneId zi,
        Locale locale,
        String username,
        String clusterName,
        QueryPragmas pragmas,
        int resultTruncationMaxSize,
        int resultTruncationDefaultSize,
        String query,
        boolean profile
    ) {
        super(zi, username, clusterName);
        this.locale = locale;
        this.pragmas = pragmas;
        this.resultTruncationMaxSize = resultTruncationMaxSize;
        this.resultTruncationDefaultSize = resultTruncationDefaultSize;
        this.query = query;
        this.profile = profile;
    }

    public EsqlConfiguration(StreamInput in) throws IOException {
        super(in.readZoneId(), Instant.ofEpochSecond(in.readVLong(), in.readVInt()), in.readOptionalString(), in.readOptionalString());
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
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeZoneId(zoneId);
        var instant = now.toInstant();
        out.writeVLong(instant.getEpochSecond());
        out.writeVInt(instant.getNano());
        out.writeOptionalString(username);
        out.writeOptionalString(clusterName);
        out.writeString(locale.toLanguageTag());
        pragmas.writeTo(out);
        out.writeVInt(resultTruncationMaxSize);
        out.writeVInt(resultTruncationDefaultSize);
        writeQuery(out, query);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
            out.writeBoolean(profile);
        }
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
     * Note: Currently, it returns {@link System#currentTimeMillis()}, but this value will be serialized between nodes.
     */
    public long absoluteStartedTimeInMillis() {
        return System.currentTimeMillis();
    }

    /**
     * Enable profiling, sacrificing performance to return information about
     * what operations are taking the most time.
     */
    public boolean profile() {
        return profile;
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
        if (super.equals(o)) {
            EsqlConfiguration that = (EsqlConfiguration) o;
            return resultTruncationMaxSize == that.resultTruncationMaxSize
                && resultTruncationDefaultSize == that.resultTruncationDefaultSize
                && Objects.equals(pragmas, that.pragmas)
                && Objects.equals(locale, that.locale)
                && Objects.equals(that.query, query)
                && profile == that.profile;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), pragmas, resultTruncationMaxSize, resultTruncationDefaultSize, locale, query, profile);
    }
}
