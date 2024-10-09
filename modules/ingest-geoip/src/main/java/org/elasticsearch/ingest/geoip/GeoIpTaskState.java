/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.ingest.geoip.GeoIpDownloader.GEOIP_DOWNLOADER;
import static org.elasticsearch.persistent.PersistentTasksCustomMetadata.getTaskWithId;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class GeoIpTaskState implements PersistentTaskState, VersionedNamedWriteable {

    private static boolean includeSha256(TransportVersion version) {
        return version.isPatchFrom(TransportVersions.ENTERPRISE_GEOIP_DOWNLOADER_BACKPORT_8_15)
            || version.onOrAfter(TransportVersions.ENTERPRISE_GEOIP_DOWNLOADER);
    }

    private static final ParseField DATABASES = new ParseField("databases");

    static final GeoIpTaskState EMPTY = new GeoIpTaskState(Map.of());

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<GeoIpTaskState, Void> PARSER = new ConstructingObjectParser<>(
        GEOIP_DOWNLOADER,
        true,
        args -> {
            List<Tuple<String, Metadata>> databases = (List<Tuple<String, Metadata>>) args[0];
            return new GeoIpTaskState(databases.stream().collect(Collectors.toMap(Tuple::v1, Tuple::v2)));
        }
    );

    static {
        PARSER.declareNamedObjects(constructorArg(), (p, c, name) -> Tuple.tuple(name, Metadata.fromXContent(p)), DATABASES);
    }

    public static GeoIpTaskState fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    private final Map<String, Metadata> databases;

    GeoIpTaskState(Map<String, Metadata> databases) {
        this.databases = Map.copyOf(databases);
    }

    GeoIpTaskState(StreamInput input) throws IOException {
        databases = input.readImmutableMap(
            in -> new Metadata(
                in.readLong(),
                in.readVInt(),
                in.readVInt(),
                in.readString(),
                in.readLong(),
                includeSha256(in.getTransportVersion()) ? input.readOptionalString() : null
            )
        );
    }

    public GeoIpTaskState put(String name, Metadata metadata) {
        HashMap<String, Metadata> newDatabases = new HashMap<>(databases);
        newDatabases.put(name, metadata);
        return new GeoIpTaskState(newDatabases);
    }

    public Map<String, Metadata> getDatabases() {
        return databases;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GeoIpTaskState that = (GeoIpTaskState) o;
        return databases.equals(that.databases);
    }

    @Override
    public int hashCode() {
        return Objects.hash(databases);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.startObject("databases");
            for (Map.Entry<String, Metadata> e : databases.entrySet()) {
                builder.field(e.getKey(), e.getValue());
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return "geoip-downloader";
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_7_13_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(databases, (o, v) -> {
            o.writeLong(v.lastUpdate);
            o.writeVInt(v.firstChunk);
            o.writeVInt(v.lastChunk);
            o.writeString(v.md5);
            o.writeLong(v.lastCheck);
            if (includeSha256(o.getTransportVersion())) {
                o.writeOptionalString(v.sha256);
            }
        });
    }

    public record Metadata(long lastUpdate, int firstChunk, int lastChunk, String md5, long lastCheck, @Nullable String sha256)
        implements
            ToXContentObject {

        /**
         * An empty Metadata object useful for getOrDefault -type calls. Crucially, the 'lastChunk' is -1, so it's safe to use
         * with logic that says the new firstChunk is the old lastChunk + 1.
         */
        static Metadata EMPTY = new Metadata(-1, -1, -1, "", -1, null);

        private static final String NAME = GEOIP_DOWNLOADER + "-metadata";
        private static final ParseField LAST_CHECK = new ParseField("last_check");
        private static final ParseField LAST_UPDATE = new ParseField("last_update");
        private static final ParseField FIRST_CHUNK = new ParseField("first_chunk");
        private static final ParseField LAST_CHUNK = new ParseField("last_chunk");
        private static final ParseField MD5 = new ParseField("md5");
        private static final ParseField SHA256 = new ParseField("sha256");

        private static final ConstructingObjectParser<Metadata, Void> PARSER = new ConstructingObjectParser<>(
            NAME,
            true,
            args -> new Metadata(
                (long) args[0],
                (int) args[1],
                (int) args[2],
                (String) args[3],
                (long) (args[4] == null ? args[0] : args[4]),
                (String) args[5]
            )
        );

        static {
            PARSER.declareLong(constructorArg(), LAST_UPDATE);
            PARSER.declareInt(constructorArg(), FIRST_CHUNK);
            PARSER.declareInt(constructorArg(), LAST_CHUNK);
            PARSER.declareString(constructorArg(), MD5);
            PARSER.declareLong(optionalConstructorArg(), LAST_CHECK);
            PARSER.declareString(optionalConstructorArg(), SHA256);
        }

        public static Metadata fromXContent(XContentParser parser) {
            try {
                return PARSER.parse(parser, null);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public Metadata {
            Objects.requireNonNull(md5);
        }

        Metadata(long lastUpdate, int firstChunk, int lastChunk, String md5, long lastCheck) {
            this(lastUpdate, firstChunk, lastChunk, md5, lastCheck, null);
        }

        public boolean isCloseToExpiration() {
            return Instant.ofEpochMilli(lastCheck).isBefore(Instant.now().minus(25, ChronoUnit.DAYS));
        }

        public boolean isNewEnough(Settings settings) {
            TimeValue valid = settings.getAsTime("ingest.geoip.database_validity", TimeValue.timeValueDays(30));
            return Instant.ofEpochMilli(lastCheck).isAfter(Instant.now().minus(valid.getMillis(), ChronoUnit.MILLIS));
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.field(LAST_UPDATE.getPreferredName(), lastUpdate);
                builder.field(LAST_CHECK.getPreferredName(), lastCheck);
                builder.field(FIRST_CHUNK.getPreferredName(), firstChunk);
                builder.field(LAST_CHUNK.getPreferredName(), lastChunk);
                builder.field(MD5.getPreferredName(), md5);
                if (sha256 != null) { // only serialize if not null, for prettiness reasons
                    builder.field(SHA256.getPreferredName(), sha256);
                }
            }
            builder.endObject();
            return builder;
        }
    }

    /**
     * Retrieves the geoip downloader's task state from the cluster state. This may return null in some circumstances,
     * for example if the geoip downloader task hasn't been created yet (which it wouldn't be if it's disabled).
     *
     * @param state the cluster state to read the task state from
     * @return the geoip downloader's task state or null if there is not a state to read
     */
    @Nullable
    static GeoIpTaskState getGeoIpTaskState(ClusterState state) {
        PersistentTasksCustomMetadata.PersistentTask<?> task = getTaskWithId(state, GeoIpDownloader.GEOIP_DOWNLOADER);
        return (task == null) ? null : (GeoIpTaskState) task.getState();
    }

}
