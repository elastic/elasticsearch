/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.persistent.PersistentTaskState;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.ingest.geoip.GeoIpDownloader.GEOIP_DOWNLOADER;

class GeoIpTaskState implements PersistentTaskState, VersionedNamedWriteable {

    private static final ParseField DATABASES = new ParseField("databases");

    static final GeoIpTaskState EMPTY = new GeoIpTaskState(Collections.emptyMap());

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<GeoIpTaskState, Void> PARSER =
        new ConstructingObjectParser<>(GEOIP_DOWNLOADER, true,
            args -> {
                List<Tuple<String, Metadata>> databases = (List<Tuple<String, Metadata>>) args[0];
                return new GeoIpTaskState(databases.stream().collect(Collectors.toMap(Tuple::v1, Tuple::v2)));
            });

    static {
        PARSER.declareNamedObjects(constructorArg(), (p, c, name) -> Tuple.tuple(name, Metadata.fromXContent(p)), DATABASES);
    }

    public static GeoIpTaskState fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    private final Map<String, Metadata> databases;

    GeoIpTaskState(Map<String, Metadata> databases) {
        this.databases = Collections.unmodifiableMap(new HashMap<>(databases));
    }

    GeoIpTaskState(StreamInput input) throws IOException {
        databases = Collections.unmodifiableMap(input.readMap(StreamInput::readString,
            in -> {
                long lastUpdate = in.readLong();
                return new Metadata(lastUpdate, in.readVInt(), in.readVInt(), in.readString(),
                    in.getVersion().onOrAfter(Version.V_7_14_0) ? in.readLong() : lastUpdate);
            }));
    }

    public GeoIpTaskState put(String name, Metadata metadata) {
        HashMap<String, Metadata> newDatabases = new HashMap<>(databases);
        newDatabases.put(name, metadata);
        return new GeoIpTaskState(newDatabases);
    }

    public Map<String, Metadata> getDatabases() {
        return databases;
    }

    public boolean contains(String name) {
        return databases.containsKey(name);
    }

    public Metadata get(String name) {
        return databases.get(name);
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
    public Version getMinimalSupportedVersion() {
        return Version.V_7_13_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(databases, StreamOutput::writeString, (o, v) -> {
            o.writeLong(v.lastUpdate);
            o.writeVInt(v.firstChunk);
            o.writeVInt(v.lastChunk);
            o.writeString(v.md5);
            if (o.getVersion().onOrAfter(Version.V_7_14_0)) {
                o.writeLong(v.lastCheck);
            }
        });
    }

    static class Metadata implements ToXContentObject {

        static final String NAME = GEOIP_DOWNLOADER + "-metadata";
        private static final ParseField LAST_CHECK = new ParseField("last_check");
        private static final ParseField LAST_UPDATE = new ParseField("last_update");
        private static final ParseField FIRST_CHUNK = new ParseField("first_chunk");
        private static final ParseField LAST_CHUNK = new ParseField("last_chunk");
        private static final ParseField MD5 = new ParseField("md5");

        private static final ConstructingObjectParser<Metadata, Void> PARSER =
            new ConstructingObjectParser<>(NAME, true,
                args -> new Metadata((long) args[0], (int) args[1], (int) args[2], (String) args[3], (long) (args[4] == null ? args[0] :
                    args[4])));

        static {
            PARSER.declareLong(constructorArg(), LAST_UPDATE);
            PARSER.declareInt(constructorArg(), FIRST_CHUNK);
            PARSER.declareInt(constructorArg(), LAST_CHUNK);
            PARSER.declareString(constructorArg(), MD5);
            PARSER.declareLong(optionalConstructorArg(), LAST_CHECK);
        }

        public static Metadata fromXContent(XContentParser parser) {
            try {
                return PARSER.parse(parser, null);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private final long lastUpdate;
        private final int firstChunk;
        private final int lastChunk;
        private final String md5;
        private final long lastCheck;

        Metadata(long lastUpdate, int firstChunk, int lastChunk, String md5, long lastCheck) {
            this.lastUpdate = lastUpdate;
            this.firstChunk = firstChunk;
            this.lastChunk = lastChunk;
            this.md5 = Objects.requireNonNull(md5);
            this.lastCheck = lastCheck;
        }

        public long getLastUpdate() {
            return lastUpdate;
        }

        public boolean isCloseToExpiration(){
            return Instant.ofEpochMilli(lastCheck).isBefore(Instant.now().minus(25, ChronoUnit.DAYS));
        }

        public boolean isValid(Settings settings) {
            TimeValue valid = settings.getAsTime("ingest.geoip.database_validity", TimeValue.timeValueDays(30));
            return Instant.ofEpochMilli(lastCheck).isAfter(Instant.now().minus(valid.getMillis(), ChronoUnit.MILLIS));
        }

        public int getFirstChunk() {
            return firstChunk;
        }

        public int getLastChunk() {
            return lastChunk;
        }

        public String getMd5() {
            return md5;
        }

        public long getLastCheck() {
            return lastCheck;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Metadata metadata = (Metadata) o;
            return lastUpdate == metadata.lastUpdate
                && firstChunk == metadata.firstChunk
                && lastChunk == metadata.lastChunk
                && lastCheck == metadata.lastCheck
                && md5.equals(metadata.md5);
        }

        @Override
        public int hashCode() {
            return Objects.hash(lastUpdate, firstChunk, lastChunk, md5, lastCheck);
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
            }
            builder.endObject();
            return builder;
        }
    }
}
