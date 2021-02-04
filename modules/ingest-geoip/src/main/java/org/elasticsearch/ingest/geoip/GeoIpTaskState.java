/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip;

import org.elasticsearch.Version;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.persistent.PersistentTaskState;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.ingest.geoip.GeoIpDownloader.GEOIP_DOWNLOADER;

public class GeoIpTaskState implements PersistentTaskState, VersionedNamedWriteable {

    private static ParseField DATABASES = new ParseField("databases");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<GeoIpTaskState, Void> PARSER =
        new ConstructingObjectParser<>(GEOIP_DOWNLOADER, true,
            args -> {
                List<Map<String, Metadata>> databases = (List<Map<String, Metadata>>) args[0];
                GeoIpTaskState geoIpTaskState = new GeoIpTaskState();
                for (Map<String, Metadata> database : databases) {
                    geoIpTaskState.databases.putAll(database);
                }
                return geoIpTaskState;
            });

    static {
        PARSER.declareNamedObjects(constructorArg(), (p, c, name) -> Map.of(name, Metadata.fromXContent(p)), DATABASES);
    }

    public static GeoIpTaskState fromXContent(XContentParser parser) {
        try {
            return PARSER.parse(parser, null);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private final Map<String, Metadata> databases = new TreeMap<>();

    public GeoIpTaskState() {
    }

    public GeoIpTaskState(StreamInput input) throws IOException {
        databases.putAll(input.readMap(StreamInput::readString, in -> new Metadata(in.readLong(), in.readVInt(), in.readVInt(),
            in.readString())));
    }

    public Map<String, Metadata> getDatabases() {
        return databases;
    }

    public GeoIpTaskState copy() {
        GeoIpTaskState geoIpTaskState = new GeoIpTaskState();
        geoIpTaskState.getDatabases().putAll(databases);
        return geoIpTaskState;
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
                builder.startObject(e.getKey());
                {
                    Metadata meta = e.getValue();
                    builder.field("last_update", meta.lastUpdate);
                    builder.field("first_chunk", meta.firstChunk);
                    builder.field("last_chunk", meta.lastChunk);
                    builder.field("md5", meta.md5);
                }
                builder.endObject();
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
        return Version.V_8_0_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(databases, StreamOutput::writeString, (o, v) -> {
            o.writeLong(v.lastUpdate);
            o.writeVInt(v.firstChunk);
            o.writeVInt(v.lastChunk);
            o.writeString(v.md5);
        });
    }

    public static class Metadata {

        public static final String NAME = GEOIP_DOWNLOADER + "-metadata";

        private static ParseField LAST_UPDATE = new ParseField("last_update");
        private static ParseField FIRST_CHUNK = new ParseField("first_chunk");
        private static ParseField LAST_CHUNK = new ParseField("last_chunk");
        private static ParseField MD5 = new ParseField("md5");

        private static final ConstructingObjectParser<Metadata, Void> PARSER =
            new ConstructingObjectParser<>(NAME, true,
                args -> new Metadata((long) args[0], (int) args[1], (int) args[2], (String) args[3]));

        static {
            PARSER.declareLong(constructorArg(), LAST_UPDATE);
            PARSER.declareInt(constructorArg(), FIRST_CHUNK);
            PARSER.declareInt(constructorArg(), LAST_CHUNK);
            PARSER.declareString(constructorArg(), MD5);
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

        public Metadata(long lastUpdate, int firstChunk, int lastChunk, String md5) {
            this.lastUpdate = lastUpdate;
            this.firstChunk = firstChunk;
            this.lastChunk = lastChunk;
            this.md5 = Objects.requireNonNull(md5);
        }

        public long getLastUpdate() {
            return lastUpdate;
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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Metadata metadata = (Metadata) o;
            return lastUpdate == metadata.lastUpdate
                && firstChunk == metadata.firstChunk
                && lastChunk == metadata.lastChunk
                && md5.equals(metadata.md5);
        }

        @Override
        public int hashCode() {
            return Objects.hash(lastUpdate, firstChunk, lastChunk, md5);
        }
    }
}
