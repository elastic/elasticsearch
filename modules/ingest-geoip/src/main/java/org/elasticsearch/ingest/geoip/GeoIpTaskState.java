/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.persistent.PersistentTaskState;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

public class GeoIpTaskState implements PersistentTaskState {

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
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(databases, StreamOutput::writeString, (o, v) -> {
            o.writeLong(v.lastUpdate);
            o.writeVInt(v.firstChunk);
            o.writeVInt(v.lastChunk);
            o.writeString(v.md5);
        });
    }

    public static class Metadata {
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
