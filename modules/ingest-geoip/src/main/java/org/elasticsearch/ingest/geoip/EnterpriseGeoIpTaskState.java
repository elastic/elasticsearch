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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.ingest.EnterpriseGeoIpTask;
import org.elasticsearch.ingest.geoip.GeoIpTaskState.Metadata;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.ingest.geoip.GeoIpDownloader.GEOIP_DOWNLOADER;
import static org.elasticsearch.persistent.PersistentTasksCustomMetadata.getTaskWithId;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

class EnterpriseGeoIpTaskState implements PersistentTaskState, VersionedNamedWriteable {

    private static final ParseField DATABASES = new ParseField("databases");

    static final EnterpriseGeoIpTaskState EMPTY = new EnterpriseGeoIpTaskState(Map.of());

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<EnterpriseGeoIpTaskState, Void> PARSER = new ConstructingObjectParser<>(
        GEOIP_DOWNLOADER,
        true,
        args -> {
            List<Tuple<String, Metadata>> databases = (List<Tuple<String, Metadata>>) args[0];
            return new EnterpriseGeoIpTaskState(databases.stream().collect(Collectors.toMap(Tuple::v1, Tuple::v2)));
        }
    );

    static {
        PARSER.declareNamedObjects(constructorArg(), (p, c, name) -> Tuple.tuple(name, Metadata.fromXContent(p)), DATABASES);
    }

    public static EnterpriseGeoIpTaskState fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    private final Map<String, Metadata> databases;

    EnterpriseGeoIpTaskState(Map<String, Metadata> databases) {
        this.databases = Map.copyOf(databases);
    }

    EnterpriseGeoIpTaskState(StreamInput input) throws IOException {
        databases = input.readImmutableMap(
            in -> new Metadata(in.readLong(), in.readVInt(), in.readVInt(), in.readString(), in.readLong(), in.readOptionalString())
        );
    }

    public EnterpriseGeoIpTaskState put(String name, Metadata metadata) {
        HashMap<String, Metadata> newDatabases = new HashMap<>(databases);
        newDatabases.put(name, metadata);
        return new EnterpriseGeoIpTaskState(newDatabases);
    }

    public EnterpriseGeoIpTaskState remove(String name) {
        HashMap<String, Metadata> newDatabases = new HashMap<>(databases);
        newDatabases.remove(name);
        return new EnterpriseGeoIpTaskState(newDatabases);
    }

    public Map<String, Metadata> getDatabases() {
        return databases;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EnterpriseGeoIpTaskState that = (EnterpriseGeoIpTaskState) o;
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
        return "enterprise-geoip-downloader";
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_16_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(databases, (o, v) -> {
            o.writeLong(v.lastUpdate());
            o.writeVInt(v.firstChunk());
            o.writeVInt(v.lastChunk());
            o.writeString(v.md5());
            o.writeLong(v.lastCheck());
            o.writeOptionalString(v.sha256());
        });
    }

    /**
     * Retrieves the geoip downloader's task state from the cluster state. This may return null in some circumstances,
     * for example if the geoip downloader task hasn't been created yet (which it wouldn't be if it's disabled).
     *
     * @param state the cluster state to read the task state from
     * @return the geoip downloader's task state or null if there is not a state to read
     */
    @Nullable
    static EnterpriseGeoIpTaskState getEnterpriseGeoIpTaskState(ClusterState state) {
        PersistentTasksCustomMetadata.PersistentTask<?> task = getTaskWithId(state, EnterpriseGeoIpTask.ENTERPRISE_GEOIP_DOWNLOADER);
        return (task == null) ? null : (EnterpriseGeoIpTaskState) task.getState();
    }

}
