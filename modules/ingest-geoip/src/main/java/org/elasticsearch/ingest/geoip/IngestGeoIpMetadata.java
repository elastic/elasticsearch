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
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.ingest.geoip.direct.DatabaseConfigurationMetadata;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Holds the ingest-geoip databases that are available in the cluster state.
 */
public final class IngestGeoIpMetadata implements Metadata.ProjectCustom {

    public static final String TYPE = "ingest_geoip";
    private static final ParseField DATABASES_FIELD = new ParseField("databases");

    public static final IngestGeoIpMetadata EMPTY = new IngestGeoIpMetadata(Map.of());

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<IngestGeoIpMetadata, Void> PARSER = new ConstructingObjectParser<>(
        TYPE,
        a -> new IngestGeoIpMetadata(
            ((List<DatabaseConfigurationMetadata>) a[0]).stream().collect(Collectors.toMap((m) -> m.database().id(), Function.identity()))
        )
    );
    static {
        PARSER.declareNamedObjects(ConstructingObjectParser.constructorArg(), (p, c, n) -> DatabaseConfigurationMetadata.parse(p, n), v -> {
            throw new IllegalArgumentException("ordered " + DATABASES_FIELD.getPreferredName() + " are not supported");
        }, DATABASES_FIELD);
    }

    private final Map<String, DatabaseConfigurationMetadata> databases;

    public IngestGeoIpMetadata(Map<String, DatabaseConfigurationMetadata> databases) {
        this.databases = Map.copyOf(databases);
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_16_0;
    }

    public Map<String, DatabaseConfigurationMetadata> getDatabases() {
        return databases;
    }

    public IngestGeoIpMetadata(StreamInput in) throws IOException {
        this.databases = in.readMap(StreamInput::readString, DatabaseConfigurationMetadata::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(databases, StreamOutput::writeWriteable);
    }

    public static IngestGeoIpMetadata fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params ignored) {
        return ChunkedToXContentHelper.xContentObjectFields(DATABASES_FIELD.getPreferredName(), databases);
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return Metadata.ALL_CONTEXTS;
    }

    @Override
    public Diff<Metadata.ProjectCustom> diff(Metadata.ProjectCustom before) {
        return new GeoIpMetadataDiff((IngestGeoIpMetadata) before, this);
    }

    static class GeoIpMetadataDiff implements NamedDiff<Metadata.ProjectCustom> {

        final Diff<Map<String, DatabaseConfigurationMetadata>> databases;

        GeoIpMetadataDiff(IngestGeoIpMetadata before, IngestGeoIpMetadata after) {
            this.databases = DiffableUtils.diff(before.databases, after.databases, DiffableUtils.getStringKeySerializer());
        }

        GeoIpMetadataDiff(StreamInput in) throws IOException {
            databases = DiffableUtils.readJdkMapDiff(
                in,
                DiffableUtils.getStringKeySerializer(),
                DatabaseConfigurationMetadata::new,
                DatabaseConfigurationMetadata::readDiffFrom
            );
        }

        @Override
        public Metadata.ProjectCustom apply(Metadata.ProjectCustom part) {
            return new IngestGeoIpMetadata(databases.apply(((IngestGeoIpMetadata) part).databases));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            databases.writeTo(out);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersions.V_8_16_0;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IngestGeoIpMetadata that = (IngestGeoIpMetadata) o;
        return Objects.equals(databases, that.databases);
    }

    @Override
    public int hashCode() {
        return Objects.hash(databases);
    }
}
