/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Encapsulates data source definitions as custom metadata inside ProjectMetadata within cluster state.
 */
public final class DataSourceMetadata extends AbstractNamedDiffable<Metadata.ProjectCustom> implements Metadata.ProjectCustom {

    public static final String TYPE = "esql_datasource";
    public static final List<NamedWriteableRegistry.Entry> ENTRIES = List.of(
        new NamedWriteableRegistry.Entry(Metadata.ProjectCustom.class, TYPE, DataSourceMetadata::readFromStream),
        new NamedWriteableRegistry.Entry(
            NamedDiff.class,
            TYPE,
            in -> DataSourceMetadata.readDiffFrom(Metadata.ProjectCustom.class, TYPE, in)
        )
    );
    public static final DataSourceMetadata EMPTY = new DataSourceMetadata(Collections.emptyMap());

    /**
     * Feature flag that gates the ES|QL external data sources feature end-to-end: metadata registration,
     * dataset-aware error messages, collision detection preambles, and the downstream CRUD/EXTERNAL surfaces.
     * Kept here (on the server-side foundational class) so that both {@code server} and {@code x-pack} consumers
     * can reference a single source of truth. System property: {@code es.esql_external_datasources_feature_flag_enabled}.
     */
    public static final FeatureFlag ESQL_EXTERNAL_DATASOURCES_FEATURE_FLAG = new FeatureFlag("esql_external_datasources");

    /**
     * Shared transport version for {@link DataSourceMetadata} and {@link DatasetMetadata}. Both metadata
     * containers are introduced together and evolve together, so they share a single version gate.
     */
    static final TransportVersion ESQL_DATASOURCES = TransportVersion.fromName("esql_datasources");

    private static final ParseField DATA_SOURCES = new ParseField("data_sources");

    private final Map<String, DataSource> dataSources;

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<DataSourceMetadata, Void> PARSER = new ConstructingObjectParser<>(
        "data_source_metadata",
        true,
        (args, ctx) -> new DataSourceMetadata((Map<String, DataSource>) args[0])
    );

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> {
            Map<String, DataSource> dataSources = new HashMap<>();
            while (p.nextToken() != XContentParser.Token.END_OBJECT) {
                String name = p.currentName();
                dataSources.put(name, DataSource.fromXContent(p));
            }
            return dataSources;
        }, DATA_SOURCES);
    }

    public static DataSourceMetadata fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public static DataSourceMetadata readFromStream(StreamInput in) throws IOException {
        return new DataSourceMetadata(in.readMap(DataSource::new));
    }

    public DataSourceMetadata(Map<String, DataSource> dataSources) {
        assert dataSources.entrySet().stream().allMatch(e -> e.getKey().equals(e.getValue().name()))
            : "DataSourceMetadata map key must match DataSource.name(): " + dataSources;
        this.dataSources = Collections.unmodifiableMap(dataSources);
    }

    public Map<String, DataSource> dataSources() {
        return dataSources;
    }

    @Nullable
    public DataSource get(String name) {
        return dataSources.get(name);
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        // GATEWAY only. API is excluded because the raw XContent contains plaintext secret setting values, which must
        // not appear in GET /_cluster/state; the CRUD REST layer exposes data sources separately via a masked path
        // (presentationValue). SNAPSHOT is excluded because snapshot restore has no mechanism to re-provision secrets,
        // so restoring a cluster state containing data sources would produce unusable configurations. Snapshot support
        // is tracked as a future milestone and will require a key-availability story for restore.
        return EnumSet.of(Metadata.XContentContext.GATEWAY);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return ESQL_DATASOURCES;
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(this.dataSources, StreamOutput::writeWriteable);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params ignored) {
        return ChunkedToXContentHelper.xContentObjectFields(DATA_SOURCES.getPreferredName(), dataSources);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataSourceMetadata that = (DataSourceMetadata) o;
        return Objects.equals(dataSources, that.dataSources);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataSources);
    }

    @Override
    public String toString() {
        // Names only — values can contain secrets and must never appear in toString. For per-setting presentation
        // output, callers should use DataSource.toPresentationMap() explicitly.
        return "DataSourceMetadata{count=" + dataSources.size() + ", names=" + dataSources.keySet() + "}";
    }

    public static DataSourceMetadata get(ProjectMetadata project) {
        return project.custom(TYPE, EMPTY);
    }
}
