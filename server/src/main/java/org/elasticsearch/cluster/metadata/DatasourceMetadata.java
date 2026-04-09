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
 * Encapsulates datasource definitions as custom metadata inside ProjectMetadata within cluster state.
 * Follows the same pattern as {@link ViewMetadata}.
 */
public final class DatasourceMetadata extends AbstractNamedDiffable<Metadata.ProjectCustom> implements Metadata.ProjectCustom {

    public static final String TYPE = "esql_datasource";
    public static final List<NamedWriteableRegistry.Entry> ENTRIES = List.of(
        new NamedWriteableRegistry.Entry(Metadata.ProjectCustom.class, TYPE, DatasourceMetadata::readFromStream),
        new NamedWriteableRegistry.Entry(
            NamedDiff.class,
            TYPE,
            in -> DatasourceMetadata.readDiffFrom(Metadata.ProjectCustom.class, TYPE, in)
        )
    );
    public static final DatasourceMetadata EMPTY = new DatasourceMetadata(Collections.emptyMap());

    private static final TransportVersion ESQL_DATASOURCES = TransportVersion.fromName("esql_datasources");
    private static final ParseField DATASOURCES = new ParseField("datasources");

    private final Map<String, Datasource> datasources;

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<DatasourceMetadata, Void> PARSER = new ConstructingObjectParser<>(
        "datasource_metadata",
        true,
        (args, ctx) -> new DatasourceMetadata((Map<String, Datasource>) args[0])
    );

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> {
            Map<String, Datasource> datasources = new HashMap<>();
            while (p.nextToken() != XContentParser.Token.END_OBJECT) {
                String name = p.currentName();
                datasources.put(name, Datasource.fromXContent(p));
            }
            return datasources;
        }, DATASOURCES);
    }

    public static DatasourceMetadata fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public static DatasourceMetadata readFromStream(StreamInput in) throws IOException {
        return new DatasourceMetadata(in.readMap(Datasource::new));
    }

    public DatasourceMetadata(Map<String, Datasource> datasources) {
        this.datasources = Collections.unmodifiableMap(datasources);
    }

    public Map<String, Datasource> datasources() {
        return datasources;
    }

    @Nullable
    public Datasource get(String name) {
        return datasources.get(name);
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return Metadata.ALL_CONTEXTS;
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
        out.writeMap(this.datasources, StreamOutput::writeWriteable);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params ignored) {
        return ChunkedToXContentHelper.xContentObjectFields(DATASOURCES.getPreferredName(), datasources);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DatasourceMetadata that = (DatasourceMetadata) o;
        return Objects.equals(datasources, that.datasources);
    }

    @Override
    public int hashCode() {
        return Objects.hash(datasources);
    }

    public static DatasourceMetadata get(ProjectMetadata project) {
        return project.custom(TYPE, EMPTY);
    }
}
