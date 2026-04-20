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
 * Encapsulates dataset definitions as custom metadata inside ProjectMetadata within cluster state.
 * Datasets participate in the index namespace (via {@link IndexAbstraction.Type#DATASET}).
 */
public final class DatasetMetadata extends AbstractNamedDiffable<Metadata.ProjectCustom> implements Metadata.ProjectCustom {

    public static final String TYPE = "esql_dataset";
    public static final List<NamedWriteableRegistry.Entry> ENTRIES = List.of(
        new NamedWriteableRegistry.Entry(Metadata.ProjectCustom.class, TYPE, DatasetMetadata::readFromStream),
        new NamedWriteableRegistry.Entry(NamedDiff.class, TYPE, in -> DatasetMetadata.readDiffFrom(Metadata.ProjectCustom.class, TYPE, in))
    );
    public static final DatasetMetadata EMPTY = new DatasetMetadata(Collections.emptyMap());

    private static final ParseField DATASETS = new ParseField("datasets");

    private final Map<String, Dataset> datasets;

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<DatasetMetadata, Void> PARSER = new ConstructingObjectParser<>(
        "dataset_metadata",
        true,
        (args, ctx) -> new DatasetMetadata((Map<String, Dataset>) args[0])
    );

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> {
            Map<String, Dataset> datasets = new HashMap<>();
            while (p.nextToken() != XContentParser.Token.END_OBJECT) {
                String name = p.currentName();
                datasets.put(name, Dataset.fromXContent(p));
            }
            return datasets;
        }, DATASETS);
    }

    public static DatasetMetadata fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public static DatasetMetadata readFromStream(StreamInput in) throws IOException {
        return new DatasetMetadata(in.readMap(Dataset::new));
    }

    public DatasetMetadata(Map<String, Dataset> datasets) {
        assert datasets.entrySet().stream().allMatch(e -> e.getKey().equals(e.getValue().name()))
            : "DatasetMetadata map key must match Dataset.name(): " + datasets;
        this.datasets = Collections.unmodifiableMap(datasets);
    }

    public Map<String, Dataset> datasets() {
        return datasets;
    }

    @Nullable
    public Dataset get(String name) {
        return datasets.get(name);
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        // API + GATEWAY. Datasets carry no secrets (credentials live on the parent data source), so full API exposure
        // is intentional. SNAPSHOT is excluded to stay consistent with DataSourceMetadata: restoring datasets without
        // their data sources would leave dangling references. Snapshot support is tracked as a future milestone.
        return EnumSet.of(Metadata.XContentContext.API, Metadata.XContentContext.GATEWAY);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        // Shared with DataSourceMetadata — both metadata containers ship together
        return DataSourceMetadata.ESQL_DATASOURCES;
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(this.datasets, StreamOutput::writeWriteable);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params ignored) {
        return ChunkedToXContentHelper.xContentObjectFields(DATASETS.getPreferredName(), datasets);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DatasetMetadata that = (DatasetMetadata) o;
        return Objects.equals(datasets, that.datasets);
    }

    @Override
    public int hashCode() {
        return Objects.hash(datasets);
    }

    @Override
    public String toString() {
        return "DatasetMetadata{count=" + datasets.size() + ", names=" + datasets.keySet() + "}";
    }

    public static DatasetMetadata get(ProjectMetadata project) {
        return project.custom(TYPE, EMPTY);
    }
}
