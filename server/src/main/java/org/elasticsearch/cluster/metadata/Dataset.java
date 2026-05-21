/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.Index;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a dataset definition stored in cluster state. A dataset is a named reference
 * to external data that participates in the index namespace alongside indices, aliases, and views.
 * Datasets inherit credentials from their parent data source at query time.
 *
 * <p>Dataset settings are plain values ({@code Map<String, Object>}) — no secrets.
 * Credentials are always inherited from the parent {@link DataSource}.
 *
 * <p><b>Referential integrity.</b> The {@link #dataSource} field is a name reference, not an
 * embedded object — the data source it points to lives in {@link DataSourceMetadata}, a separate
 * cluster-state container with an independent update lifecycle. Integrity (i.e., the referenced
 * data source actually exists) is enforced at the service layer when datasets are created or
 * deleted, not by this class. A cluster state that arrives via gateway recovery or rollback could
 * in principle carry a dangling reference; query-time resolution is responsible for surfacing a
 * clear error in that case.
 */
public final class Dataset implements Writeable, ToXContentObject, IndexAbstraction {

    private static final ParseField NAME = new ParseField("name");
    private static final ParseField DATASOURCE = new ParseField("data_source");
    private static final ParseField RESOURCE = new ParseField("resource");
    private static final ParseField DESCRIPTION = new ParseField("description");
    private static final ParseField SETTINGS = new ParseField("settings");

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<Dataset, Void> PARSER = new ConstructingObjectParser<>(
        "dataset",
        false,
        (args, ctx) -> new Dataset(
            (String) args[0],
            new DataSourceReference((String) args[1]),
            (String) args[2],
            (String) args[3],
            args[4] != null ? (Map<String, Object>) args[4] : Map.of()
        )
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), NAME);
        // The `data_source` field is emitted as a bare string — DataSourceReference is a single-field wrapper today;
        // nested JSON would be verbose for users and there is no concrete near-term field to add to the reference.
        // The wrapper provides compile-time type discipline in Java without imposing extra structure on the JSON shape.
        PARSER.declareString(ConstructingObjectParser.constructorArg(), DATASOURCE);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), RESOURCE);
        PARSER.declareStringOrNull(ConstructingObjectParser.optionalConstructorArg(), DESCRIPTION);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> p.map(), SETTINGS);
    }

    private final String name;
    private final DataSourceReference dataSource;
    private final String resource;
    private final String description;
    private final Map<String, Object> settings;

    public Dataset(
        String name,
        DataSourceReference dataSource,
        String resource,
        @Nullable String description,
        Map<String, Object> settings
    ) {
        this.name = Objects.requireNonNull(name, "name must not be null");
        this.dataSource = Objects.requireNonNull(dataSource, "data source must not be null");
        this.resource = Objects.requireNonNull(resource, "resource must not be null");
        this.description = description;
        this.settings = settings != null ? Collections.unmodifiableMap(settings) : Map.of();
    }

    public Dataset(StreamInput in) throws IOException {
        this.name = in.readString();
        this.dataSource = new DataSourceReference(in);
        this.resource = in.readString();
        this.description = in.readOptionalString();
        // readMap returns a mutable HashMap when non-empty; wrap to preserve the class invariant that settings is unmodifiable
        this.settings = Collections.unmodifiableMap(in.readMap(StreamInput::readGenericValue));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        dataSource.writeTo(out);
        out.writeString(resource);
        out.writeOptionalString(description);
        out.writeMap(settings, StreamOutput::writeGenericValue);
    }

    public String name() {
        return name;
    }

    public DataSourceReference dataSource() {
        return dataSource;
    }

    public String resource() {
        return resource;
    }

    public String description() {
        return description;
    }

    public Map<String, Object> settings() {
        return settings;
    }

    public static Dataset fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(NAME.getPreferredName(), name);
        builder.field(DATASOURCE.getPreferredName(), dataSource.getName());
        builder.field(RESOURCE.getPreferredName(), resource);
        if (description != null) {
            builder.field(DESCRIPTION.getPreferredName(), description);
        }
        if (settings.isEmpty() == false) {
            builder.field(SETTINGS.getPreferredName(), settings);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public Type getType() {
        return Type.DATASET;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public List<Index> getIndices() {
        return List.of();
    }

    @Override
    public Index getWriteIndex() {
        return null;
    }

    @Override
    public DataStream getParentDataStream() {
        return null;
    }

    @Override
    public boolean isHidden() {
        return false;
    }

    @Override
    public boolean isSystem() {
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Dataset that = (Dataset) o;
        return Objects.equals(name, that.name)
            && Objects.equals(dataSource, that.dataSource)
            && Objects.equals(resource, that.resource)
            && Objects.equals(description, that.description)
            && Objects.equals(settings, that.settings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, dataSource, resource, description, settings);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
