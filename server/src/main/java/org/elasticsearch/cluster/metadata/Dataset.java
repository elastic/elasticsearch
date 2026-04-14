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
 * Datasets inherit credentials from their parent dataSource at query time.
 *
 * <p>Dataset settings are plain values ({@code Map<String, Object>}) — no secrets.
 * Credentials are always inherited from the parent {@link DataSource}.
 */
public final class Dataset implements Writeable, ToXContentObject, IndexAbstraction {

    private static final ParseField NAME = new ParseField("name");
    private static final ParseField DATASOURCE = new ParseField("dataSource");
    private static final ParseField RESOURCE = new ParseField("resource");
    private static final ParseField DESCRIPTION = new ParseField("description");
    private static final ParseField SETTINGS = new ParseField("settings");

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<Dataset, Void> PARSER = new ConstructingObjectParser<>(
        "dataset",
        false,
        (args, ctx) -> new Dataset(
            (String) args[0],
            (String) args[1],
            (String) args[2],
            (String) args[3],
            args[4] != null ? (Map<String, Object>) args[4] : Map.of()
        )
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), NAME);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), DATASOURCE);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), RESOURCE);
        PARSER.declareStringOrNull(ConstructingObjectParser.optionalConstructorArg(), DESCRIPTION);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> p.map(), SETTINGS);
    }

    private final String name;
    private final String dataSource;
    private final String resource;
    private final String description;
    private final Map<String, Object> settings;

    public Dataset(String name, String dataSource, String resource, String description, Map<String, Object> settings) {
        this.name = Objects.requireNonNull(name, "name must not be null");
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
        this.resource = Objects.requireNonNull(resource, "resource must not be null");
        this.description = description;
        this.settings = settings != null ? Collections.unmodifiableMap(settings) : Map.of();
    }

    public Dataset(StreamInput in) throws IOException {
        this.name = in.readString();
        this.dataSource = in.readString();
        this.resource = in.readString();
        this.description = in.readOptionalString();
        // readMap returns a mutable HashMap when non-empty; wrap to preserve the class invariant that settings is unmodifiable
        this.settings = Collections.unmodifiableMap(in.readMap(StreamInput::readGenericValue));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(dataSource);
        out.writeString(resource);
        out.writeOptionalString(description);
        out.writeMap(settings, StreamOutput::writeGenericValue);
    }

    public String name() {
        return name;
    }

    public String dataSource() {
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
        builder.field(DATASOURCE.getPreferredName(), dataSource);
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
