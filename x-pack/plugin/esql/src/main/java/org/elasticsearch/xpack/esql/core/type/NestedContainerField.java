/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.type;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The column that carries the child documents of a nested object through the plan: one value per parent document, the child
 * fields serialized by {@code NestedContainerCodec}. It names the nested {@link #path()} and, for each child field read, its
 * path relative to it and its type, which is all a node needs to read or decode the column.
 */
public final class NestedContainerField extends EsField {
    public static final String NAME_PREFIX = "$$nested_container.";
    public static final TransportVersion VERSION = TransportVersion.fromName("esql_nested_container");

    private final String path;
    private final List<String> paths;
    private final List<DataType> types;

    public NestedContainerField(String name, String path, List<String> paths, List<DataType> types) {
        super(name, DataType.SOURCE, Map.of(), false, TimeSeriesFieldType.NONE);
        if (paths.size() != types.size()) {
            throw new IllegalArgumentException("a container names one type for each child path");
        }
        this.path = path;
        this.paths = List.copyOf(paths);
        this.types = List.copyOf(types);
    }

    public NestedContainerField(StreamInput in) throws IOException {
        super(in);
        this.path = in.readString();
        this.paths = in.readStringCollectionAsList();
        this.types = in.readCollectionAsList(DataType::readFrom);
    }

    @Override
    public void writeContent(StreamOutput out) throws IOException {
        if (out.getTransportVersion().supports(VERSION) == false) {
            throw new IOException("nested containers need transport version [" + VERSION + "]");
        }
        super.writeContent(out);
        out.writeString(path);
        out.writeStringCollection(paths);
        out.writeCollection(types, (o, t) -> t.writeTo(o));
    }

    @Override
    public String getWriteableName(TransportVersion transportVersion) {
        return "NestedContainerField";
    }

    public String path() {
        return path;
    }

    /** The path of each child field relative to {@link #path()}. */
    public List<String> paths() {
        return paths;
    }

    public List<DataType> types() {
        return types;
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj) == false) {
            return false;
        }
        NestedContainerField other = (NestedContainerField) obj;
        return path.equals(other.path) && paths.equals(other.paths) && types.equals(other.types);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), path, paths, types);
    }
}
