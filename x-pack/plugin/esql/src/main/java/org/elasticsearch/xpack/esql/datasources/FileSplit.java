/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a byte range within a file for a file-based external source.
 * A single file may map to one or more splits. Carries Hive partition
 * key-value pairs extracted from the file path for partition pruning.
 */
public class FileSplit implements ExternalSplit {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        ExternalSplit.class,
        "FileSplit",
        FileSplit::new
    );

    private final String sourceType;
    private final StoragePath path;
    private final long offset;
    private final long length;
    private final String format;
    private final Map<String, Object> config;
    private final Map<String, Object> partitionValues;

    public FileSplit(
        String sourceType,
        StoragePath path,
        long offset,
        long length,
        String format,
        Map<String, Object> config,
        Map<String, Object> partitionValues
    ) {
        if (sourceType == null) {
            throw new IllegalArgumentException("sourceType cannot be null");
        }
        if (path == null) {
            throw new IllegalArgumentException("path cannot be null");
        }
        this.sourceType = sourceType;
        this.path = path;
        this.offset = offset;
        this.length = length;
        this.format = format;
        this.config = config != null ? Map.copyOf(config) : Map.of();
        this.partitionValues = partitionValues != null && partitionValues.isEmpty() == false
            ? Collections.unmodifiableMap(new LinkedHashMap<>(partitionValues))
            : Map.of();
    }

    public FileSplit(StreamInput in) throws IOException {
        this.sourceType = in.readString();
        this.path = StoragePath.of(in.readString());
        this.offset = in.readVLong();
        this.length = in.readVLong();
        this.format = in.readOptionalString();
        this.config = in.readGenericMap();
        this.partitionValues = in.readGenericMap();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(sourceType);
        out.writeString(path.toString());
        out.writeVLong(offset);
        out.writeVLong(length);
        out.writeOptionalString(format);
        out.writeGenericMap(config);
        out.writeGenericMap(partitionValues);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public String sourceType() {
        return sourceType;
    }

    public StoragePath path() {
        return path;
    }

    public long offset() {
        return offset;
    }

    public long length() {
        return length;
    }

    public String format() {
        return format;
    }

    public Map<String, Object> config() {
        return config;
    }

    public Map<String, Object> partitionValues() {
        return partitionValues;
    }

    @Override
    public long estimatedSizeInBytes() {
        return length;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FileSplit that = (FileSplit) o;
        return offset == that.offset
            && length == that.length
            && Objects.equals(sourceType, that.sourceType)
            && Objects.equals(path, that.path)
            && Objects.equals(format, that.format)
            && Objects.equals(config, that.config)
            && Objects.equals(partitionValues, that.partitionValues);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceType, path, offset, length, format, config, partitionValues);
    }

    @Override
    public String toString() {
        return "FileSplit[" + path + ", offset=" + offset + ", length=" + length + ", partitions=" + partitionValues + "]";
    }
}
