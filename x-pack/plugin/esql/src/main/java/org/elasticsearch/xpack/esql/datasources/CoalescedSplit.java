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

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * A composite split that groups multiple child splits into a single scheduling
 * unit. Reduces per-split overhead when thousands of tiny files (e.g. Iceberg
 * micro-partitions) would otherwise each become an independent work item.
 * Operators that encounter a {@code CoalescedSplit} iterate over its children
 * and process each one individually.
 */
public class CoalescedSplit implements ExternalSplit {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        ExternalSplit.class,
        "CoalescedSplit",
        CoalescedSplit::new
    );

    private final String sourceType;
    private final List<ExternalSplit> children;
    private final long estimatedSizeInBytes;

    public CoalescedSplit(String sourceType, List<ExternalSplit> children) {
        if (sourceType == null) {
            throw new IllegalArgumentException("sourceType cannot be null");
        }
        if (children == null || children.isEmpty()) {
            throw new IllegalArgumentException("children cannot be null or empty");
        }
        this.sourceType = sourceType;
        this.children = List.copyOf(children);
        this.estimatedSizeInBytes = computeEstimatedSize(this.children);
    }

    public CoalescedSplit(StreamInput in) throws IOException {
        this.sourceType = in.readString();
        this.children = in.readNamedWriteableCollectionAsList(ExternalSplit.class);
        this.estimatedSizeInBytes = computeEstimatedSize(this.children);
    }

    private static long computeEstimatedSize(List<ExternalSplit> children) {
        long total = 0;
        for (ExternalSplit child : children) {
            long childSize = child.estimatedSizeInBytes();
            if (childSize < 0) {
                return -1;
            }
            total = Math.addExact(total, childSize);
        }
        return total;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(sourceType);
        out.writeNamedWriteableCollection(children);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public String sourceType() {
        return sourceType;
    }

    public List<ExternalSplit> children() {
        return children;
    }

    @Override
    public long estimatedSizeInBytes() {
        return estimatedSizeInBytes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CoalescedSplit that = (CoalescedSplit) o;
        return Objects.equals(sourceType, that.sourceType) && Objects.equals(children, that.children);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceType, children);
    }

    @Override
    public String toString() {
        return "CoalescedSplit[sourceType="
            + sourceType
            + ", children="
            + children.size()
            + ", estimatedBytes="
            + estimatedSizeInBytes
            + "]";
    }
}
