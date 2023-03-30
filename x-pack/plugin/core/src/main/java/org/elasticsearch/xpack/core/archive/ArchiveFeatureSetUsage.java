/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.archive;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;

import java.io.IOException;
import java.util.Objects;

public class ArchiveFeatureSetUsage extends XPackFeatureSet.Usage {

    private final int numberOfArchiveIndices;

    public ArchiveFeatureSetUsage(StreamInput input) throws IOException {
        super(input);
        numberOfArchiveIndices = input.readVInt();
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.V_8_3_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(numberOfArchiveIndices);
    }

    public ArchiveFeatureSetUsage(boolean available, int numberOfArchiveIndices) {
        super(XPackField.ARCHIVE, available, true);
        this.numberOfArchiveIndices = numberOfArchiveIndices;
    }

    public int getNumberOfArchiveIndices() {
        return numberOfArchiveIndices;
    }

    @Override
    protected void innerXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        super.innerXContent(builder, params);
        builder.field("indices_count", numberOfArchiveIndices);
    }

    @Override
    public int hashCode() {
        return Objects.hash(available, enabled, numberOfArchiveIndices);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ArchiveFeatureSetUsage other = (ArchiveFeatureSetUsage) obj;
        return available == other.available && enabled == other.enabled && numberOfArchiveIndices == other.numberOfArchiveIndices;
    }

}
