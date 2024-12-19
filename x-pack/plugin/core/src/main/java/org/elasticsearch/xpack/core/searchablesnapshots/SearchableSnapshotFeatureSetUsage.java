/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.searchablesnapshots;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.XPackFeatureUsage;
import org.elasticsearch.xpack.core.XPackField;

import java.io.IOException;
import java.util.Objects;

public class SearchableSnapshotFeatureSetUsage extends XPackFeatureUsage {

    private final int numberOfSearchableSnapshotIndices;
    private final int numberOfFullCopySearchableSnapshotIndices;
    private final int numberOfSharedCacheSearchableSnapshotIndices;

    public SearchableSnapshotFeatureSetUsage(StreamInput input) throws IOException {
        super(input);
        numberOfSearchableSnapshotIndices = input.readVInt();
        numberOfFullCopySearchableSnapshotIndices = input.readVInt();
        numberOfSharedCacheSearchableSnapshotIndices = input.readVInt();
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ZERO;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(numberOfSearchableSnapshotIndices);
        out.writeVInt(numberOfFullCopySearchableSnapshotIndices);
        out.writeVInt(numberOfSharedCacheSearchableSnapshotIndices);
    }

    public SearchableSnapshotFeatureSetUsage(
        boolean available,
        int numberOfFullCopySearchableSnapshotIndices,
        int numberOfSharedCacheSearchableSnapshotIndices
    ) {
        super(XPackField.SEARCHABLE_SNAPSHOTS, available, true);
        this.numberOfSearchableSnapshotIndices = numberOfFullCopySearchableSnapshotIndices + numberOfSharedCacheSearchableSnapshotIndices;
        this.numberOfFullCopySearchableSnapshotIndices = numberOfFullCopySearchableSnapshotIndices;
        this.numberOfSharedCacheSearchableSnapshotIndices = numberOfSharedCacheSearchableSnapshotIndices;
    }

    @Override
    protected void innerXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        super.innerXContent(builder, params);
        builder.field("indices_count", numberOfSearchableSnapshotIndices);
        builder.field("full_copy_indices_count", numberOfFullCopySearchableSnapshotIndices);
        builder.field("shared_cache_indices_count", numberOfSharedCacheSearchableSnapshotIndices);
    }

    public int getNumberOfSearchableSnapshotIndices() {
        return numberOfSearchableSnapshotIndices;
    }

    public int getNumberOfFullCopySearchableSnapshotIndices() {
        return numberOfFullCopySearchableSnapshotIndices;
    }

    public int getNumberOfSharedCacheSearchableSnapshotIndices() {
        return numberOfSharedCacheSearchableSnapshotIndices;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            available,
            enabled,
            numberOfSearchableSnapshotIndices,
            numberOfFullCopySearchableSnapshotIndices,
            numberOfSharedCacheSearchableSnapshotIndices
        );
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        SearchableSnapshotFeatureSetUsage other = (SearchableSnapshotFeatureSetUsage) obj;
        return available == other.available
            && enabled == other.enabled
            && numberOfSearchableSnapshotIndices == other.numberOfSearchableSnapshotIndices
            && numberOfFullCopySearchableSnapshotIndices == other.numberOfFullCopySearchableSnapshotIndices
            && numberOfSharedCacheSearchableSnapshotIndices == other.numberOfSharedCacheSearchableSnapshotIndices;
    }

}
