/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.searchablesnapshots;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;

import java.io.IOException;
import java.util.Objects;

public class SearchableSnapshotFeatureSetUsage extends XPackFeatureSet.Usage {

    private final int numberOfSearchableSnapshotIndices;
    private final int numberOfFullCopySearchableSnapshotIndices;
    private final int numberOfSharedCacheSearchableSnapshotIndices;

    public SearchableSnapshotFeatureSetUsage(StreamInput input) throws IOException {
        super(input);
        numberOfSearchableSnapshotIndices = input.readVInt();
        if (input.getVersion().onOrAfter(Version.V_7_13_0)) {
            numberOfFullCopySearchableSnapshotIndices = input.readVInt();
            numberOfSharedCacheSearchableSnapshotIndices = input.readVInt();
        } else {
            numberOfFullCopySearchableSnapshotIndices = 0;
            numberOfSharedCacheSearchableSnapshotIndices = 0;
        }
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_7_9_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(numberOfSearchableSnapshotIndices);
        if (out.getVersion().onOrAfter(Version.V_7_13_0)) {
            out.writeVInt(numberOfFullCopySearchableSnapshotIndices);
            out.writeVInt(numberOfSharedCacheSearchableSnapshotIndices);
        }
    }

    public SearchableSnapshotFeatureSetUsage(boolean available,
                                             int numberOfFullCopySearchableSnapshotIndices,
                                             int numberOfSharedCacheSearchableSnapshotIndices) {
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
        return Objects.hash(available, enabled, numberOfSearchableSnapshotIndices, numberOfFullCopySearchableSnapshotIndices,
            numberOfSharedCacheSearchableSnapshotIndices);
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
        return available == other.available &&
            enabled == other.enabled &&
            numberOfSearchableSnapshotIndices == other.numberOfSearchableSnapshotIndices &&
            numberOfFullCopySearchableSnapshotIndices == other.numberOfFullCopySearchableSnapshotIndices &&
            numberOfSharedCacheSearchableSnapshotIndices == other.numberOfSharedCacheSearchableSnapshotIndices;
    }

}
