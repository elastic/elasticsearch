/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
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

    public SearchableSnapshotFeatureSetUsage(StreamInput input) throws IOException {
        super(input);
        numberOfSearchableSnapshotIndices = input.readVInt();
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_7_9_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(numberOfSearchableSnapshotIndices);
    }

    public SearchableSnapshotFeatureSetUsage(boolean available,
                                             int numberOfSearchableSnapshotIndices) {
        super(XPackField.SEARCHABLE_SNAPSHOTS, available, true);
        this.numberOfSearchableSnapshotIndices = numberOfSearchableSnapshotIndices;
    }

    @Override
    protected void innerXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        super.innerXContent(builder, params);
        builder.field("indices_count", numberOfSearchableSnapshotIndices);
    }

    public int getNumberOfSearchableSnapshotIndices() {
        return numberOfSearchableSnapshotIndices;
    }

    @Override
    public int hashCode() {
        return Objects.hash(available, enabled, numberOfSearchableSnapshotIndices);
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
        return Objects.equals(available, other.available) &&
            Objects.equals(enabled, other.enabled) &&
            Objects.equals(numberOfSearchableSnapshotIndices, other.numberOfSearchableSnapshotIndices);
    }
}
