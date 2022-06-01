/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.features;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.indices.AssociatedIndexDescriptor;
import org.elasticsearch.indices.SystemDataStreamDescriptor;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class GetSnapshottableFeaturesResponse extends ActionResponse implements ToXContentObject {

    private final List<SnapshottableFeature> snapshottableFeatures;

    public GetSnapshottableFeaturesResponse(List<SnapshottableFeature> features) {
        this.snapshottableFeatures = Collections.unmodifiableList(features);
    }

    public GetSnapshottableFeaturesResponse(StreamInput in) throws IOException {
        super(in);
        snapshottableFeatures = in.readList(SnapshottableFeature::new);
    }

    public List<SnapshottableFeature> getSnapshottableFeatures() {
        return snapshottableFeatures;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(snapshottableFeatures);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.startArray("features");
            for (SnapshottableFeature feature : snapshottableFeatures) {
                builder.value(feature);
            }
            builder.endArray();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if ((o instanceof GetSnapshottableFeaturesResponse) == false) return false;
        GetSnapshottableFeaturesResponse that = (GetSnapshottableFeaturesResponse) o;
        return snapshottableFeatures.equals(that.snapshottableFeatures);
    }

    @Override
    public int hashCode() {
        return Objects.hash(snapshottableFeatures);
    }

    public static class SnapshottableFeature implements Writeable, ToXContentObject {

        private final String featureName;
        private final String description;
        private final Integer descriptorSize;
        private final String associatedIndexDescriptors;
        private final String SystemDatastreamDescriptors;

        public SnapshottableFeature(String featureName, String description, Integer size, String associatedIndexDescriptors, String systemDataStreamDescriptors) {
            this.featureName = featureName;
            this.description = description;
            this.descriptorSize = size;
            this.associatedIndexDescriptors = associatedIndexDescriptors;
            this.SystemDatastreamDescriptors = systemDataStreamDescriptors;
        }

        public SnapshottableFeature(StreamInput in) throws IOException {
            featureName = in.readString();
            description = in.readString();
            descriptorSize = in.readInt();
            associatedIndexDescriptors = in.readString();
            SystemDatastreamDescriptors = in.readString();
        }

        public String getFeatureName() {
            return featureName;
        }

        public String getDescription() {
            return description;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(featureName);
            out.writeString(description);
            out.writeInt(descriptorSize);
            out.writeString(associatedIndexDescriptors);
            out.writeString(SystemDatastreamDescriptors);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("name", featureName);
            builder.field("description", description);
            builder.field("descriptor_size", descriptorSize);
            builder.field("associated_index_descriptors", associatedIndexDescriptors);
            builder.field("system_datastream_descriptors", SystemDatastreamDescriptors);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if ((o instanceof SnapshottableFeature) == false) return false;
            SnapshottableFeature feature = (SnapshottableFeature) o;
            return Objects.equals(getFeatureName(), feature.getFeatureName());
        }

        @Override
        public int hashCode() {
            return Objects.hash(getFeatureName());
        }
    }
}
