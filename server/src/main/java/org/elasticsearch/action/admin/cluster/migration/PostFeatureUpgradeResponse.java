/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.migration;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class PostFeatureUpgradeResponse extends ActionResponse implements ToXContentObject {

    private final boolean accepted;
    private final List<Feature> features;
    private final String reason;
    private final ElasticsearchException elasticsearchException;

    public PostFeatureUpgradeResponse(boolean accepted, List<Feature> features, String reason, ElasticsearchException exception) {
        this.accepted = accepted;
        this.features = features;
        this.reason = reason;
        this.elasticsearchException = exception;
    }

    public PostFeatureUpgradeResponse(StreamInput in) throws IOException {
        super(in);
        this.accepted = in.readBoolean();
        this.features = in.readList(Feature::new);
        this.reason = in.readOptionalString();
        this.elasticsearchException = in.readOptionalWriteable(ElasticsearchException::new);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("acknowledged", this.accepted);
        if (accepted) {
            builder.startArray("features");
            for (Feature feature : this.features) {
                feature.toXContent(builder, params);
            }
            builder.endArray();
        }
        if (Objects.nonNull(this.reason)) {
            builder.field("reason", this.reason);
        }
        if (Objects.nonNull(this.elasticsearchException)) {
            builder.field("exception", elasticsearchException);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(this.accepted);
        out.writeList(this.features);
        out.writeOptionalString(this.reason);
        out.writeOptionalWriteable(this.elasticsearchException);
    }

    public boolean isAccepted() {
        return accepted;
    }

    public List<Feature> getFeatures() {
        return features;
    }

    public String getReason() {
        return reason;
    }

    public ElasticsearchException getElasticsearchException() {
        return elasticsearchException;
    }

    /**
     * We disregard exceptions when determining response equality
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PostFeatureUpgradeResponse that = (PostFeatureUpgradeResponse) o;
        return accepted == that.accepted
            && Objects.equals(features, that.features)
            && Objects.equals(reason, that.reason);
    }

    /**
     * We disregard exceptions when calculating hash code
     */
    @Override
    public int hashCode() {
        return Objects.hash(accepted, features, reason);
    }

    @Override
    public String toString() {
        return "PostFeatureUpgradeResponse{" +
            "accepted=" + accepted +
            ", features=" + features +
            ", reason='" + reason + '\'' +
            ", elasticsearchException=" + elasticsearchException +
            '}';
    }

    public static class Feature implements Writeable, ToXContentObject {
        private final String featureName;

        public Feature(String featureName) {
            this.featureName = featureName;
        }

        public Feature(StreamInput in) throws IOException {
            this.featureName = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(featureName);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("feature_name", this.featureName);
            builder.endObject();
            return builder;
        }

        public String getFeatureName() {
            return featureName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Feature feature = (Feature) o;
            return Objects.equals(featureName, feature.featureName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(featureName);
        }

        @Override
        public String toString() {
            return "Feature{" +
                "featureName='" + featureName + '\'' +
                '}';
        }
    }
}
