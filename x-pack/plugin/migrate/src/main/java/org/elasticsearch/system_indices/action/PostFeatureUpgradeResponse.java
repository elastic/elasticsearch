/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.system_indices.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * The response to return to a request for a system feature upgrade
 */
public class PostFeatureUpgradeResponse extends ActionResponse implements ToXContentObject {

    private final boolean accepted;
    private final List<Feature> features;
    @Nullable
    private final String reason;
    @Nullable
    private final ElasticsearchException elasticsearchException;

    /**
     * @param accepted Whether the upgrade request is accepted by the server
     * @param features A list of the features that will be upgraded
     * @param reason If the upgrade is rejected, the reason for rejection. Null otherwise.
     * @param exception If the upgrade is rejected because of an exception, the exception. Null otherwise.
     */
    public PostFeatureUpgradeResponse(
        boolean accepted,
        List<Feature> features,
        @Nullable String reason,
        @Nullable ElasticsearchException exception
    ) {
        this.accepted = accepted;
        this.features = Objects.nonNull(features) ? features : Collections.emptyList();
        this.reason = reason;
        this.elasticsearchException = exception;
    }

    /**
     * @param in A stream input for a serialized response object
     * @throws IOException if we can't deserialize the object
     */
    public PostFeatureUpgradeResponse(StreamInput in) throws IOException {
        this.accepted = in.readBoolean();
        this.features = in.readCollectionAsImmutableList(Feature::new);
        this.reason = in.readOptionalString();
        this.elasticsearchException = in.readOptionalWriteable(ElasticsearchException::new);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("accepted", this.accepted);
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
            builder.field("exception");
            builder.startObject();
            elasticsearchException.toXContent(builder, params);
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(this.accepted);
        out.writeCollection(this.features);
        out.writeOptionalString(this.reason);
        out.writeOptionalWriteable(this.elasticsearchException);
    }

    public boolean isAccepted() {
        return accepted;
    }

    public List<Feature> getFeatures() {
        return Objects.isNull(features) ? Collections.emptyList() : features;
    }

    @Nullable
    public String getReason() {
        return reason;
    }

    @Nullable
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
        return accepted == that.accepted && Objects.equals(features, that.features) && Objects.equals(reason, that.reason);
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
        return "PostFeatureUpgradeResponse{"
            + "accepted="
            + accepted
            + ", features="
            + features
            + ", reason='"
            + reason
            + '\''
            + ", elasticsearchException="
            + elasticsearchException
            + '}';
    }

    /**
     * A data class representing a feature that to be upgraded
     */
    public static class Feature implements Writeable, ToXContentObject {
        private final String featureName;

        /**
         * @param featureName Name of the feature
         */
        public Feature(String featureName) {
            this.featureName = featureName;
        }

        /**
         * @param in A stream input for a serialized feature object
         * @throws IOException if we can't deserialize the object
         */
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
            return "Feature{" + "featureName='" + featureName + '\'' + '}';
        }
    }
}
