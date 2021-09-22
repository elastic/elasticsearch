/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.migration;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.core.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class PostFeatureUpgradeResponse {

    private final boolean accepted;
    private final List<Feature> features;

    @Nullable private final String reason;
    @Nullable private final ElasticsearchException elasticsearchException;

    private static final ParseField ACCEPTED = new ParseField("accepted");
    private static final ParseField FEATURES = new ParseField("features");
    private static final ParseField REASON = new ParseField("reason");
    private static final ParseField ELASTICSEARCH_EXCEPTION = new ParseField("exception");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<PostFeatureUpgradeResponse, Void> PARSER = new ConstructingObjectParser<>(
        "post_feature_upgrade_response", true, (a, ctx) -> new PostFeatureUpgradeResponse(
        (Boolean) a[0],
        (List<Feature>) a[1],
        (String) a[2],
        (ElasticsearchException) a[3]
    ));

    static {
        PARSER.declareField(ConstructingObjectParser.constructorArg(),
            (p, c) -> p.booleanValue(), ACCEPTED, ObjectParser.ValueType.BOOLEAN);
        PARSER.declareObjectArray(ConstructingObjectParser.optionalConstructorArg(),
            Feature::parse, FEATURES);
        PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> p.text(), REASON, ObjectParser.ValueType.STRING);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> ElasticsearchException.fromXContent(p), ELASTICSEARCH_EXCEPTION);
    }

    public static PostFeatureUpgradeResponse parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public PostFeatureUpgradeResponse(
        boolean accepted,
        List<Feature> features,
        @Nullable String reason,
        @Nullable ElasticsearchException elasticsearchException
    ) {
        this.accepted = accepted;
        this.features = Objects.nonNull(features) ? features : Collections.emptyList();
        this.reason = reason;
        this.elasticsearchException = elasticsearchException;
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

    public static class Feature {
        private final String featureName;

        private static final ParseField FEATURE_NAME = new ParseField("feature_name");

        private static final ConstructingObjectParser<Feature, Void> PARSER = new ConstructingObjectParser<>(
            "feature", true, (a, ctx) -> new Feature((String) a[0])
        );

        static {
            PARSER.declareField(ConstructingObjectParser.constructorArg(),
                (p, c) -> p.text(), FEATURE_NAME, ObjectParser.ValueType.STRING);
        }

        public static Feature parse(XContentParser parser, Void ctx) {
            return PARSER.apply(parser, ctx);
        }

        public Feature(String featureName) {
            this.featureName = featureName;
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
    }
}
