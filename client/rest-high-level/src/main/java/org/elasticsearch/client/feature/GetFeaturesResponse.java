/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.feature;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.util.List;
import java.util.Objects;

public class GetFeaturesResponse {

    private final List<SnapshottableFeature> features;

    private static final ParseField FEATURES = new ParseField("features");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<GetFeaturesResponse, Void> PARSER = new ConstructingObjectParser<>(
        "snapshottable_features_response", true, (a, ctx) -> new GetFeaturesResponse((List<SnapshottableFeature>) a[0])
    );

    static {
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), SnapshottableFeature::parse, FEATURES);
    }

    public GetFeaturesResponse(List<SnapshottableFeature> features) {
        this.features = features;
    }

    public List<SnapshottableFeature> getFeatures() {
        return features;
    }

    public static GetFeaturesResponse parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if ((o instanceof GetFeaturesResponse) == false) return false;
        GetFeaturesResponse that = (GetFeaturesResponse) o;
        return getFeatures().equals(that.getFeatures());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getFeatures());
    }

    public static class SnapshottableFeature {

        private final String featureName;
        private final String description;

        private static final ParseField FEATURE_NAME = new ParseField("name");
        private static final ParseField DESCRIPTION = new ParseField("description");

        private static final ConstructingObjectParser<SnapshottableFeature, Void> PARSER =  new ConstructingObjectParser<>(
            "feature", true, (a, ctx) -> new SnapshottableFeature((String) a[0], (String) a[1])
        );

        static {
            PARSER.declareField(ConstructingObjectParser.constructorArg(),
                (p, c) -> p.text(), FEATURE_NAME, ObjectParser.ValueType.STRING);
            PARSER.declareField(ConstructingObjectParser.constructorArg(),
                (p, c) -> p.text(), DESCRIPTION, ObjectParser.ValueType.STRING);
        }

        public SnapshottableFeature(String featureName, String description) {
            this.featureName = featureName;
            this.description = description;
        }

        public static SnapshottableFeature parse(XContentParser parser, Void ctx) {
            return PARSER.apply(parser, ctx);
        }

        public String getFeatureName() {
            return featureName;
        }

        public String getDescription() {
            return description;
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
