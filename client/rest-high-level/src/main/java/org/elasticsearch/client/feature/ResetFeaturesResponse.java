/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.feature;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.util.List;

public class ResetFeaturesResponse {
    private final List<ResetFeatureStateStatus> features;

    private static final ParseField FEATURES = new ParseField("features");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<ResetFeaturesResponse, Void> PARSER = new ConstructingObjectParser<>(
        "snapshottable_features_response", true,
        (a, ctx) -> new ResetFeaturesResponse((List<ResetFeatureStateStatus>) a[0])
    );

    static {
        PARSER.declareObjectArray(
            ConstructingObjectParser.constructorArg(),
            ResetFeaturesResponse.ResetFeatureStateStatus::parse, FEATURES);
    }

    public ResetFeaturesResponse(List<ResetFeatureStateStatus> features) {
        this.features = features;
    }

    public List<ResetFeatureStateStatus> getFeatures() {
        return features;
    }

    public static ResetFeaturesResponse parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public static class ResetFeatureStateStatus {
        private final String featureName;
        private final String status;

        private static final ParseField FEATURE_NAME = new ParseField("feature_name");
        private static final ParseField STATUS = new ParseField("status");

        private static final ConstructingObjectParser<ResetFeatureStateStatus, Void> PARSER =  new ConstructingObjectParser<>(
            "features", true, (a, ctx) -> new ResetFeatureStateStatus((String) a[0], (String) a[1])
        );

        static {
            PARSER.declareField(ConstructingObjectParser.constructorArg(),
                (p, c) -> p.text(), FEATURE_NAME, ObjectParser.ValueType.STRING);
            PARSER.declareField(ConstructingObjectParser.constructorArg(),
                (p, c) -> p.text(), STATUS, ObjectParser.ValueType.STRING);
        }

        ResetFeatureStateStatus(String featureName, String status) {
            this.featureName = featureName;
            this.status = status;
        }

        public static ResetFeatureStateStatus parse(XContentParser parser, Void ctx) {
            return PARSER.apply(parser, ctx);
        }

        public String getFeatureName() {
            return featureName;
        }

        public String getStatus() {
            return status;
        }
    }
}
