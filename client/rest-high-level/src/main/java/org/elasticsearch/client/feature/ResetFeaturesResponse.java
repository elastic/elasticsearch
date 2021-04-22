/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.feature;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
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

    public static ResetFeaturesResponse parse(XContentParser parser) throws IOException {
        String currentFieldName = null;
        List<ResetFeatureStateStatus> statuses = new ArrayList<>();
        for (XContentParser.Token token = parser.nextToken(); token != XContentParser.Token.END_OBJECT; token = parser.nextToken()) {
            switch (token) {
                case FIELD_NAME:
                    currentFieldName = parser.currentName();
                    break;
                case START_ARRAY:
                    if (FEATURES.getPreferredName().equals(currentFieldName)) {
                        for (token = parser.nextToken(); token != XContentParser.Token.END_ARRAY; token = parser.nextToken()) {
                            if (token == XContentParser.Token.START_OBJECT) {
                                statuses.add(ResetFeatureStateStatus.parse(parser, null));
                            }
                        }
                    }
                    break;
                default:
                    // If unknown tokens are encounter then these should be ignored, because
                    // this is parsing logic on the client side.
                    break;
            }
        }
        return new ResetFeaturesResponse(statuses);
    }

    public static class ResetFeatureStateStatus {
        private final String featureName;
        private final String status;
        private final Exception exception;

        private static final ParseField FEATURE_NAME = new ParseField("feature_name");
        private static final ParseField STATUS = new ParseField("status");
        private static final ParseField ERROR = new ParseField("error");

        private static final ConstructingObjectParser<ResetFeatureStateStatus, Void> PARSER =  new ConstructingObjectParser<>(
            "features", true, (a, ctx) -> new ResetFeatureStateStatus((String) a[0], (String) a[1], (Exception) a[2])
        );

        static {
            PARSER.declareField(ConstructingObjectParser.constructorArg(),
                (p, c) -> p.text(), FEATURE_NAME, ObjectParser.ValueType.STRING);
            PARSER.declareField(ConstructingObjectParser.constructorArg(),
                (p, c) -> p.text(), STATUS, ObjectParser.ValueType.STRING);
            PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(),
                (p, c) -> p.text(), ERROR, ObjectParser.ValueType.OBJECT_OR_NULL);
        }

        ResetFeatureStateStatus(String featureName, String status, Exception exception) {
            this.featureName = featureName;
            this.status = status;
            this.exception = exception;
        }

        public static ResetFeatureStateStatus parse(XContentParser parser, Void ctx) throws IOException {
            String currentFieldName = null;
            String featureName = null;
            String status = null;
            Exception exception = null;
            for (XContentParser.Token token = parser.nextToken(); token != XContentParser.Token.END_OBJECT; token = parser.nextToken()) {
                switch (token) {
                    case FIELD_NAME:
                        currentFieldName = parser.currentName();
                        break;
                    case VALUE_STRING:
                        if (FEATURE_NAME.match(currentFieldName, parser.getDeprecationHandler())) {
                            featureName = parser.text();
                        } else if (STATUS.match(currentFieldName, parser.getDeprecationHandler())) {
                            status = parser.text();
                        }
                        break;
                    case START_OBJECT:
                        if (ERROR.match(currentFieldName, parser.getDeprecationHandler())) {
                            exception = ElasticsearchException.fromXContent(parser);
                        }
                        break;
                    default:
                        // If unknown tokens are encounter then these should be ignored, because
                        // this is parsing logic on the client side.
                        break;
                }
            }
            return new ResetFeatureStateStatus(featureName, status, exception);
        }

        public String getFeatureName() {
            return featureName;
        }

        public String getStatus() {
            return status;
        }

        public Exception getException() {
            return exception;
        }
    }
}
