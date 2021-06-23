/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.feature;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.util.List;
import java.util.Objects;

/**
 * This class represents the response of the Feature State Reset API. It is a
 * list containing the response of every feature whose state can be reset. The
 * response from each feature will indicate success or failure. In the case of a
 * failure, the cause will be returned as well.
 */
public class ResetFeaturesResponse {
    private final List<ResetFeatureStateStatus> features;

    private static final ParseField FEATURES = new ParseField("features");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<ResetFeaturesResponse, Void> PARSER = new ConstructingObjectParser<>(
        "features_reset_status_response", true,
        (a, ctx) -> new ResetFeaturesResponse((List<ResetFeatureStateStatus>) a[0])
    );

    static {
        PARSER.declareObjectArray(
            ConstructingObjectParser.constructorArg(),
            ResetFeaturesResponse.ResetFeatureStateStatus::parse, FEATURES);
    }

    /**
     * Create a new ResetFeaturesResponse
     * @param features A full list of status responses from individual feature reset operations.
     */
    public ResetFeaturesResponse(List<ResetFeatureStateStatus> features) {
        this.features = features;
    }

    /**
     * @return List containing a reset status for each feature that we have tried to reset.
     */
    public List<ResetFeatureStateStatus> getFeatureResetStatuses() {
        return features;
    }

    public static ResetFeaturesResponse parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    /**
     * A class representing the status of an attempt to reset a feature's state.
     * The attempt to reset either succeeds and we return the name of the
     * feature and a success flag; or it fails and we return the name of the feature,
     * a status flag, and the exception thrown during the attempt to reset the feature.
     */
    public static class ResetFeatureStateStatus {
        private final String featureName;
        private final String status;
        private final Exception exception;

        private static final ParseField FEATURE_NAME = new ParseField("feature_name");
        private static final ParseField STATUS = new ParseField("status");
        private static final ParseField EXCEPTION = new ParseField("exception");

        private static final ConstructingObjectParser<ResetFeatureStateStatus, Void> PARSER = new ConstructingObjectParser<>(
            "feature_state_reset_stats", true,
            (a, ctx) -> new ResetFeatureStateStatus((String) a[0], (String) a[1], (ElasticsearchException) a[2])
        );

        static {
            PARSER.declareField(ConstructingObjectParser.constructorArg(),
                (p, c) -> p.text(), FEATURE_NAME, ObjectParser.ValueType.STRING);
            PARSER.declareField(ConstructingObjectParser.constructorArg(),
                (p, c) -> p.text(), STATUS, ObjectParser.ValueType.STRING);
            PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(),
                (p, c) -> ElasticsearchException.fromXContent(p), EXCEPTION);
        }

        /**
         * Create a ResetFeatureStateStatus.
         * @param featureName Name of the feature whose status has been reset.
         * @param status Whether the reset attempt succeeded or failed.
         * @param exception If the reset attempt failed, the exception that caused the
         *                  failure. Must be null when status is "SUCCESS".
         */
        ResetFeatureStateStatus(String featureName, String status, @Nullable Exception exception) {
            this.featureName = featureName;
            assert "SUCCESS".equals(status) || "FAILURE".equals(status);
            this.status = status;
            assert "FAILURE".equals(status) ? Objects.nonNull(exception) : Objects.isNull(exception);
            this.exception = exception;
        }

        public static ResetFeatureStateStatus parse(XContentParser parser, Void ctx) {
            return PARSER.apply(parser, ctx);
        }

        /**
         * @return Name of the feature that we tried to reset
         */
        public String getFeatureName() {
            return featureName;
        }

        /**
         * @return "SUCCESS" if the reset attempt succeeded, "FAILURE" otherwise.
         */
        public String getStatus() {
            return status;
        }

        /**
         * @return The exception that caused the reset attempt to fail.
         */
        @Nullable
        public Exception getException() {
            return exception;
        }
    }
}
