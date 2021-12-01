/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class RegressionFeatureImportance extends AbstractFeatureImportance {

    private final double importance;
    private final String featureName;
    static final String IMPORTANCE = "importance";
    static final String FEATURE_NAME = "feature_name";

    private static final ConstructingObjectParser<RegressionFeatureImportance, Void> PARSER = new ConstructingObjectParser<>(
        "regression_feature_importance",
        a -> new RegressionFeatureImportance((String) a[0], (Double) a[1])
    );

    static {
        PARSER.declareString(constructorArg(), new ParseField(RegressionFeatureImportance.FEATURE_NAME));
        PARSER.declareDouble(constructorArg(), new ParseField(RegressionFeatureImportance.IMPORTANCE));
    }

    public static RegressionFeatureImportance fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public RegressionFeatureImportance(String featureName, double importance) {
        this.featureName = Objects.requireNonNull(featureName);
        this.importance = importance;
    }

    public RegressionFeatureImportance(StreamInput in) throws IOException {
        this.featureName = in.readString();
        this.importance = in.readDouble();
    }

    public double getImportance() {
        return importance;
    }

    @Override
    public String getFeatureName() {
        return featureName;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(featureName);
        out.writeDouble(importance);
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put(FEATURE_NAME, featureName);
        map.put(IMPORTANCE, importance);
        return map;
    }

    @Override
    public boolean equals(Object object) {
        if (object == this) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        RegressionFeatureImportance that = (RegressionFeatureImportance) object;
        return Objects.equals(featureName, that.featureName) && Objects.equals(importance, that.importance);
    }

    @Override
    public int hashCode() {
        return Objects.hash(featureName, importance);
    }
}
