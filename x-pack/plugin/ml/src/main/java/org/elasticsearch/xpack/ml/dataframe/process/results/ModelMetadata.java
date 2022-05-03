/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.dataframe.process.results;

import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.metadata.FeatureImportanceBaseline;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.metadata.Hyperparameters;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.metadata.TotalFeatureImportance;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class ModelMetadata implements ToXContentObject {

    public static final ParseField TOTAL_FEATURE_IMPORTANCE = new ParseField("total_feature_importance");
    public static final ParseField FEATURE_IMPORTANCE_BASELINE = new ParseField("feature_importance_baseline");
    public static final ParseField HYPERPARAMETERS = new ParseField("hyperparameters");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<ModelMetadata, Void> PARSER = new ConstructingObjectParser<>(
        "trained_model_metadata",
        a -> new ModelMetadata((List<TotalFeatureImportance>) a[0], (FeatureImportanceBaseline) a[1], (List<Hyperparameters>) a[2])
    );

    static {
        PARSER.declareObjectArray(constructorArg(), TotalFeatureImportance.STRICT_PARSER, TOTAL_FEATURE_IMPORTANCE);
        PARSER.declareObject(optionalConstructorArg(), FeatureImportanceBaseline.STRICT_PARSER, FEATURE_IMPORTANCE_BASELINE);
        PARSER.declareObjectArray(optionalConstructorArg(), Hyperparameters.STRICT_PARSER, HYPERPARAMETERS);
    }

    private final List<TotalFeatureImportance> featureImportances;
    private final FeatureImportanceBaseline featureImportanceBaseline;
    private final List<Hyperparameters> hyperparameters;

    public ModelMetadata(
        List<TotalFeatureImportance> featureImportances,
        FeatureImportanceBaseline featureImportanceBaseline,
        List<Hyperparameters> hyperparameters
    ) {
        this.featureImportances = featureImportances;
        this.featureImportanceBaseline = featureImportanceBaseline;
        this.hyperparameters = hyperparameters;
    }

    public List<TotalFeatureImportance> getFeatureImportances() {
        return featureImportances;
    }

    public FeatureImportanceBaseline getFeatureImportanceBaseline() {
        return featureImportanceBaseline;
    }

    public List<Hyperparameters> getHyperparameters() {
        return hyperparameters;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ModelMetadata that = (ModelMetadata) o;
        return Objects.equals(featureImportances, that.featureImportances)
            && Objects.equals(featureImportanceBaseline, that.featureImportanceBaseline)
            && Objects.equals(hyperparameters, that.hyperparameters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(featureImportances, featureImportanceBaseline, hyperparameters);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TOTAL_FEATURE_IMPORTANCE.getPreferredName(), featureImportances);
        if (featureImportanceBaseline != null) {
            builder.field(FEATURE_IMPORTANCE_BASELINE.getPreferredName(), featureImportanceBaseline);
        }
        if (hyperparameters != null) {
            builder.field(HYPERPARAMETERS.getPreferredName(), hyperparameters);
        }
        builder.endObject();
        return builder;
    }

}
