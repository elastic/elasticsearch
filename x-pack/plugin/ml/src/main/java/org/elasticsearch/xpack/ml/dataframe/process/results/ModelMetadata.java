/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.dataframe.process.results;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.metadata.TotalFeatureImportance;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public class ModelMetadata implements ToXContentObject {

    public static final ParseField TOTAL_FEATURE_IMPORTANCE = new ParseField("total_feature_importance");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<ModelMetadata, Void> PARSER = new ConstructingObjectParser<>(
        "trained_model_metadata",
        a -> new ModelMetadata((List<TotalFeatureImportance>) a[0]));

    static {
        PARSER.declareObjectArray(constructorArg(), TotalFeatureImportance.STRICT_PARSER, TOTAL_FEATURE_IMPORTANCE);
    }

    private final List<TotalFeatureImportance> featureImportances;

    public ModelMetadata(List<TotalFeatureImportance> featureImportances) {
        this.featureImportances = featureImportances;
    }

    public List<TotalFeatureImportance> getFeatureImportances() {
        return featureImportances;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ModelMetadata that = (ModelMetadata) o;
        return Objects.equals(featureImportances, that.featureImportances);
    }

    @Override
    public int hashCode() {
        return Objects.hash(featureImportances);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TOTAL_FEATURE_IMPORTANCE.getPreferredName(), featureImportances);
        builder.endObject();
        return builder;
    }

}
