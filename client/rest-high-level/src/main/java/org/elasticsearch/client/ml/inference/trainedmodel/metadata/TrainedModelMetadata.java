/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.ml.inference.trainedmodel.metadata;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class TrainedModelMetadata implements ToXContentObject {

    public static final String NAME = "trained_model_metadata";
    public static final ParseField TOTAL_FEATURE_IMPORTANCE = new ParseField("total_feature_importance");
    public static final ParseField MODEL_ID = new ParseField("model_id");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<TrainedModelMetadata, Void> PARSER = new ConstructingObjectParser<>(NAME,
        true,
        a -> new TrainedModelMetadata((String)a[0], (List<TotalFeatureImportance>)a[1]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), MODEL_ID);
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), TotalFeatureImportance.PARSER, TOTAL_FEATURE_IMPORTANCE);
    }

    public static TrainedModelMetadata fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final List<TotalFeatureImportance> totalFeatureImportances;
    private final String modelId;

    public TrainedModelMetadata(String modelId, List<TotalFeatureImportance> totalFeatureImportances) {
        this.modelId = Objects.requireNonNull(modelId);
        this.totalFeatureImportances = Collections.unmodifiableList(totalFeatureImportances);
    }

    public String getModelId() {
        return modelId;
    }

    public List<TotalFeatureImportance> getTotalFeatureImportances() {
        return totalFeatureImportances;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TrainedModelMetadata that = (TrainedModelMetadata) o;
        return Objects.equals(totalFeatureImportances, that.totalFeatureImportances) &&
            Objects.equals(modelId, that.modelId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(totalFeatureImportances, modelId);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(MODEL_ID.getPreferredName(), modelId);
        builder.field(TOTAL_FEATURE_IMPORTANCE.getPreferredName(), totalFeatureImportances);
        builder.endObject();
        return builder;
    }
}
