/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel.metadata;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.persistence.InferenceIndexConstants;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class TrainedModelMetadata implements ToXContentObject, Writeable {

    public static final String NAME = "trained_model_metadata";
    public static final ParseField TOTAL_FEATURE_IMPORTANCE = new ParseField("total_feature_importance");
    public static final ParseField MODEL_ID = new ParseField("model_id");

    // These parsers follow the pattern that metadata is parsed leniently (to allow for enhancements), whilst config is parsed strictly
    public static final ConstructingObjectParser<TrainedModelMetadata, Void> LENIENT_PARSER = createParser(true);
    public static final ConstructingObjectParser<TrainedModelMetadata, Void> STRICT_PARSER = createParser(false);

    @SuppressWarnings("unchecked")
    private static ConstructingObjectParser<TrainedModelMetadata, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<TrainedModelMetadata, Void> parser = new ConstructingObjectParser<>(NAME,
            ignoreUnknownFields,
            a -> new TrainedModelMetadata((String)a[0], (List<TotalFeatureImportance>)a[1]));
        parser.declareString(ConstructingObjectParser.constructorArg(), MODEL_ID);
        parser.declareObjectArray(ConstructingObjectParser.constructorArg(),
            ignoreUnknownFields ? TotalFeatureImportance.LENIENT_PARSER : TotalFeatureImportance.STRICT_PARSER,
            TOTAL_FEATURE_IMPORTANCE);
        return parser;
    }

    public static TrainedModelMetadata fromXContent(XContentParser parser, boolean lenient) throws IOException {
        return lenient ? LENIENT_PARSER.parse(parser, null) : STRICT_PARSER.parse(parser, null);
    }

    public static String docId(String modelId) {
        return NAME + "-" + modelId;
    }

    public static String modelId(String docId) {
        return docId.substring(NAME.length() + 1);
    }

    private final List<TotalFeatureImportance> totalFeatureImportances;
    private final String modelId;

    public TrainedModelMetadata(StreamInput in) throws IOException {
        this.modelId = in.readString();
        this.totalFeatureImportances = in.readList(TotalFeatureImportance::new);
    }

    public TrainedModelMetadata(String modelId, List<TotalFeatureImportance> totalFeatureImportances) {
        this.modelId = ExceptionsHelper.requireNonNull(modelId, MODEL_ID);
        this.totalFeatureImportances = Collections.unmodifiableList(totalFeatureImportances);
    }

    public String getModelId() {
        return modelId;
    }

    public String getDocId() {
        return docId(modelId);
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
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(modelId);
        out.writeList(totalFeatureImportances);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (params.paramAsBoolean(ToXContentParams.FOR_INTERNAL_STORAGE, false)) {
            builder.field(InferenceIndexConstants.DOC_TYPE.getPreferredName(), NAME);
        }
        builder.field(MODEL_ID.getPreferredName(), modelId);
        builder.field(TOTAL_FEATURE_IMPORTANCE.getPreferredName(), totalFeatureImportances);
        builder.endObject();
        return builder;
    }
}
