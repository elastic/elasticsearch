/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.persistence.InferenceIndexConstants;

import java.io.IOException;
import java.util.Objects;

public class IndexLocation implements StrictlyParsedTrainedModelLocation, LenientlyParsedTrainedModelLocation {

    public static final ParseField INDEX = new ParseField("index");
    private static final ParseField NAME = new ParseField("name");

    private static final ConstructingObjectParser<IndexLocation, Void> LENIENT_PARSER = createParser(true);
    private static final ConstructingObjectParser<IndexLocation, Void> STRICT_PARSER = createParser(false);

    private static ConstructingObjectParser<IndexLocation, Void> createParser(boolean lenient) {
        ConstructingObjectParser<IndexLocation, Void> parser = new ConstructingObjectParser<>(
            NAME.getPreferredName(),
            lenient,
            a -> new IndexLocation((String) a[0], (String) a[1])
        );
        parser.declareString(ConstructingObjectParser.optionalConstructorArg(), NAME);
        parser.declareString(ConstructingObjectParser.optionalConstructorArg(), TrainedModelConfig.MODEL_ID);
        return parser;
    }

    public static IndexLocation fromXContentStrict(XContentParser parser) throws IOException {
        return STRICT_PARSER.parse(parser, null);
    }

    public static IndexLocation fromXContentLenient(XContentParser parser) throws IOException {
        return LENIENT_PARSER.parse(parser, null);
    }

    public static IndexLocation forDefaultIndex(String modelId) {
        return new IndexLocation(InferenceIndexConstants.nativeDefinitionStore(), modelId);
    }

    private final String indexName;
    private final String modelId;

    public IndexLocation(String indexName) {
        this.indexName = Objects.requireNonNull(indexName);
        this.modelId = null;
    }

    public IndexLocation(String indexName, String modelId) {
        this.indexName = indexName;
        this.modelId = modelId;
    }

    public IndexLocation(StreamInput in) throws IOException {
        this.indexName = in.readString();
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_7_0)) {
            this.modelId = in.readOptionalString();
        } else {
            this.modelId = null;
        }
    }

    public String getIndexName() {
        return indexName;
    }

    public String getModelId() {
        return modelId;
    }

    @Override
    public String getLocationName() {
        return getIndexName();
    }

    public boolean isIndexAndModelIdDefined() {
        return modelId != null && indexName != null;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(NAME.getPreferredName(), indexName);
        if (modelId != null) {
            builder.field(TrainedModelConfig.MODEL_ID.getPreferredName(), modelId);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(indexName);
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_7_0)) {
            out.writeOptionalString(modelId);
        }
    }

    @Override
    public String getWriteableName() {
        return INDEX.getPreferredName();
    }

    @Override
    public String getName() {
        return INDEX.getPreferredName();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IndexLocation that = (IndexLocation) o;
        return Objects.equals(indexName, that.indexName) && Objects.equals(modelId, that.modelId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexName, modelId);
    }
}
