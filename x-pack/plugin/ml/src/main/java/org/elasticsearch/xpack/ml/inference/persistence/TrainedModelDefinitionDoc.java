/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.inference.persistence;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.persistence.InferenceIndexConstants;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

/**
 * Used to store and retrieve the model definition from the stored index.
 *
 * As such, this does not support serialization between nodes as it is used to build a new TrainedModelDefinition object
 * That object is then used in the TrainedModelConfiguration and is serialized between nodes for inference.
 */
public class TrainedModelDefinitionDoc implements ToXContentObject {

    public static final String NAME = "trained_model_definition_doc";

    public static final ParseField DOC_NUM = new ParseField("doc_num");
    public static final ParseField DEFINITION = new ParseField("definition");
    public static final ParseField COMPRESSION_VERSION = new ParseField("compression_version");
    public static final ParseField TOTAL_DEFINITION_LENGTH = new ParseField("total_definition_length");
    public static final ParseField DEFINITION_LENGTH = new ParseField("definition_length");

    // These parsers follow the pattern that metadata is parsed leniently (to allow for enhancements), whilst config is parsed strictly
    public static final ObjectParser<TrainedModelDefinitionDoc.Builder, Void> LENIENT_PARSER = createParser(true);
    public static final ObjectParser<TrainedModelDefinitionDoc.Builder, Void> STRICT_PARSER = createParser(false);

    private static ObjectParser<TrainedModelDefinitionDoc.Builder, Void> createParser(boolean ignoreUnknownFields) {
        ObjectParser<TrainedModelDefinitionDoc.Builder, Void> parser = new ObjectParser<>(NAME,
            ignoreUnknownFields,
            TrainedModelDefinitionDoc.Builder::new);
        parser.declareString(TrainedModelDefinitionDoc.Builder::setModelId, TrainedModelConfig.MODEL_ID);
        parser.declareString(TrainedModelDefinitionDoc.Builder::setCompressedString, DEFINITION);
        parser.declareInt(TrainedModelDefinitionDoc.Builder::setDocNum, DOC_NUM);
        parser.declareInt(TrainedModelDefinitionDoc.Builder::setCompressionVersion, COMPRESSION_VERSION);
        parser.declareLong(TrainedModelDefinitionDoc.Builder::setDefinitionLength, DEFINITION_LENGTH);
        parser.declareLong(TrainedModelDefinitionDoc.Builder::setTotalDefinitionLength, TOTAL_DEFINITION_LENGTH);
        return parser;
    }

    public static TrainedModelDefinitionDoc.Builder fromXContent(XContentParser parser, boolean lenient) throws IOException {
        return lenient ? LENIENT_PARSER.parse(parser, null) : STRICT_PARSER.parse(parser, null);
    }

    // Opting for a `-` and not `#` for HTML encoding pains
    public static String docId(String modelId, int docNum) {
        return NAME + "-" + modelId + "-" + docNum;
    }

    private final String compressedString;
    private final String modelId;
    private final int docNum;
    private final long totalDefinitionLength;
    private final long definitionLength;
    private final int compressionVersion;

    private TrainedModelDefinitionDoc(String compressedString,
                                      String modelId,
                                      int docNum,
                                      long totalDefinitionLength,
                                      long definitionLength,
                                      int compressionVersion) {
        this.compressedString = ExceptionsHelper.requireNonNull(compressedString, DEFINITION);
        this.modelId = ExceptionsHelper.requireNonNull(modelId, TrainedModelConfig.MODEL_ID);
        if (docNum < 0) {
            throw new IllegalArgumentException("[doc_num] must be greater than or equal to 0");
        }
        this.docNum = docNum;
        if (totalDefinitionLength <= 0L) {
            throw new IllegalArgumentException("[total_definition_length] must be greater than 0");
        }
        this.totalDefinitionLength = totalDefinitionLength;
        if (definitionLength <= 0L) {
            throw new IllegalArgumentException("[definition_length] must be greater than 0");
        }
        this.definitionLength = definitionLength;
        this.compressionVersion = compressionVersion;
    }

    public String getCompressedString() {
        return compressedString;
    }

    public String getModelId() {
        return modelId;
    }

    public int getDocNum() {
        return docNum;
    }

    public long getTotalDefinitionLength() {
        return totalDefinitionLength;
    }

    public long getDefinitionLength() {
        return definitionLength;
    }

    public int getCompressionVersion() {
        return compressionVersion;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(InferenceIndexConstants.DOC_TYPE.getPreferredName(), NAME);
        builder.field(TrainedModelConfig.MODEL_ID.getPreferredName(), modelId);
        builder.field(DOC_NUM.getPreferredName(), docNum);
        builder.field(TOTAL_DEFINITION_LENGTH.getPreferredName(), totalDefinitionLength);
        builder.field(DEFINITION_LENGTH.getPreferredName(), definitionLength);
        builder.field(COMPRESSION_VERSION.getPreferredName(), compressionVersion);
        builder.field(DEFINITION.getPreferredName(), compressedString);
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TrainedModelDefinitionDoc that = (TrainedModelDefinitionDoc) o;
        return Objects.equals(modelId, that.modelId) &&
            Objects.equals(docNum, that.docNum) &&
            Objects.equals(definitionLength, that.definitionLength) &&
            Objects.equals(totalDefinitionLength, that.totalDefinitionLength) &&
            Objects.equals(compressionVersion, that.compressionVersion) &&
            Objects.equals(compressedString, that.compressedString);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelId, docNum, totalDefinitionLength, definitionLength, compressionVersion, compressedString);
    }

    public static class Builder {

        private String modelId;
        private String compressedString;
        private int docNum;
        private long totalDefinitionLength;
        private long definitionLength;
        private int compressionVersion;

        public Builder setModelId(String modelId) {
            this.modelId = modelId;
            return this;
        }

        public Builder setCompressedString(String compressedString) {
            this.compressedString = compressedString;
            return this;
        }

        public Builder setDocNum(int docNum) {
            this.docNum = docNum;
            return this;
        }

        public Builder setTotalDefinitionLength(long totalDefinitionLength) {
            this.totalDefinitionLength = totalDefinitionLength;
            return this;
        }

        public Builder setDefinitionLength(long definitionLength) {
            this.definitionLength = definitionLength;
            return this;
        }

        public Builder setCompressionVersion(int compressionVersion) {
            this.compressionVersion = compressionVersion;
            return this;
        }

        public TrainedModelDefinitionDoc build() {
            return new TrainedModelDefinitionDoc(
                this.compressedString,
                this.modelId,
                this.docNum,
                this.totalDefinitionLength,
                this.definitionLength,
                this.compressionVersion);
        }
    }

}
