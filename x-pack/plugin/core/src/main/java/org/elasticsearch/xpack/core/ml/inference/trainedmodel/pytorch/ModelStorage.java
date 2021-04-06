/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel.pytorch;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.common.time.TimeUtils;
import org.elasticsearch.xpack.core.ml.inference.persistence.InferenceIndexConstants;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Meta information describing the size of a PyTorch model
 * and where to find the model chunks.
 *
 * Binary data is stored base64 encoded. {@link #getModelSize()}
 * is the total size of the raw binary chunks before they have
 * been base64 encoded
 */
public class ModelStorage implements ToXContentObject, Writeable {

    public static final String TYPE = "model_storage";

    public static final ParseField MODEL_ID = new ParseField("model_id");
    public static final ParseField DOC_PREFIX = new ParseField("doc_prefix");
    public static final ParseField CREATE_TIME = new ParseField("create_time");
    public static final ParseField INDEX = new ParseField("index");
    public static final ParseField FIELD_NAME = new ParseField("field_name");
    public static final ParseField DESCRIPTION = new ParseField("description");
    public static final ParseField MODEL_DOC_COUNT = new ParseField("model_doc_count");
    public static final ParseField MODEL_SIZE_BYTES = new ParseField("model_size_bytes");

    public static final ConstructingObjectParser<ModelStorage, Void> STRICT_PARSER = createParser(false);
    public static final ConstructingObjectParser<ModelStorage, Void> LENIENT_PARSER = createParser(true);

    private static ConstructingObjectParser<ModelStorage, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<ModelStorage, Void> parser =
            new ConstructingObjectParser<>(TYPE, ignoreUnknownFields,
                a -> new ModelStorage((String) a[0], (String) a[1], (Instant) a[2], (String) a[3],
                    (String) a[4], (int) a[5], (long) a[6]));

        parser.declareString(constructorArg(), MODEL_ID);
        parser.declareString(constructorArg(), DOC_PREFIX);
        parser.declareField(constructorArg(),
            (p, c) -> TimeUtils.parseTimeFieldToInstant(p, CREATE_TIME.getPreferredName()),
            CREATE_TIME,
            ObjectParser.ValueType.VALUE);
        parser.declareString((m, s) -> {}, INDEX); // index is hard coded
        parser.declareString(constructorArg(), FIELD_NAME);
        parser.declareString(optionalConstructorArg(), DESCRIPTION);
        parser.declareInt(constructorArg(), MODEL_DOC_COUNT);
        parser.declareLong(constructorArg(), MODEL_SIZE_BYTES);
        return parser;
    }

    public static ModelStorage fromXContent(XContentParser parser, boolean lenient) {
        return lenient ? LENIENT_PARSER.apply(parser, null) : STRICT_PARSER.apply(parser, null);
    }

    public static String docIdFromModelId(String modelId) {
        return "model_storage_" + modelId;
    }

    private final String modelId;
    private final String documentPrefix;
    private final Instant createTime;
    private final String fieldName;
    private final String description;
    private final int modelDocCount;
    private final ByteSizeValue modelSizeInBytes;

    public ModelStorage(String modelId, String prefix, Instant createTime, String fieldName,
                        String description, int modelDocCount, long modelSizeInBytes) {
        this.modelId = Objects.requireNonNull(modelId);
        this.documentPrefix = Objects.requireNonNull(prefix);
        this.createTime = Objects.requireNonNull(createTime);
        this.fieldName = Objects.requireNonNull(fieldName);
        this.description = description;
        this.modelDocCount = modelDocCount;
        this.modelSizeInBytes = ByteSizeValue.ofBytes(modelSizeInBytes);
    }

    public ModelStorage(StreamInput in) throws IOException {
        modelId = in.readString();
        documentPrefix = in.readString();
        createTime = in.readInstant();
        fieldName = in.readString();
        description = in.readOptionalString();
        modelDocCount = in.readInt();
        modelSizeInBytes = new ByteSizeValue(in);
    }

    public List<String> documentIds() {
        List<String> ids = new ArrayList<>(modelDocCount);
        for (int i = 0; i<modelDocCount; i++) {
            ids.add(documentPrefix + i);
        }

        return ids;
    }

    public String getModelId() {
        return modelId;
    }

    public String getDocumentPrefix() {
        return documentPrefix;
    }

    /**
     * @return The index the model is stored in
     */
    public String getIndex() {
        return InferenceIndexConstants.LATEST_INDEX_NAME;
    }

    /**
     * The field in the document containing the model definition
     * @return The name
     */
    public String getFieldName() {
        return fieldName;
    }

    public Instant getCreateTime() {
        return createTime;
    }

    public String getDescription() {
        return description;
    }

    public int getModelDocCount() {
        return modelDocCount;
    }

    public ByteSizeValue getModelSize() {
        return modelSizeInBytes;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(modelId);
        out.writeString(documentPrefix);
        out.writeInstant(createTime);
        out.writeString(fieldName);
        out.writeOptionalString(description);
        out.writeInt(modelDocCount);
        modelSizeInBytes.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(MODEL_ID.getPreferredName(), modelId);
        builder.field(DOC_PREFIX.getPreferredName(), documentPrefix);
        builder.field(CREATE_TIME.getPreferredName(), createTime);
        builder.field(INDEX.getPreferredName(), getIndex());
        builder.field(FIELD_NAME.getPreferredName(), fieldName);
        if (description != null) {
            builder.field(DESCRIPTION.getPreferredName(), description);
        }
        builder.field(MODEL_DOC_COUNT.getPreferredName(), modelDocCount);
        builder.field(MODEL_SIZE_BYTES.getPreferredName(), modelSizeInBytes.getBytes());
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ModelStorage that = (ModelStorage) o;
        return Objects.equals(modelId, that.modelId) &&
            Objects.equals(documentPrefix, that.documentPrefix) &&
            Objects.equals(createTime, that.createTime) &&
            Objects.equals(fieldName, that.fieldName) &&
            Objects.equals(getIndex(), that.getIndex()) &&
            Objects.equals(description, that.description) &&
            Objects.equals(modelSizeInBytes, modelSizeInBytes) &&
            modelDocCount == that.modelDocCount;
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelId, documentPrefix, createTime, fieldName, getIndex(),
            description, modelDocCount, modelSizeInBytes);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
