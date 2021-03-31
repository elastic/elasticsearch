/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ml.inference.trainedmodel.pytorch;

import org.elasticsearch.client.common.TimeUtil;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.time.Instant;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class ModelStorage implements ToXContentObject {

    public static final String TYPE = "model_storage";

    public static final ParseField MODEL_ID = new ParseField("model_id");
    public static final ParseField DOC_PREFIX = new ParseField("doc_prefix");
    public static final ParseField CREATE_TIME = new ParseField("create_time");
    public static final ParseField DESCRIPTION = new ParseField("description");
    public static final ParseField MODEL_DOC_COUNT = new ParseField("model_doc_count");
    public static final ParseField MODEL_SIZE_BYTES = new ParseField("model_size_bytes");

    public static final ConstructingObjectParser<ModelStorage, Void> PARSER =
            new ConstructingObjectParser<>(TYPE, true,
                a -> new ModelStorage((String) a[0], (String) a[1], (Instant) a[2], (String) a[3],
                    (int) a[4], (long) a[5]));

    static {
        PARSER.declareString(constructorArg(), MODEL_ID);
        PARSER.declareString(constructorArg(), DOC_PREFIX);
        PARSER.declareField(constructorArg(),
            (p, c) -> TimeUtil.parseTimeFieldToInstant(p, CREATE_TIME.getPreferredName()),
            CREATE_TIME,
            ObjectParser.ValueType.VALUE);
        PARSER.declareString(optionalConstructorArg(), DESCRIPTION);
        PARSER.declareInt(constructorArg(), MODEL_DOC_COUNT);
        PARSER.declareLong(constructorArg(), MODEL_SIZE_BYTES);
    }

    public static ModelStorage fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final String modelId;
    private final String documentPrefix;
    private final Instant createTime;
    private final String description;
    private final int modelDocCount;
    private final long modelSizeInBytes;

    public ModelStorage(String modelId, String prefix, Instant createTime,
                        String description, int modelDocCount, long modelSizeInBytes) {
        this.modelId = Objects.requireNonNull(modelId);
        this.documentPrefix = Objects.requireNonNull(prefix);
        this.createTime = Objects.requireNonNull(createTime);
        this.description = description;
        this.modelDocCount = modelDocCount;
        this.modelSizeInBytes = modelSizeInBytes;
    }

    public String getModelId() {
        return modelId;
    }

    public String getDocumentPrefix() {
        return documentPrefix;
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

    public long getModelSizeInBytes() {
        return modelSizeInBytes;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(MODEL_ID.getPreferredName(), modelId);
        builder.field(DOC_PREFIX.getPreferredName(), documentPrefix);
        builder.field(CREATE_TIME.getPreferredName(), createTime);
        if (description != null) {
            builder.field(DESCRIPTION.getPreferredName(), description);
        }
        builder.field(MODEL_DOC_COUNT.getPreferredName(), modelDocCount);
        builder.field(MODEL_SIZE_BYTES.getPreferredName(), modelSizeInBytes);
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
            Objects.equals(description, that.description) &&
            modelDocCount == that.modelDocCount &&
            modelSizeInBytes == that.modelSizeInBytes;
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelId, documentPrefix, createTime, description,
            modelDocCount, modelSizeInBytes);
    }
}
