/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser.ValueType;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class TransformIndexerPosition implements Writeable, ToXContentObject {
    public static final String NAME = "data_frame/indexer_position";

    public static final ParseField INDEXER_POSITION = new ParseField("indexer_position");
    public static final ParseField BUCKET_POSITION = new ParseField("bucket_position");

    private final Map<String, Object> indexerPosition;
    private final Map<String, Object> bucketPosition;

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<TransformIndexerPosition, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        true,
        args -> new TransformIndexerPosition((Map<String, Object>) args[0], (Map<String, Object>) args[1])
    );

    static {
        PARSER.declareField(optionalConstructorArg(), XContentParser::mapOrdered, INDEXER_POSITION, ValueType.OBJECT);
        PARSER.declareField(optionalConstructorArg(), XContentParser::mapOrdered, BUCKET_POSITION, ValueType.OBJECT);
    }

    public TransformIndexerPosition(Map<String, Object> indexerPosition, Map<String, Object> bucketPosition) {
        this.indexerPosition = indexerPosition == null ? null : Collections.unmodifiableMap(indexerPosition);
        this.bucketPosition = bucketPosition == null ? null : Collections.unmodifiableMap(bucketPosition);
    }

    public TransformIndexerPosition(StreamInput in) throws IOException {
        Map<String, Object> position = in.readGenericMap();
        indexerPosition = position == null ? null : Collections.unmodifiableMap(position);
        position = in.readGenericMap();
        bucketPosition = position == null ? null : Collections.unmodifiableMap(position);
    }

    public Map<String, Object> getIndexerPosition() {
        return indexerPosition;
    }

    public Map<String, Object> getBucketsPosition() {
        return bucketPosition;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeGenericMap(indexerPosition);
        out.writeGenericMap(bucketPosition);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (indexerPosition != null) {
            builder.field(INDEXER_POSITION.getPreferredName(), indexerPosition);
        }
        if (bucketPosition != null) {
            builder.field(BUCKET_POSITION.getPreferredName(), bucketPosition);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        TransformIndexerPosition that = (TransformIndexerPosition) other;

        return Objects.equals(this.indexerPosition, that.indexerPosition) && Objects.equals(this.bucketPosition, that.bucketPosition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexerPosition, bucketPosition);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    public static TransformIndexerPosition fromXContent(XContentParser parser) {
        try {
            return PARSER.parse(parser, null);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
