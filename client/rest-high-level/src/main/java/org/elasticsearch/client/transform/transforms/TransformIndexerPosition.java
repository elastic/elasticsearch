/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform.transforms;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Holds state of the cursors:
 *
 *  indexer_position: the position of the indexer querying the source
 *  bucket_position: the position used for identifying changes
 */
public class TransformIndexerPosition {
    public static final ParseField INDEXER_POSITION = new ParseField("indexer_position");
    public static final ParseField BUCKET_POSITION = new ParseField("bucket_position");

    private final Map<String, Object> indexerPosition;
    private final Map<String, Object> bucketPosition;

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<TransformIndexerPosition, Void> PARSER = new ConstructingObjectParser<>(
            "transform_indexer_position",
            true,
            args -> new TransformIndexerPosition((Map<String, Object>) args[0],(Map<String, Object>) args[1]));

    static {
        PARSER.declareField(optionalConstructorArg(), XContentParser::mapOrdered, INDEXER_POSITION, ValueType.OBJECT);
        PARSER.declareField(optionalConstructorArg(), XContentParser::mapOrdered, BUCKET_POSITION, ValueType.OBJECT);
    }

    public TransformIndexerPosition(Map<String, Object> indexerPosition, Map<String, Object> bucketPosition) {
        this.indexerPosition = indexerPosition == null ? null : Collections.unmodifiableMap(indexerPosition);
        this.bucketPosition = bucketPosition == null ? null : Collections.unmodifiableMap(bucketPosition);
    }

    public Map<String, Object> getIndexerPosition() {
        return indexerPosition;
    }

    public Map<String, Object> getBucketsPosition() {
        return bucketPosition;
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

        return Objects.equals(this.indexerPosition, that.indexerPosition) &&
            Objects.equals(this.bucketPosition, that.bucketPosition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexerPosition, bucketPosition);
    }

    public static TransformIndexerPosition fromXContent(XContentParser parser) {
        try {
            return PARSER.parse(parser, null);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
