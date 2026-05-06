/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import io.opentelemetry.proto.common.v1.KeyValue;

import org.elasticsearch.core.Nullable;

import java.util.List;

/**
 * Elastic-specific OTLP attributes that configure per-document indexing metadata.
 */
public final class DocumentMetadata {

    public static final String ATTRIBUTE_PREFIX = "elasticsearch";
    public static final String DOCUMENT_ID_ATTRIBUTE = ATTRIBUTE_PREFIX + ".document_id";
    public static final String INGEST_PIPELINE_ATTRIBUTE = ATTRIBUTE_PREFIX + ".ingest_pipeline";

    private DocumentMetadata() {}

    /**
     * Extracts the Elasticsearch document id metadata attribute, if present.
     */
    public static @Nullable String documentId(List<KeyValue> attributes) {
        return attributeValue(DOCUMENT_ID_ATTRIBUTE, attributes);
    }

    /**
     * Extracts the Elasticsearch ingest pipeline metadata attribute, if present.
     */
    public static @Nullable String ingestPipeline(List<KeyValue> attributes) {
        return attributeValue(INGEST_PIPELINE_ATTRIBUTE, attributes);
    }

    /**
     * Returns whether an attribute configures per-document indexing metadata.
     */
    public static boolean isDocumentMetadataAttribute(String attributeKey) {
        return attributeKey.equals(DOCUMENT_ID_ATTRIBUTE) || attributeKey.equals(INGEST_PIPELINE_ATTRIBUTE);
    }

    private static @Nullable String attributeValue(String key, List<KeyValue> attributes) {
        for (int i = 0, size = attributes.size(); i < size; i++) {
            KeyValue attribute = attributes.get(i);
            if (key.equals(attribute.getKey())) {
                return attribute.getValue().getStringValue();
            }
        }
        return null;
    }
}
