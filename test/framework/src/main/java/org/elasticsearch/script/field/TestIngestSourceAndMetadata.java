/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import org.elasticsearch.ingest.IngestDocument;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * Build an {@link IngestSourceAndMetadata} without validators for testing
 */
public class TestIngestSourceAndMetadata {
    private static final Map<String, BiFunction<String, Object, Object>> VALIDATORS;
    static {
        Map<String, BiFunction<String, Object, Object>> validators = new HashMap<>(IngestSourceAndMetadata.VALIDATORS);
        validators.replace(IngestDocument.Metadata.VERSION.getFieldName(), (key, value) -> {
            if (value == null) {
                return null;
            }
            if (value instanceof Number num) {
                return num;
            }
            throw new IllegalArgumentException("invalid [" + key + "]: [" + value + "]");
        });
        VALIDATORS = validators;
    }

    public static IngestSourceAndMetadata withoutVersionValidation(Map<String, Object> ctx) {
        return new IngestSourceAndMetadata(IngestSourceAndMetadata.extractMetadata(ctx), ctx, null, VALIDATORS);
    }

    public static IngestSourceAndMetadata withoutVersionValidation(IngestSourceAndMetadata ingestSourceAndMetadata) {
        return new IngestSourceAndMetadata(
            ingestSourceAndMetadata.source,
            ingestSourceAndMetadata.metadata,
            ingestSourceAndMetadata.timestamp,
            VALIDATORS
        );
    }
}
