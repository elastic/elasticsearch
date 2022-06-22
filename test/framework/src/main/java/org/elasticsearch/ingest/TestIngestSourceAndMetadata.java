/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * Construct {@link IngestSourceAndMetadata} for testing purposes
 */
public class TestIngestSourceAndMetadata {
    public static IngestSourceAndMetadata withoutValidators(Map<String, Object> source, Map<String, Object> metadata) {
        Map<String, BiFunction<String, Object, Object>> validators = new HashMap<>();
        for (String key : metadata.keySet()) {
            validators.put(key, IngestSourceAndMetadata.VALIDATORS.getOrDefault(key, (k, v) -> v));
        }
        return new IngestSourceAndMetadata(source, metadata, null, validators);
    }
}
