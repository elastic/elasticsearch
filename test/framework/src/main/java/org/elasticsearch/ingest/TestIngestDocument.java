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

/**
 * Construct ingest documents for testing purposes
 */
public class TestIngestDocument {

    /**
     * These two test static factory methods are needed for testing and allow to the creation of a new {@link IngestDocument} given the
     * provided elasticsearch metadata, source and ingest metadata.
     *
     * This is needed because the ingest metadata will be initialized with the current timestamp at init time, which makes equality
     * comparisons impossible in tests.
     */
    public static IngestDocument ofSourceAndIngest(Map<String, Object> sourceAndMetadata, Map<String, Object> ingestMetadata) {
        return new IngestDocument(sourceAndMetadata, ingestMetadata);
    }

    /**
     * Create an IngestDocument for testing as in {@link #ofSourceAndIngest(Map, Map)} but pass an empty mutable map for ingestMetaata
     */
    public static IngestDocument ofSourceAndMetadata(Map<String, Object> sourceAndMetadata) {
        return new IngestDocument(sourceAndMetadata, new HashMap<>());
    }

    /**
     * Create an empty ingest document for testing
     */
    public static IngestDocument emptyIngestDocument() {
        return new IngestDocument(new HashMap<>(), new HashMap<>());
    }
}
