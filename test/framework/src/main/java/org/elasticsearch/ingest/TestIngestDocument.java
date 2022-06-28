/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Construct ingest documents for testing purposes
 */
public class TestIngestDocument {

    /**
     * Create an IngestDocument for testing that pass an empty mutable map for ingestMetaata
     */
    public static IngestDocument ofSourceAndMetadata(Map<String, Object> sourceAndMetadata) {
        return new IngestDocument(sourceAndMetadata, new HashMap<>());
    }

    /**
     * Create an IngestDocument with a metadata map and validators.  The metadata map is passed by reference, not copied, so callers
     * can observe changes to the map directly.
     */
    public static IngestDocument ofMetadataWithValidator(Map<String, Object> metadata, Map<String, BiConsumer<String, Object>> validators) {
        return new IngestDocument(new IngestSourceAndMetadata(new HashMap<>(), metadata, null, validators), new HashMap<>());
    }

    /**
     * Create an empty ingest document for testing
     */
    public static IngestDocument emptyIngestDocument() {
        return new IngestDocument(new HashMap<>(), new HashMap<>());
    }

    public static Tuple<String, Object> randomMetadata() {
        IngestDocument.Metadata metadata = ESTestCase.randomFrom(IngestDocument.Metadata.values());
        return new Tuple<>(metadata.getFieldName(), switch (metadata) {
            case VERSION, IF_SEQ_NO, IF_PRIMARY_TERM -> ESTestCase.randomIntBetween(0, 124);
            case VERSION_TYPE -> VersionType.toString(ESTestCase.randomFrom(VersionType.values()));
            case DYNAMIC_TEMPLATES -> Map.of(ESTestCase.randomAlphaOfLengthBetween(5, 10), ESTestCase.randomAlphaOfLengthBetween(5, 10));
            default -> ESTestCase.randomAlphaOfLengthBetween(5, 10);
        });
    }
}
