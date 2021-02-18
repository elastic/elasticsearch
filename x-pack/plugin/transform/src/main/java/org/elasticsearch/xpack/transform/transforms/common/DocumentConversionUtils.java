/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.index.IndexRequest;

import java.util.HashMap;
import java.util.Map;

import static java.util.function.Predicate.not;

/**
 * Helper functions for converting raw maps into documents and determining index mappings
 */
public class DocumentConversionUtils {

    private static final Logger logger = LogManager.getLogger(DocumentConversionUtils.class);

    /**
     * Convert a raw string, object map to a valid index request
     * @param docId The document ID
     * @param document The document contents
     * @param destinationIndex The index where the document is to be indexed
     * @param destinationPipeline Optional destination pipeline
     * @return A valid {@link IndexRequest}
     */
    public static IndexRequest convertDocumentToIndexRequest(String docId,
                                                             Map<String, Object> document,
                                                             String destinationIndex,
                                                             String destinationPipeline) {
        if (docId == null) {
            throw new RuntimeException("Expected a document id but got null.");
        }
        return new IndexRequest(destinationIndex).id(docId).source(document).setPipeline(destinationPipeline);
    }

    /**
     * Removes all the internal entries from the map. The entry is considered internal when the key starts with underscore character ('_').
     * The original document is *not* changed. The method returns a new document instead.
     *
     * TODO: Find out how to properly handle user-provided fields whose names start with underscore character ('_').
     * @param document the document to index represented as a {@link Map}
     * @param <V> Value type of document map.
     * @return A new {@link Map} but with all keys that start with "_" removed
     */
    public static <V> Map<String, V> removeInternalFields(Map<String, V> document) {
        return document.entrySet().stream()
            .filter(not(e -> e.getKey() != null && e.getKey().startsWith("_")))
            // Workaround for handling null keys properly. For details see https://bugs.openjdk.java.net/browse/JDK-8148463
            .collect(HashMap::new, (m, e) -> m.put(e.getKey(), e.getValue()), HashMap::putAll);
    }

    /**
     * Extract the field mapping values from the field capabilities response
     *
     * @param response The {@link FieldCapabilitiesResponse} for the indices from which we want the field maps
     * @return A {@link Map} mapping "field_name" to the mapped type
     */
    public static Map<String, String> extractFieldMappings(FieldCapabilitiesResponse response) {
        Map<String, String> extractedTypes = new HashMap<>();

        response.get()
            .forEach(
                (fieldName, capabilitiesMap) -> {
                    // TODO: overwrites types, requires resolve if types are mixed
                    capabilitiesMap.forEach((name, capability) -> {
                        logger.trace(() -> new ParameterizedMessage("Extracted type for [{}] : [{}]", fieldName, capability.getType()));
                        extractedTypes.put(fieldName, capability.getType());
                    });
                }
            );
        return extractedTypes;
    }

    private DocumentConversionUtils() {}
}
