/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.transforms.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.xpack.core.transform.TransformField;

import java.util.HashMap;
import java.util.Map;

import static java.util.function.Predicate.not;

public class DocumentConversionUtils {

    private static final Logger logger = LogManager.getLogger(DocumentConversionUtils.class);

    public static IndexRequest convertDocumentToIndexRequest(Map<String, Object> document,
                                                             String destinationIndex,
                                                             String destinationPipeline) {
        String id = (String) document.get(TransformField.DOCUMENT_ID_FIELD);
        if (id == null) {
            throw new RuntimeException("Expected a document id but got null.");
        }

        document = removeInternalFields(document);
        return new IndexRequest(destinationIndex).id(id).source(document).setPipeline(destinationPipeline);
    }

    /**
     * Removes all the internal entries from the map. The entry is considered internal when the key starts with underscore character ('_').
     * The original document is *not* changed. The method returns a new document instead.
     *
     * TODO: Find out how to properly handle user-provided fields whose names start with underscore character ('_').
     */
    public static <V> Map<String, V> removeInternalFields(Map<String, V> document) {
        return document.entrySet().stream()
            .filter(not(e -> e.getKey() != null && e.getKey().startsWith("_")))
            // Workaround for handling null keys properly. For details see https://bugs.openjdk.java.net/browse/JDK-8148463
            .collect(HashMap::new, (m, e) -> m.put(e.getKey(), e.getValue()), HashMap::putAll);
    }

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
