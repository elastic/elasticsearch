/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.otel;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static java.util.Map.entry;

/**
 * This processor is responsible for transforming non-OpenTelemetry-compliant documents into a namespaced flavor of ECS
 * that makes them compatible with OpenTelemetry.
 * It DOES NOT translate the entire ECS schema into OpenTelemetry semantic conventions.
 *
 * <p>More specifically, this processor performs the following operations:
 * <ul>
 *   <li>Renames specific ECS fields to their corresponding OpenTelemetry-compatible counterparts.</li>
 *   <li>Moves all other fields to the "attributes" namespace.</li>
 *   <li>Flattens all attributes in the "attributes" namespace.</li>
 *   <li>Moves resource fields from the "attributes" namespace to the "resource.attributes" namespace.</li>
 * </ul>
 *
 * <p>If a document is identified as OpenTelemetry-compatible, no transformation is performed.
 * @see org.elasticsearch.ingest.AbstractProcessor
 */
public class NormalizeForStreamProcessor extends AbstractProcessor {

    public static final String TYPE = "normalize_for_stream";

    /**
     * Mapping of ECS field names to their corresponding OpenTelemetry-compatible counterparts.
     */
    private static final Map<String, String> RENAME_KEYS = Map.ofEntries(
        entry("span.id", "span_id"),
        entry("message", "body.text"),
        entry("log.level", "severity_text"),
        entry("trace.id", "trace_id")
    );

    /**
     * A closed-set of keys that should be kept at the top level of the processed document after applying the namespacing.
     * In essence, these are the fields that should not be moved to the "attributes" or "resource.attributes" namespaces.
     * Besides the @timestamp field, this set obviously contains the attributes and the resource fields, as well as the
     * OpenTelemetry-compatible fields that are renamed by the processor.
     */
    private static final Set<String> KEEP_KEYS;
    static {
        Set<String> keepKeys = new HashSet<>(Set.of("@timestamp", "attributes", "resource"));
        Set<String> renamedTopLevelFields = new HashSet<>();
        for (String value : RENAME_KEYS.values()) {
            // if the renamed field is nested, we only need to know the top level field
            int dotIndex = value.indexOf('.');
            if (dotIndex != -1) {
                renamedTopLevelFields.add(value.substring(0, dotIndex));
            } else {
                renamedTopLevelFields.add(value);
            }
        }
        keepKeys.addAll(renamedTopLevelFields);
        KEEP_KEYS = Set.copyOf(keepKeys);
    }

    private static final String ATTRIBUTES_KEY = "attributes";
    private static final String RESOURCE_KEY = "resource";
    private static final String SCOPE_KEY = "scope";
    private static final String BODY_KEY = "body";
    private static final String TEXT_KEY = "text";
    private static final String STRUCTURED_KEY = "structured";

    NormalizeForStreamProcessor(String tag, String description) {
        super(tag, description);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public IngestDocument execute(IngestDocument document) {
        Map<String, Object> source = document.getSource();

        boolean isOTel = isOTelDocument(source);
        if (isOTel) {
            return document;
        }

        // non-OTel document

        Map<String, Object> newAttributes = new HashMap<>();
        // The keep keys indicate the fields that should be kept at the top level later on when applying the namespacing.
        // However, at this point we need to move their original values (if they exist) to the one of the new attributes namespaces, except
        // for the @timestamp field. The assumption is that at this point the document is not OTel compliant, so even if a valid top
        // level field is found, we assume that it does not bear the OTel semantics.
        for (String keepKey : KEEP_KEYS) {
            if (keepKey.equals("@timestamp")) {
                continue;
            }
            if (source.containsKey(keepKey)) {
                newAttributes.put(keepKey, source.remove(keepKey));
            }
        }

        source.put(ATTRIBUTES_KEY, newAttributes);

        renameSpecialKeys(document);

        // move all top level keys except from specific ones to the "attributes" namespace
        final var sourceItr = source.entrySet().iterator();
        while (sourceItr.hasNext()) {
            final var entry = sourceItr.next();
            if (KEEP_KEYS.contains(entry.getKey()) == false) {
                newAttributes.put(entry.getKey(), entry.getValue());
                sourceItr.remove();
            }
        }

        // Flatten attributes
        Map<String, Object> flattenAttributes = Maps.flatten(newAttributes, false, false);
        source.put(ATTRIBUTES_KEY, flattenAttributes);

        Map<String, Object> newResource = new HashMap<>();
        Map<String, Object> newResourceAttributes = new HashMap<>();
        newResource.put(ATTRIBUTES_KEY, newResourceAttributes);
        source.put(RESOURCE_KEY, newResource);
        moveResourceAttributes(flattenAttributes, newResourceAttributes);

        return document;
    }

    /**
     * Checks if the given document is OpenTelemetry-compliant.
     *
     * <p>A document is considered OpenTelemetry-compliant if it meets the following criteria:
     * <ul>
     *   <li>The "resource" field is present and is a map
     *   <li>The resource field either doesn't contain an "attributes" field, or the "attributes" field is a map.</li>
     *   <li>The "scope" field is either absent or a map.</li>
     *   <li>The "attributes" field is either absent or a map.</li>
     *   <li>The "body" field is either absent or a map.</li>
     *   <li>If exists, the "body" either doesn't contain a "text" field, or the "text" field is a string.</li>
     *   <li>If exists, the "body" either doesn't contain a "structured" field, or the "structured" field is not a string.</li>
     * </ul>
     *
     * @param source the document to check
     * @return {@code true} if the document is OpenTelemetry-compliant, {@code false} otherwise
     */
    static boolean isOTelDocument(Map<String, Object> source) {
        Object resource = source.get(RESOURCE_KEY);
        if (resource instanceof Map<?, ?> resourceMap) {
            Object resourceAttributes = resourceMap.get(ATTRIBUTES_KEY);
            if (resourceAttributes != null && (resourceAttributes instanceof Map) == false) {
                return false;
            }
        } else {
            return false;
        }

        Object scope = source.get(SCOPE_KEY);
        if (scope != null && scope instanceof Map == false) {
            return false;
        }

        Object attributes = source.get(ATTRIBUTES_KEY);
        if (attributes != null && attributes instanceof Map == false) {
            return false;
        }

        Object body = source.get(BODY_KEY);
        if (body != null) {
            if (body instanceof Map<?, ?> bodyMap) {
                Object bodyText = bodyMap.get(TEXT_KEY);
                if (bodyText != null && (bodyText instanceof String) == false) {
                    return false;
                }
                Object bodyStructured = bodyMap.get(STRUCTURED_KEY);
                return (bodyStructured instanceof String) == false;
            } else {
                return false;
            }
        }
        return true;
    }

    /**
     * Renames specific ECS keys in the given document to their OpenTelemetry-compatible counterparts, based on the {@code RENAME_KEYS} map.
     *
     * <p>This method performs the following operations:
     * <ul>
     *   <li>For each key in the {@code RENAME_KEYS} map, it checks if a corresponding field exists in the document. It first looks for the
     *   field assuming dot notation for nested fields. If the field is not found, it looks for a top level field with a dotted name.</li>
     *   <li>If the field exists, it removes it from the document and adds a new field with the corresponding name from the
     *   {@code RENAME_KEYS} map and the same value.</li>
     *   <li>If the key is nested (contains dots), it recursively removes empty parent fields after renaming.</li>
     * </ul>
     *
     * @param document the document to process
     */
    static void renameSpecialKeys(IngestDocument document) {
        RENAME_KEYS.forEach((nonOtelName, otelName) -> {
            boolean fieldExists = false;
            Object value = null;
            // first look assuming dot notation for nested fields
            if (document.hasField(nonOtelName)) {
                fieldExists = true;
                value = document.getFieldValue(nonOtelName, Object.class, true);
                document.removeField(nonOtelName);
                // recursively remove empty parent fields
                int lastDot = nonOtelName.lastIndexOf('.');
                while (lastDot > 0) {
                    String parentName = nonOtelName.substring(0, lastDot);
                    // parent should never be null and must be a map if we are here
                    @SuppressWarnings("unchecked")
                    Map<String, Object> parent = (Map<String, Object>) document.getFieldValue(parentName, Map.class);
                    if (parent.isEmpty()) {
                        document.removeField(parentName);
                    } else {
                        break;
                    }
                    lastDot = parentName.lastIndexOf('.');
                }
            } else if (nonOtelName.contains(".")) {
                // look for dotted field names
                Map<String, Object> source = document.getSource();
                if (source.containsKey(nonOtelName)) {
                    fieldExists = true;
                    value = source.remove(nonOtelName);
                }
            }
            if (fieldExists) {
                document.setFieldValue(otelName, value);
            }
        });
    }

    private static void moveResourceAttributes(Map<String, Object> attributes, Map<String, Object> resourceAttributes) {
        Set<String> ecsResourceFields = EcsOTelResourceAttributes.LATEST;
        Iterator<Map.Entry<String, Object>> attributeIterator = attributes.entrySet().iterator();
        while (attributeIterator.hasNext()) {
            Map.Entry<String, Object> entry = attributeIterator.next();
            if (ecsResourceFields.contains(entry.getKey())) {
                resourceAttributes.put(entry.getKey(), entry.getValue());
                attributeIterator.remove();
            }
        }
    }

    public static final class Factory implements Processor.Factory {
        @Override
        public Processor create(
            Map<String, Processor.Factory> registry,
            String tag,
            String description,
            Map<String, Object> config,
            ProjectId projectId
        ) {
            return new NormalizeForStreamProcessor(tag, description);
        }
    }
}
