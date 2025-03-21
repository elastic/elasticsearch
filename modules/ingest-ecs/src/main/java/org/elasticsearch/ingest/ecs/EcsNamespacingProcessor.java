/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.ecs;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@SuppressWarnings("unchecked")
public final class EcsNamespacingProcessor extends AbstractProcessor {

    public static final String TYPE = "ecs_namespacing";

    private static final Set<String> KEEP_KEYS = Set.of(
        "@timestamp",
        "observed_timestamp",
        "trace_id",
        "span_id",
        "severity_text",
        "body",
        "severity_number",
        "event_name",
        "attributes",
        "resource",
        "dropped_attributes_count",
        "scope"
    );

    private static final Map<String, String> RENAME_KEYS = Map.of(
        "span.id",
        "span_id",
        "message",
        "body.text",
        "log.level",
        "severity_text",
        "trace.id",
        "trace_id"
    );

    private static final List<String> SPECIAL_KEYS = List.of("error.exception.type", "span.id", "log.level", "trace.id");

    private static final String AGENT_PREFIX = "agent.";
    private static final String CLOUD_PREFIX = "cloud.";
    private static final String HOST_PREFIX = "host.";

    private static final String ATTRIBUTES_KEY = "attributes";
    private static final String ATTRIBUTES_PREFIX = ATTRIBUTES_KEY + ".";
    private static final String ATTRIBUTES_ATTRIBUTES_KEY = ATTRIBUTES_PREFIX + ATTRIBUTES_KEY;
    private static final String RESOURCE_KEY = "resource";
    private static final String RESOURCE_PREFIX = RESOURCE_KEY + ".";
    private static final String RESOURCE_ATTRIBUTES_KEY = RESOURCE_PREFIX + ATTRIBUTES_KEY;

    EcsNamespacingProcessor(String tag, String description) {
        super(tag, description);
    }

    @Override
    public IngestDocument execute(IngestDocument document) {

        initializeAttributes(document);

        normalizeSpecialKeys(document);

        // Iterate through all key/value pairs of the incoming document
        for (String key : IngestDocument.getAllTopLevelFields(document.getSource())) {
            if (shouldKeep(key)) {
                continue;
            }
            if (renameKey(document, key)) {
                // all renamed keys are also keep-keys, so we continue here
                continue;
            }
            if (shouldMoveToResourceAttributes(key)) {
                moveToResourceAttributes(document, key);
            } else {
                moveToAttributes(document, key);
            }
        }
        return document;
    }

    /**
     * Initializes the attributes and resource attributes maps in the source map.
     * If the attributes or resource attributes maps already exist, they are used.
     * If the attributes or resource attributes maps are not maps, they are moved to the attributes map.
     */
    void initializeAttributes(IngestDocument document) {
        Object attributes = document.getTopLevelFieldValue(ATTRIBUTES_KEY);
        if (attributes == null) {
            attributes = new HashMap<>();
            document.setTopLevelFieldValue(ATTRIBUTES_KEY, attributes, false, false);
        } else {
            if (attributes instanceof Map == false) {
                Object existingAttributes = attributes;
                attributes = new HashMap<>();
                document.setTopLevelFieldValue(ATTRIBUTES_KEY, attributes, false, false);
                document.setDirectChildFieldValue(attributes, ATTRIBUTES_KEY, existingAttributes, ATTRIBUTES_ATTRIBUTES_KEY);
            }
        }

        Object resource = document.getTopLevelFieldValue(RESOURCE_KEY);
        if (resource == null) {
            initializeResource(document);
        } else {
            if (resource instanceof Map) {
                Object resourceAttributes = document.getDirectChildFieldValue(resource, ATTRIBUTES_KEY);
                if (resourceAttributes == null) {
                    document.setDirectChildFieldValue(resource, ATTRIBUTES_KEY, new HashMap<>(), RESOURCE_ATTRIBUTES_KEY);
                } else {
                    if (resourceAttributes instanceof Map == false) {
                        document.appendDirectChildFieldValue(attributes, RESOURCE_ATTRIBUTES_KEY, resourceAttributes);
                        document.setDirectChildFieldValue(resource, ATTRIBUTES_KEY, new HashMap<>(), RESOURCE_ATTRIBUTES_KEY);
                    }
                }
            } else {
                document.appendDirectChildFieldValue(attributes, RESOURCE_KEY, resource);
                initializeResource(document);
            }
        }

        // not strictly required as it would be handled by the moveToAttributes phase, but helps us test related edge cases in isolation
        Object resourceAttributes = document.getTopLevelFieldValue(RESOURCE_ATTRIBUTES_KEY);
        if (resourceAttributes != null) {
            document.appendDirectChildFieldValue(attributes, RESOURCE_ATTRIBUTES_KEY, resourceAttributes);
            document.removeTopLevelField(RESOURCE_ATTRIBUTES_KEY);
        }
    }

    private void initializeResource(IngestDocument document) {
        Map<String, Object> resource = new HashMap<>();
        document.setTopLevelFieldValue(RESOURCE_KEY, resource, false, false);
        Map<String, Object> resourceAttributes = new HashMap<>();
        document.setDirectChildFieldValue(resource, ATTRIBUTES_KEY, resourceAttributes, RESOURCE_ATTRIBUTES_KEY);
    }

    /**
     * Normalization of a field extracts and removes all values from a given map that have full path from the root that match the given key.
     * For example, given the key {@code error.exception.type} and the following document:
     * <pre>
     *  {
     *      "error": {
     *          "exception": {
     *              "type": "value1",
     *              "foo": "bar"
     *          },
     *          "exception.type": "value2",
     *          "foo": "bar"
     *      },
     *      "error.exception": {
     *          "type": "value3",
     *          "foo": "bar"
     *      },
     *      "error.exception.type": "value4"
     *  }
     *  </pre>
     *  The processor should properly resolve all paths that match the {@code error.exception.type}, collect and remove the values and set
     *  the result as a top level field. The resulting document should look like:
     *  <pre>
     *  {
     *      "error": {
     *          "exception": {
     *              "foo": "bar"
     *          },
     *          "foo": "bar"
     *      },
     *      "error.exception": {
     *          "foo": "bar"
     *      },
     *      "error.exception.type": ["value1", "value2", "value3", "value4"]
     *  }
     *  </pre>
     * The processor should also remove all keys that have paths that are resolved to {@code error.exception.type}
     * @param document The document to normalize
     */
    void normalizeSpecialKeys(IngestDocument document) {
        for (String specialKey : SPECIAL_KEYS) {
            document.normalizeField(specialKey);
        }
    }

    private boolean shouldKeep(String key) {
        return KEEP_KEYS.contains(key);
    }

    private boolean renameKey(IngestDocument document, String key) {
        boolean renamed = false;
        String newKey = RENAME_KEYS.get(key);
        if (newKey != null) {
            // todo - currently this always ends with a list of values, even if there is only one value. is this what we want?
            Object value = document.getTopLevelFieldValue(key);
            document.appendTopLevelFieldValue(newKey, value);
            document.removeTopLevelField(key);
            renamed = true;
        }
        return renamed;
    }

    private boolean shouldMoveToResourceAttributes(String key) {
        return key.startsWith(AGENT_PREFIX)
            || key.equals("agent")
            || key.startsWith(CLOUD_PREFIX)
            || key.equals("cloud")
            || key.startsWith(HOST_PREFIX)
            || key.equals("host");
    }

    private void moveToResourceAttributes(IngestDocument document, String key) {
        Object value = document.getTopLevelFieldValue(key);
        Object resourceAttributes = document.getFieldValue(RESOURCE_ATTRIBUTES_KEY, Map.class);
        document.appendDirectChildFieldValue(resourceAttributes, key, value);
        document.removeTopLevelField(key);
    }

    private void moveToAttributes(IngestDocument document, String key) {
        Object value = document.getTopLevelFieldValue(key);
        Object attributes = document.getFieldValue(ATTRIBUTES_KEY, Map.class);
        document.appendDirectChildFieldValue(attributes, key, value);
        document.removeTopLevelField(key);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory implements Processor.Factory {
        @Override
        public Processor create(
            Map<String, Processor.Factory> processorFactories,
            String tag,
            String description,
            Map<String, Object> config,
            ProjectId projectId
        ) throws Exception {
            return new EcsNamespacingProcessor(tag, description);
        }
    }
}
