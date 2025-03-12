/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
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
    private static final String RESOURCE_KEY = "resource";
    private static final String RESOURCE_PREFIX = RESOURCE_KEY + ".";
    private static final String RESOURCE_ATTRIBUTES_KEY = RESOURCE_PREFIX + ATTRIBUTES_KEY;

    // reusing the value list in order to avoid allocations, as in most cases we won't need it, or there will be only one value
    private static final ThreadLocal<List<Object>> threadLocalValues = ThreadLocal.withInitial(ArrayList::new);

    EcsNamespacingProcessor(String tag, String description) {
        super(tag, description);
    }

    @Override
    public IngestDocument execute(IngestDocument document) {
        Map<String, Object> source = document.getSourceAndMetadata();

        Map<String, Object> attributes;
        Map<String, Object> resourceAttributes;

        // Ensure attributes and resource objects exist
        Object existingAttributes = source.get(ATTRIBUTES_KEY);
        if (existingAttributes == null) {
            attributes = new HashMap<>();
            source.put(ATTRIBUTES_KEY, attributes);
        } else {
            if (existingAttributes instanceof Map) {
                attributes = (Map<String, Object>) existingAttributes;
            } else {
                attributes = new HashMap<>();
                attributes.put(ATTRIBUTES_KEY, existingAttributes);
                source.put(ATTRIBUTES_KEY, attributes);
            }
        }

        Object existingResource = source.get(RESOURCE_KEY);
        if (existingResource == null) {
            resourceAttributes = initializeResource(source);
        } else {
            if (existingResource instanceof Map) {
                Object existingResourceAttributes = ((Map<String, Object>) existingResource).get(ATTRIBUTES_KEY);
                if (existingResourceAttributes == null) {
                    resourceAttributes = new HashMap<>();
                    ((Map<String, Object>) existingResource).put(ATTRIBUTES_KEY, resourceAttributes);
                } else {
                    if (existingResourceAttributes instanceof Map) {
                        resourceAttributes = (Map<String, Object>) existingResourceAttributes;
                    } else {
                        // todo: adding a non-map "resource.attributes" value to "attributes.[resource.attributes]". is this what we want?
                        attributes.put(RESOURCE_ATTRIBUTES_KEY, existingResourceAttributes);
                        resourceAttributes = new HashMap<>();
                        ((Map<String, Object>) existingResource).put(ATTRIBUTES_KEY, resourceAttributes);
                    }
                }
            } else {
                attributes.put(RESOURCE_KEY, existingResource);
                resourceAttributes = initializeResource(source);
            }
        }

        // Normalize special keys
        normalizeSpecialKeys(source);

        // Iterate through all key/value pairs of the incoming document
        for (Map.Entry<String, Object> entry : source.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            if (shouldKeep(key)) {
                continue;
            } else if (renameKey(source, attributes, key, value)) {
                continue;
            } else if (shouldMoveToResourceAttributes(key)) {
                moveToResourceAttributes(resourceAttributes, key, value);
            } else {
                moveToAttributes(attributes, key, value);
            }
        }

        return document;
    }

    private static Map<String, Object> initializeResource(Map<String, Object> source) {
        Map<String, Object> resource = new HashMap<>();
        Map<String, Object> resourceAttributes = new HashMap<>();
        resource.put(ATTRIBUTES_KEY, resourceAttributes);
        source.put(RESOURCE_KEY, resource);
        return resourceAttributes;
    }

    void normalizeSpecialKeys(Map<String, Object> source) {
        List<Object> values = threadLocalValues.get();
        for (String specialKey : SPECIAL_KEYS) {
            values.clear();
            extractAndRemoveAllKeyValues(source, specialKey, 0, values);
            if (values.isEmpty() == false) {
                if (values.size() == 1) {
                    source.put(specialKey, values.getFirst());
                } else {
                    // allocating a new list only if needed
                    source.put(specialKey, new ArrayList<>(values));
                }
            }
        }
    }

    /**
     * Extracts and removes all values from a given map that have full path from the root that match the given key.
     * For example, given the key {@code error.exception.type} and the following map:
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
     *  The processor should properly resolve all paths that match the {@code error.exception.type} key and extract the values:
     *  <ul>
     *      <li>value1</li>
     *      <li>value2</li>
     *      <li>value3</li>
     *      <li>value4</li>
     *  </ul>
     * The processor should also remove all keys that have paths that are resolved to {@code error.exception.type}
     * @param root The root map to search for the key
     * @param key The key to search for
     * @param startIndex The starting index to search for the key
     * @param values The list to add the extracted values to
     */
    void extractAndRemoveAllKeyValues(Map<String, Object> root, String key, int startIndex, List<Object> values) {
        int nextDot = key.indexOf('.', startIndex);
        String testKey;
        if (nextDot < 0) {
            testKey = key;
        } else {
            testKey = key.substring(0, nextDot);
        }
        Object value = root.get(testKey);
        if (value != null) {
            if (value instanceof Map) {
                // noinspection unchecked
                extractAndRemoveAllKeyValues((Map<String, Object>) value, key.substring(nextDot + 1), 0, values);
            } else {
                values.add(value);
                root.remove(testKey);
            }
        }

        if (nextDot > 0) {
            extractAndRemoveAllKeyValues(root, key, nextDot + 1, values);
        }
    }

    private boolean shouldKeep(String key) {
        return KEEP_KEYS.contains(key);
    }

    private boolean renameKey(Map<String, Object> source, Map<String, Object> attributes, String key, Object value) {
        boolean renamed = false;
        String newKey = RENAME_KEYS.get(key);
        if (newKey != null) {
            appendValue(source, value, newKey);
            renamed = true;
        }
        return renamed;
    }

    /**
     * Appends a value to the source map without resolving dot notation for the given key.
     * @param source The map to append the value to
     * @param value The value to append
     * @param key The key to append the value to
     */
    private void appendValue(Map<String, Object> source, Object value, String key) {
        Object existingValue = source.get(key);
        if (existingValue != null) {
            if (existingValue instanceof List) {
                ((List<Object>) existingValue).add(value);
            } else {
                List<Object> newValue = new ArrayList<>();
                newValue.add(existingValue);
                newValue.add(value);
                source.put(key, newValue);
            }
        } else {
            source.put(key, value);
        }
    }

    private boolean shouldMoveToResourceAttributes(String key) {
        return key.startsWith(AGENT_PREFIX)
            || key.equals("agent")
            || key.startsWith(CLOUD_PREFIX)
            || key.equals("cloud")
            || key.startsWith(HOST_PREFIX)
            || key.equals("host")
            || key.startsWith(RESOURCE_PREFIX);
    }

    private void moveToResourceAttributes(Map<String, Object> resourceAttributes, String key, Object value) {
        if (key.startsWith(RESOURCE_PREFIX)) {
            key = key.substring(RESOURCE_PREFIX.length());
        }
        appendValue(resourceAttributes, value, key);
    }

    private void moveToAttributes(Map<String, Object> attributes, String key, Object value) {
        if (key.startsWith(ATTRIBUTES_PREFIX)) {
            key = key.substring(ATTRIBUTES_PREFIX.length());
        }
        appendValue(attributes, value, key);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory implements Processor.Factory {
        @Override
        public EcsNamespacingProcessor create(
            Map<String, Processor.Factory> registry,
            String processorTag,
            String description,
            Map<String, Object> config
        ) throws Exception {
            return new EcsNamespacingProcessor(processorTag, description);
        }
    }
}
