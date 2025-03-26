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
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class EcsNamespacingProcessor extends AbstractProcessor {

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

    private static final String AGENT_PREFIX = "agent.";
    private static final String CLOUD_PREFIX = "cloud.";
    private static final String HOST_PREFIX = "host.";

    private static final String ATTRIBUTES_KEY = "attributes";
    private static final String RESOURCE_KEY = "resource";
    private static final String SCOPE_KEY = "scope";
    private static final String BODY_KEY = "body";
    private static final String TEXT_KEY = "text";
    private static final String STRUCTURED_KEY = "structured";

    EcsNamespacingProcessor(String tag, String description) {
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
        Object oldAttributes = source.remove(ATTRIBUTES_KEY);
        if (oldAttributes != null) {
            newAttributes.put(ATTRIBUTES_KEY, oldAttributes);
        }
        source.put(ATTRIBUTES_KEY, newAttributes);

        Map<String, Object> newResource = new HashMap<>();
        Map<String, Object> newResourceAttributes = new HashMap<>();
        newResource.put(ATTRIBUTES_KEY, newResourceAttributes);
        Object oldResource = source.remove(RESOURCE_KEY);
        if (oldResource != null) {
            newAttributes.put(RESOURCE_KEY, oldResource);
        }
        source.put(RESOURCE_KEY, newResource);

        renameSpecialKeys(document);

        // Iterate through all top level keys and move them to the appropriate namespace
        for (String key : Sets.newHashSet(source.keySet())) {
            if (KEEP_KEYS.contains(key)) {
                continue;
            }
            if (shouldMoveToResourceAttributes(key)) {
                Object value = source.remove(key);
                newResourceAttributes.put(key, value);
            } else {
                Object value = source.remove(key);
                newAttributes.put(key, value);
            }
        }

        // Flatten attributes
        source.replace(ATTRIBUTES_KEY, Maps.flatten(newAttributes, false, false));
        newResource.replace(ATTRIBUTES_KEY, Maps.flatten(newResourceAttributes, false, false));

        return document;
    }

    @SuppressWarnings("unchecked")
    boolean isOTelDocument(Map<String, Object> source) {
        Object resource = source.get(RESOURCE_KEY);
        if (resource instanceof Map == false) {
            return false;
        } else {
            Object resourceAttributes = ((Map<String, Object>) resource).get(ATTRIBUTES_KEY);
            if (resourceAttributes != null && (resourceAttributes instanceof Map) == false) {
                return false;
            }
        }

        Object scope = source.get(SCOPE_KEY);
        if (scope instanceof Map == false) {
            return false;
        }

        Object attributes = source.get(ATTRIBUTES_KEY);
        if (attributes != null && attributes instanceof Map == false) {
            return false;
        }

        Object body = source.get(BODY_KEY);
        if (body != null) {
            if (body instanceof Map == false) {
                return false;
            }
            Object bodyText = ((Map<String, Object>) body).get(TEXT_KEY);
            if (bodyText != null && (bodyText instanceof String) == false) {
                return false;
            }
            Object bodyStructured = ((Map<String, Object>) body).get(STRUCTURED_KEY);
            return (bodyStructured instanceof String) == false;
        }
        return true;
    }

    private void renameSpecialKeys(IngestDocument document) {
        RENAME_KEYS.forEach((nonOtelName, otelName) -> {
            // first look assuming dot notation
            Object value = document.getFieldValue(nonOtelName, Object.class, true);
            if (value != null) {
                document.removeField(nonOtelName);
                // remove the parent field if it is empty
                int lastDot = nonOtelName.lastIndexOf('.');
                if (lastDot > 0) {
                    String parent = nonOtelName.substring(0, lastDot);
                    document.removeField(parent, true);
                }
            } else if (nonOtelName.contains(".")) {
                // look for dotted field names
                value = document.getSource().remove(nonOtelName);
            }
            if (value != null) {
                document.setFieldValue(otelName, value);
            }
        });
    }

    private boolean shouldMoveToResourceAttributes(String key) {
        return key.startsWith(AGENT_PREFIX)
            || key.equals("agent")
            || key.startsWith(CLOUD_PREFIX)
            || key.equals("cloud")
            || key.startsWith(HOST_PREFIX)
            || key.equals("host");
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
