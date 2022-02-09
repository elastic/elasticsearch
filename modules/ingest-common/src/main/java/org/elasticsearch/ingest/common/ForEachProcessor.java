/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.WrappingProcessor;
import org.elasticsearch.script.ScriptService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import static org.elasticsearch.ingest.ConfigurationUtils.newConfigurationException;
import static org.elasticsearch.ingest.ConfigurationUtils.readBooleanProperty;
import static org.elasticsearch.ingest.ConfigurationUtils.readMap;
import static org.elasticsearch.ingest.ConfigurationUtils.readStringProperty;

/**
 * Processor that executes another processor for each value in a list or map field.
 *
 * This can be useful for performing string operations on arrays of strings,
 * removing or modifying a field in objects inside arrays or maps, etc.
 */
public final class ForEachProcessor extends AbstractProcessor implements WrappingProcessor {

    public static final String TYPE = "foreach";

    private final String field;
    private final Processor processor;
    private final boolean ignoreMissing;

    ForEachProcessor(String tag, String description, String field, Processor processor, boolean ignoreMissing) {
        super(tag, description);
        this.field = field;
        this.processor = processor;
        this.ignoreMissing = ignoreMissing;
    }

    boolean isIgnoreMissing() {
        return ignoreMissing;
    }

    @Override
    public void execute(IngestDocument ingestDocument, BiConsumer<IngestDocument, Exception> handler) {
        Object o = ingestDocument.getFieldValue(field, Object.class, ignoreMissing);
        if (o == null) {
            if (ignoreMissing) {
                handler.accept(ingestDocument, null);
            } else {
                handler.accept(null, new IllegalArgumentException("field [" + field + "] is null, cannot loop over its elements."));
            }
        } else if (o instanceof Map<?, ?> map) {
            List<?> keys = new ArrayList<>(map.keySet());
            innerExecuteMap(0, new HashMap<Object, Object>(map), keys, Maps.newMapWithExpectedSize(map.size()), ingestDocument, handler);
        } else if (o instanceof List<?> list) {
            innerExecuteList(0, new ArrayList<>(list), new ArrayList<>(list.size()), ingestDocument, handler);
        } else {
            throw new IllegalArgumentException(
                "field [" + field + "] of type [" + o.getClass().getName() + "] cannot be cast to a " + "list or map"
            );
        }
    }

    void innerExecuteMap(
        int keyIndex,
        Map<?, ?> map,
        List<?> keys,
        Map<Object, Object> newValues,
        IngestDocument document,
        BiConsumer<IngestDocument, Exception> handler
    ) {
        for (; keyIndex < keys.size(); keyIndex++) {
            AtomicBoolean shouldContinueHere = new AtomicBoolean();
            String key = (String) keys.get(keyIndex);
            Object previousKey = document.getIngestMetadata().put("_key", key);
            Object value = map.get(key);
            Object previousValue = document.getIngestMetadata().put("_value", value);
            int nextIndex = keyIndex + 1;
            processor.execute(document, (result, e) -> {
                String newKey = (String) document.getIngestMetadata().get("_key");
                if (Strings.hasText(newKey)) {
                    newValues.put(newKey, document.getIngestMetadata().put("_value", previousValue));
                }
                document.getIngestMetadata().put("_key", previousKey);
                if (e != null || result == null) {
                    handler.accept(result, e);
                } else if (shouldContinueHere.getAndSet(true)) {
                    innerExecuteMap(nextIndex, map, keys, newValues, document, handler);
                }
            });

            if (shouldContinueHere.getAndSet(true) == false) {
                return;
            }
        }

        if (keyIndex == keys.size()) {
            document.setFieldValue(field, new HashMap<>(newValues));
            handler.accept(document, null);
        }
    }

    void innerExecuteList(
        int index,
        List<?> values,
        List<Object> newValues,
        IngestDocument document,
        BiConsumer<IngestDocument, Exception> handler
    ) {
        for (; index < values.size(); index++) {
            AtomicBoolean shouldContinueHere = new AtomicBoolean();
            Object value = values.get(index);
            Object previousValue = document.getIngestMetadata().put("_value", value);
            int nextIndex = index + 1;
            processor.execute(document, (result, e) -> {
                newValues.add(document.getIngestMetadata().put("_value", previousValue));
                if (e != null || result == null) {
                    handler.accept(result, e);
                } else if (shouldContinueHere.getAndSet(true)) {
                    innerExecuteList(nextIndex, values, newValues, document, handler);
                }
            });

            if (shouldContinueHere.getAndSet(true) == false) {
                return;
            }
        }

        if (index == values.size()) {
            document.setFieldValue(field, new ArrayList<>(newValues));
            handler.accept(document, null);
        }
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        throw new UnsupportedOperationException("this method should not get executed");
    }

    @Override
    public String getType() {
        return TYPE;
    }

    String getField() {
        return field;
    }

    public Processor getInnerProcessor() {
        return processor;
    }

    public static final class Factory implements Processor.Factory {

        private final ScriptService scriptService;

        Factory(ScriptService scriptService) {
            this.scriptService = scriptService;
        }

        @Override
        public ForEachProcessor create(Map<String, Processor.Factory> factories, String tag, String description, Map<String, Object> config)
            throws Exception {
            String field = readStringProperty(TYPE, tag, config, "field");
            boolean ignoreMissing = readBooleanProperty(TYPE, tag, config, "ignore_missing", false);
            Map<String, Map<String, Object>> processorConfig = readMap(TYPE, tag, config, "processor");
            Set<Map.Entry<String, Map<String, Object>>> entries = processorConfig.entrySet();
            if (entries.size() != 1) {
                throw newConfigurationException(TYPE, tag, "processor", "Must specify exactly one processor type");
            }
            Map.Entry<String, Map<String, Object>> entry = entries.iterator().next();
            Processor processor = ConfigurationUtils.readProcessor(factories, scriptService, entry.getKey(), entry.getValue());
            return new ForEachProcessor(tag, description, field, processor, ignoreMissing);
        }
    }
}
