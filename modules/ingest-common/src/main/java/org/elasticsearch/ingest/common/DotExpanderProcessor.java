/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;

import java.util.ArrayList;
import java.util.Map;

public final class DotExpanderProcessor extends AbstractProcessor {

    static final String TYPE = "dot_expander";

    private final String path;
    private final String field;
    private final boolean override;

    DotExpanderProcessor(String tag, String description, String path, String field) {
        this(tag, description, path, field, false);
    }

    DotExpanderProcessor(String tag, String description, String path, String field, boolean override) {
        super(tag, description);
        this.path = path;
        this.field = field;
        this.override = override;
    }

    @Override
    @SuppressWarnings("unchecked")
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        String pathToExpand;
        Map<String, Object> map;
        if (this.path != null) {
            pathToExpand = this.path + "." + field;
            map = ingestDocument.getFieldValue(this.path, Map.class);
        } else {
            pathToExpand = field;
            map = ingestDocument.getSourceAndMetadata();
        }

        if (this.field.equals("*")) {
            for (String key : new ArrayList<>(map.keySet())) {
                if (key.indexOf('.') > 0) {
                    pathToExpand = this.path != null ? this.path + "." + key : key;
                    expandDot(ingestDocument, pathToExpand, key, map);
                }
            }
        } else {
            expandDot(ingestDocument, pathToExpand, field, map);
        }

        return ingestDocument;
    }

    private void expandDot(IngestDocument ingestDocument, String pathToExpand, String fieldName, Map<String, Object> map) {
        if (map.containsKey(fieldName)) {
            Object value = map.remove(fieldName);
            if (ingestDocument.hasField(pathToExpand)) {
                if (override) {
                    ingestDocument.setFieldValue(pathToExpand, value);
                } else {
                    ingestDocument.appendFieldValue(pathToExpand, value);
                }
            } else {
                // check whether we actually can expand the field in question into an object field.
                // part of the path may already exist and if part of it would be a value field (string, integer etc.)
                // then we can't override it with an object field and we should fail with a good reason.
                // IngestDocument#setFieldValue(...) would fail too, but the error isn't very understandable
                for (int index = pathToExpand.indexOf('.'); index != -1; index = pathToExpand.indexOf('.', index + 1)) {
                    String partialPath = pathToExpand.substring(0, index);
                    if (ingestDocument.hasField(partialPath)) {
                        Object val = ingestDocument.getFieldValue(partialPath, Object.class);
                        if ((val instanceof Map) == false) {
                            throw new IllegalArgumentException(
                                "cannot expand ["
                                    + pathToExpand
                                    + "], because ["
                                    + partialPath
                                    + "] is not an object field, but a value field"
                            );
                        }
                    } else {
                        break;
                    }
                }
                ingestDocument.setFieldValue(pathToExpand, value);
            }
        }
    }

    @Override
    public String getType() {
        return TYPE;
    }

    String getPath() {
        return path;
    }

    String getField() {
        return field;
    }

    public static final class Factory implements Processor.Factory {

        @Override
        public Processor create(
            Map<String, Processor.Factory> processorFactories,
            String tag,
            String description,
            Map<String, Object> config
        ) throws Exception {
            String field = ConfigurationUtils.readStringProperty(TYPE, tag, config, "field");
            if (field.contains(".") == false && field.equals("*") == false) {
                throw ConfigurationUtils.newConfigurationException(
                    ConfigurationUtils.TAG_KEY,
                    tag,
                    "field",
                    "field does not contain a dot and is not a wildcard"
                );
            }
            if (field.indexOf('.') == 0 || field.lastIndexOf('.') == field.length() - 1) {
                throw ConfigurationUtils.newConfigurationException(
                    ConfigurationUtils.TAG_KEY,
                    tag,
                    "field",
                    "Field can't start or end with a dot"
                );
            }
            int firstIndex = -1;
            for (int index = field.indexOf('.'); index != -1; index = field.indexOf('.', index + 1)) {
                if (index - firstIndex == 1) {
                    throw ConfigurationUtils.newConfigurationException(ConfigurationUtils.TAG_KEY, tag, "field", "No space between dots");
                }
                firstIndex = index;
            }

            String path = ConfigurationUtils.readOptionalStringProperty(TYPE, tag, config, "path");
            boolean override = ConfigurationUtils.readBooleanProperty(TYPE, tag, config, "override", false);
            return new DotExpanderProcessor(tag, null, path, field, override);
        }
    }
}
