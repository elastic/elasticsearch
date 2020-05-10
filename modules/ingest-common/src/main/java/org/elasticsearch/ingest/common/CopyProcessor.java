/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;

import java.util.Map;

import static org.elasticsearch.ingest.ConfigurationUtils.newConfigurationException;

/**
 * Processor that adds a new field with value copied from other existed field.
 */
public class CopyProcessor extends AbstractProcessor {

    public static final String TYPE = "copy";

    private final String field;
    private final String targetField;
    private final boolean addToRoot;
    private final boolean ignoreMissing;

    CopyProcessor(String tag, String field, String targetField, boolean addToRoot, boolean ignoreMissing)  {
        super(tag);
        this.field = field;
        this.targetField = targetField;
        this.addToRoot = addToRoot;
        this.ignoreMissing = ignoreMissing;
    }

    public String getField() {
        return field;
    }

    public String getTargetField() {
        return targetField;
    }

    public boolean isAddToRoot() {
        return addToRoot;
    }

    public boolean isIgnoreMissing() {
        return ignoreMissing;
    }

    private static void apply(Map<String, Object> ctx, Object value) {
        if (value instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) value;
            ctx.putAll(map);
        } else {
            throw new IllegalArgumentException("cannot add non-map fields to root of document");
        }
    }

    @Override
    public IngestDocument execute(IngestDocument document) {
        Object fieldValue = document.getFieldValue(field, Object.class, ignoreMissing);

        if (fieldValue == null && ignoreMissing) {
            return document;
        } else if (fieldValue == null) {
            throw new IllegalArgumentException("field [" + field + "] is null, cannot be copied");
        }

        if (field.equals(targetField)) {
            return document;
        }

        if (addToRoot) {
            apply(document.getSourceAndMetadata(), fieldValue);
        } else {
            document.setFieldValue(targetField, IngestDocument.deepCopy(fieldValue));
        }

        return document;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory implements Processor.Factory {
        @Override
        public CopyProcessor create(
            Map<String, Processor.Factory> registry, String processorTag,
            Map<String, Object> config) throws Exception {
            String field = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "field");
            String targetField = ConfigurationUtils.readOptionalStringProperty(TYPE, processorTag, config, "target_field");
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "ignore_missing", false);
            boolean addToRoot = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "add_to_root", false);

            if (addToRoot == false && targetField == null) {
                throw newConfigurationException(TYPE, processorTag, "target_field",
                    "either `target_field` or `add_to_root` must be set");
            }
            if (addToRoot && targetField != null) {
                throw newConfigurationException(TYPE, processorTag, "target_field",
                    "cannot set `target_field` while also setting `add_to_root` to true");
            }

            return new CopyProcessor(
                processorTag,
                field,
                targetField,
                addToRoot,
                ignoreMissing);
        }
    }
}
