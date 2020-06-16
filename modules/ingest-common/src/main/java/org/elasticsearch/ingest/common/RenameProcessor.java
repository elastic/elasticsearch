/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.TemplateScript;

import java.util.Map;

/**
 * Processor that allows to rename existing fields. Will throw exception if the field is not present.
 */
public final class RenameProcessor extends AbstractProcessor {

    public static final String TYPE = "rename";

    private final TemplateScript.Factory field;
    private final TemplateScript.Factory targetField;
    private final boolean ignoreMissing;

    RenameProcessor(String tag, String description, TemplateScript.Factory field, TemplateScript.Factory targetField,
                    boolean ignoreMissing) {
        super(tag, description);
        this.field = field;
        this.targetField = targetField;
        this.ignoreMissing = ignoreMissing;
    }

    TemplateScript.Factory getField() {
        return field;
    }

    TemplateScript.Factory getTargetField() {
        return targetField;
    }

    boolean isIgnoreMissing() {
        return ignoreMissing;
    }

    @Override
    public IngestDocument execute(IngestDocument document) {
        String path = document.renderTemplate(field);
        if (document.hasField(path, true) == false) {
            if (ignoreMissing) {
                return document;
            } else {
                throw new IllegalArgumentException("field [" + path + "] doesn't exist");
            }
        }
        // We fail here if the target field point to an array slot that is out of range.
        // If we didn't do this then we would fail if we set the value in the target_field
        // and then on failure processors would not see that value we tried to rename as we already
        // removed it.
        String target = document.renderTemplate(targetField);
        if (document.hasField(target, true)) {
            throw new IllegalArgumentException("field [" + target + "] already exists");
        }

        Object value = document.getFieldValue(path, Object.class);
        document.removeField(path);
        try {
            document.setFieldValue(target, value);
        } catch (Exception e) {
            // setting the value back to the original field shouldn't as we just fetched it from that field:
            document.setFieldValue(path, value);
            throw e;
        }
        return document;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory implements Processor.Factory {

        private final ScriptService scriptService;

        public Factory(ScriptService scriptService) {
            this.scriptService = scriptService;
        }

        @Override
        public RenameProcessor create(Map<String, Processor.Factory> registry, String processorTag,
                                      String description, Map<String, Object> config) throws Exception {
            String field = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "field");
            TemplateScript.Factory fieldTemplate = ConfigurationUtils.compileTemplate(TYPE, processorTag,
                "field", field, scriptService);
            String targetField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "target_field");
            TemplateScript.Factory targetFieldTemplate = ConfigurationUtils.compileTemplate(TYPE, processorTag,
                "target_field", targetField, scriptService);
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "ignore_missing", false);
            return new RenameProcessor(processorTag, description, fieldTemplate, targetFieldTemplate , ignoreMissing);
        }
    }
}
