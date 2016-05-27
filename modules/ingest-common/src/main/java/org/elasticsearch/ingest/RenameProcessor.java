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

package org.elasticsearch.ingest;

import org.elasticsearch.ingest.core.AbstractProcessor;
import org.elasticsearch.ingest.core.AbstractProcessorFactory;
import org.elasticsearch.ingest.core.IngestDocument;
import org.elasticsearch.ingest.core.ConfigurationUtils;

import java.util.Map;

/**
 * Processor that allows to rename existing fields. Will throw exception if the field is not present.
 */
public final class RenameProcessor extends AbstractProcessor {

    public static final String TYPE = "rename";

    private final String field;
    private final String targetField;

    RenameProcessor(String tag, String field, String targetField) {
        super(tag);
        this.field = field;
        this.targetField = targetField;
    }

    String getField() {
        return field;
    }

    String getTargetField() {
        return targetField;
    }

    @Override
    public void execute(IngestDocument document) {
        if (document.hasField(field) == false) {
            throw new IllegalArgumentException("field [" + field + "] doesn't exist");
        }
        if (document.hasField(targetField)) {
            throw new IllegalArgumentException("field [" + targetField + "] already exists");
        }

        Object oldValue = document.getFieldValue(field, Object.class);
        document.setFieldValue(targetField, oldValue);
        try {
            document.removeField(field);
        } catch (Exception e) {
            //remove the new field if the removal of the old one failed
            document.removeField(targetField);
            throw e;
        }
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory extends AbstractProcessorFactory<RenameProcessor> {
        @Override
        public RenameProcessor doCreate(String processorTag, Map<String, Object> config) throws Exception {
            String field = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "field");
            String targetField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "target_field");
            return new RenameProcessor(processorTag, field, targetField);
        }
    }
}
