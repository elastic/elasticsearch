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

package org.elasticsearch.ingest.processor;

import org.elasticsearch.ingest.core.AbstractProcessor;
import org.elasticsearch.ingest.core.AbstractProcessorFactory;
import org.elasticsearch.ingest.core.IngestDocument;
import org.elasticsearch.ingest.core.ConfigurationUtils;

import java.util.Map;

/**
 * Processor that allows to rename existing fields. Will throw exception if the field is not present.
 */
public class RenameProcessor extends AbstractProcessor {

    public static final String TYPE = "rename";

    private final String oldFieldName;
    private final String newFieldName;

    RenameProcessor(String tag, String oldFieldName, String newFieldName) {
        super(tag);
        this.oldFieldName = oldFieldName;
        this.newFieldName = newFieldName;
    }

    String getOldFieldName() {
        return oldFieldName;
    }

    String getNewFieldName() {
        return newFieldName;
    }

    @Override
    public void execute(IngestDocument document) {
        if (document.hasField(oldFieldName) == false) {
            throw new IllegalArgumentException("field [" + oldFieldName + "] doesn't exist");
        }
        if (document.hasField(newFieldName)) {
            throw new IllegalArgumentException("field [" + newFieldName + "] already exists");
        }

        Object oldValue = document.getFieldValue(oldFieldName, Object.class);
        document.setFieldValue(newFieldName, oldValue);
        try {
            document.removeField(oldFieldName);
        } catch (Exception e) {
            //remove the new field if the removal of the old one failed
            document.removeField(newFieldName);
            throw e;
        }
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static class Factory extends AbstractProcessorFactory<RenameProcessor> {
        @Override
        public RenameProcessor doCreate(String processorTag, Map<String, Object> config) throws Exception {
            String field = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "field");
            String newField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "to");
            return new RenameProcessor(processorTag, field, newField);
        }
    }
}
