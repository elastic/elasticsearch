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
import org.elasticsearch.ingest.core.TemplateService;
import org.elasticsearch.ingest.core.ValueSource;
import org.elasticsearch.ingest.core.ConfigurationUtils;

import java.util.Map;

/**
 * Processor that adds new fields with their corresponding values. If the field is already present, its value
 * will be replaced with the provided one.
 */
public final class SetProcessor extends AbstractProcessor {

    public static final String TYPE = "set";

    private final boolean overrideEnabled;
    private final TemplateService.Template field;
    private final ValueSource value;

    SetProcessor(String tag, TemplateService.Template field, ValueSource value)  {
        this(tag, field, value, true);
    }

    SetProcessor(String tag, TemplateService.Template field, ValueSource value, boolean overrideEnabled)  {
        super(tag);
        this.overrideEnabled = overrideEnabled;
        this.field = field;
        this.value = value;
    }

    public boolean isOverrideEnabled() {
        return overrideEnabled;
    }

    public TemplateService.Template getField() {
        return field;
    }

    public ValueSource getValue() {
        return value;
    }

    @Override
    public void execute(IngestDocument document) {
        if (overrideEnabled || document.hasField(field) == false || document.getFieldValue(field, Object.class) == null) {
            document.setFieldValue(field, value);
        }
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory extends AbstractProcessorFactory<SetProcessor> {

        private final TemplateService templateService;

        public Factory(TemplateService templateService) {
            this.templateService = templateService;
        }

        @Override
        public SetProcessor doCreate(String processorTag, Map<String, Object> config) throws Exception {
            String field = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "field");
            Object value = ConfigurationUtils.readObject(TYPE, processorTag, config, "value");
            boolean overrideEnabled = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "override", true);
            return new SetProcessor(
                    processorTag,
                    templateService.compile(field),
                    ValueSource.wrap(value, templateService),
                    overrideEnabled);
        }
    }
}
