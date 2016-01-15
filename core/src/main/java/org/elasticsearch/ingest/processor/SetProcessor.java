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

import org.elasticsearch.ingest.core.AbstractProcessorFactory;
import org.elasticsearch.ingest.core.IngestDocument;
import org.elasticsearch.ingest.core.TemplateService;
import org.elasticsearch.ingest.core.ValueSource;
import org.elasticsearch.ingest.core.ConfigurationUtils;
import org.elasticsearch.ingest.core.Processor;

import java.util.Map;

/**
 * Processor that adds new fields with their corresponding values. If the field is already present, its value
 * will be replaced with the provided one.
 */
public class SetProcessor implements Processor {

    public static final String TYPE = "set";

    private final String processorTag;
    private final TemplateService.Template field;
    private final ValueSource value;

    SetProcessor(String processorTag, TemplateService.Template field, ValueSource value) {
        this.processorTag = processorTag;
        this.field = field;
        this.value = value;
    }

    public TemplateService.Template getField() {
        return field;
    }

    public ValueSource getValue() {
        return value;
    }

    @Override
    public void execute(IngestDocument document) {
        document.setFieldValue(field, value);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public String getTag() {
        return processorTag;
    }

    public static final class Factory extends AbstractProcessorFactory<SetProcessor> {

        private final TemplateService templateService;

        public Factory(TemplateService templateService) {
            this.templateService = templateService;
        }

        @Override
        public SetProcessor doCreate(String processorTag, Map<String, Object> config) throws Exception {
            String field = ConfigurationUtils.readStringProperty(config, "field");
            Object value = ConfigurationUtils.readObject(config, "value");
            return new SetProcessor(processorTag, templateService.compile(field), ValueSource.wrap(value, templateService));
        }
    }
}
