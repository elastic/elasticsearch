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

import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.TemplateService;
import org.elasticsearch.ingest.ValueSource;

import java.util.Map;

/**
 * Processor that appends value or values to existing lists. If the field is not present a new list holding the
 * provided values will be added. If the field is a scalar it will be converted to a single item list and the provided
 * values will be added to the newly created list.
 */
public class AppendProcessor implements Processor {

    public static final String TYPE = "append";

    private final TemplateService.Template field;
    private final ValueSource value;

    AppendProcessor(TemplateService.Template field, ValueSource value) {
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
    public void execute(IngestDocument ingestDocument) throws Exception {
        ingestDocument.appendFieldValue(field, value);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory implements Processor.Factory<AppendProcessor> {

        private final TemplateService templateService;

        public Factory(TemplateService templateService) {
            this.templateService = templateService;
        }

        @Override
        public AppendProcessor create(Map<String, Object> config) throws Exception {
            String field = ConfigurationUtils.readStringProperty(config, "field");
            Object value = ConfigurationUtils.readObject(config, "value");
            return new AppendProcessor(templateService.compile(field), ValueSource.wrap(value, templateService));
        }
    }
}
