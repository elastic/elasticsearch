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

package org.elasticsearch.ingest.processor.set;

import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.processor.ConfigurationUtils;
import org.elasticsearch.ingest.processor.Processor;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

/**
 * Processor that adds new fields with their corresponding values. If the field is already present, its value
 * will be replaced with the provided one.
 */
public class SetProcessor implements Processor {

    public static final String TYPE = "set";

    private final String field;
    private final Object value;

    SetProcessor(String field, Object value) {
        this.field = field;
        this.value = value;
    }

    String getField() {
        return field;
    }

    Object getValue() {
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

    public static final class Factory implements Processor.Factory<SetProcessor> {
        @Override
        public SetProcessor create(Map<String, Object> config) throws Exception {
            String field = ConfigurationUtils.readStringProperty(config, "field");
            Object value = ConfigurationUtils.readObject(config, "value");
            return new SetProcessor(field, value);
        }
    }
}
