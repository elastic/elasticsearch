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

package org.elasticsearch.ingest.processor.simple;

import org.elasticsearch.ingest.Data;
import org.elasticsearch.ingest.processor.Processor;

import java.io.IOException;
import java.util.Map;

public final class SimpleProcessor implements Processor {

    public static final String TYPE = "simple";

    private final String path;
    private final String expectedValue;

    private final String addField;
    private final String addFieldValue;

    public SimpleProcessor(String path, String expectedValue, String addField, String addFieldValue) {
        this.path = path;
        this.expectedValue = expectedValue;
        this.addField = addField;
        this.addFieldValue = addFieldValue;
    }

    @Override
    public void execute(Data data) {
        Object value = data.getProperty(path);
        if (value != null) {
            if (value.toString().equals(this.expectedValue)) {
                data.addField(addField, addFieldValue);
            }
        }
    }

    public static class Factory implements Processor.Factory {

        public Processor create(Map<String, Object> config) {
            String path = (String) config.get("path");
            String expectedValue = (String) config.get("expected_value");
            String addField = (String) config.get("add_field");
            String addFieldValue = (String) config.get("add_field_value");
            return new SimpleProcessor(path, expectedValue, addField, addFieldValue);
        }

    }

}
