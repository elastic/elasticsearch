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

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * Processor that adds new fields with their corresponding values. If the field is already present, its value
 * will be replaced with the provided one.
 */
public class SetProcessor implements Processor {

    public static final String TYPE = "set";

    private final Map<String, Object> fields;

    SetProcessor(Map<String, Object> fields) {
        this.fields = fields;
    }

    Map<String, Object> getFields() {
        return fields;
    }

    @Override
    public void execute(IngestDocument document) {
        for(Map.Entry<String, Object> entry : fields.entrySet()) {
            document.setFieldValue(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory implements Processor.Factory<SetProcessor> {
        @Override
        public SetProcessor create(Map<String, Object> config) throws IOException {
            Map<String, Object> fields = ConfigurationUtils.readMap(config, "fields");
            return new SetProcessor(Collections.unmodifiableMap(fields));
        }
    }
}
