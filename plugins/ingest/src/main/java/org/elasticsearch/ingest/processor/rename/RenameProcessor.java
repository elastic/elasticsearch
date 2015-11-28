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

package org.elasticsearch.ingest.processor.rename;

import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.processor.ConfigurationUtils;
import org.elasticsearch.ingest.processor.Processor;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * Processor that allows to rename existing fields. Will throw exception if the field is not present.
 */
public class RenameProcessor implements Processor {

    public static final String TYPE = "rename";

    private final Map<String, String> fields;

    RenameProcessor(Map<String, String> fields) {
        this.fields = fields;
    }

    Map<String, String> getFields() {
        return fields;
    }

    @Override
    public void execute(IngestDocument document) {
        for(Map.Entry<String, String> entry : fields.entrySet()) {
            if (document.hasField(entry.getKey())) {
                if (document.hasField(entry.getKey()) == false) {
                    throw new IllegalArgumentException("field [" + entry.getKey() + "] doesn't exist");
                }
                Object oldValue = document.getFieldValue(entry.getKey(), Object.class);
                document.removeField(entry.getKey());
                document.setFieldValue(entry.getValue(), oldValue);
            }
        }
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static class Factory implements Processor.Factory<RenameProcessor> {
        @Override
        public RenameProcessor create(Map<String, Object> config) throws IOException {
            Map<String, String> fields = ConfigurationUtils.readMap(config, "fields");
            return new RenameProcessor(Collections.unmodifiableMap(fields));
        }
    }
}
