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

package org.elasticsearch.ingest.processor.join;

import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.processor.ConfigurationUtils;
import org.elasticsearch.ingest.processor.Processor;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Processor that joins the different items of an array into a single string value using a separator between each item.
 * Throws exception is the specified field is not an array.
 */
public class JoinProcessor implements Processor {

    public static final String TYPE = "join";

    private final Map<String, String> fields;

    JoinProcessor(Map<String, String> fields) {
        this.fields = fields;
    }

    Map<String, String> getFields() {
        return fields;
    }

    @Override
    public void execute(IngestDocument document) {
        for(Map.Entry<String, String> entry : fields.entrySet()) {
            List<?> list = document.getPropertyValue(entry.getKey(), List.class);
            if (list == null) {
                throw new IllegalArgumentException("field [" + entry.getKey() + "] is null, cannot join.");
            }
            String joined = list.stream()
                    .map(Object::toString)
                    .collect(Collectors.joining(entry.getValue()));
            document.setPropertyValue(entry.getKey(), joined);
        }
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static class Factory implements Processor.Factory<JoinProcessor> {
        @Override
        public JoinProcessor create(Map<String, Object> config) throws IOException {
            Map<String, String> fields = ConfigurationUtils.readMap(config, "fields");
            return new JoinProcessor(Collections.unmodifiableMap(fields));
        }
    }
}

