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

package org.elasticsearch.ingest.processor.remove;

import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.processor.ConfigurationUtils;
import org.elasticsearch.ingest.processor.Processor;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Processor that removes existing fields. Nothing happens if the field is not present.
 */
public class RemoveProcessor implements Processor {

    public static final String TYPE = "remove";

    private final Collection<String> fields;

    RemoveProcessor(Collection<String> fields) {
        this.fields = fields;
    }

    Collection<String> getFields() {
        return fields;
    }

    @Override
    public void execute(IngestDocument document) {
        for(String field : fields) {
            document.removeField(field);
        }
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static class Factory implements Processor.Factory<RemoveProcessor> {
        @Override
        public RemoveProcessor create(Map<String, Object> config) throws IOException {
            List<String> fields = ConfigurationUtils.readList(config, "fields");
            return new RemoveProcessor(Collections.unmodifiableList(fields));
        }
    }
}

