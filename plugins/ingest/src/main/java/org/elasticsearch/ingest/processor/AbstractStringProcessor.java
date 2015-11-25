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

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Base class for processors that manipulate strings and require a single "fields" array config value, which
 * holds a list of field names in string format.
 */
public abstract class AbstractStringProcessor implements Processor {

    private final Collection<String> fields;

    protected AbstractStringProcessor(Collection<String> fields) {
        this.fields = fields;
    }

    public Collection<String> getFields() {
        return fields;
    }

    @Override
    public final void execute(IngestDocument document) {
        for(String field : fields) {
            String val = document.getFieldValue(field, String.class);
            if (val == null) {
                throw new IllegalArgumentException("field [" + field + "] is null, cannot process it.");
            }
            document.setFieldValue(field, process(val));
        }
    }

    protected abstract String process(String value);

    public static abstract class Factory<T extends AbstractStringProcessor> implements Processor.Factory<T> {
        @Override
        public T create(Map<String, Object> config) throws IOException {
            List<String> fields = ConfigurationUtils.readList(config, "fields");
            return newProcessor(Collections.unmodifiableList(fields));
        }

        protected abstract T newProcessor(Collection<String> fields);
    }
}
