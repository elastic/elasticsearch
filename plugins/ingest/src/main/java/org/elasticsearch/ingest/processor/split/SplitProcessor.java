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

package org.elasticsearch.ingest.processor.split;

import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.processor.ConfigurationUtils;
import org.elasticsearch.ingest.processor.Processor;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

/**
 * Processor that splits fields content into different items based on the occurrence of a specified separator.
 * New field value will be an array containing all of the different extracted items.
 * Throws exception if the field is null or a type other than string.
 */
public class SplitProcessor implements Processor {

    public static final String TYPE = "split";

    private final String field;
    private final String separator;

    SplitProcessor(String field, String separator) {
        this.field = field;
        this.separator = separator;
    }

    String getField() {
        return field;
    }

    String getSeparator() {
        return separator;
    }

    @Override
    public void execute(IngestDocument document) {
        String oldVal = document.getFieldValue(field, String.class);
        if (oldVal == null) {
            throw new IllegalArgumentException("field [" + field + "] is null, cannot split.");
        }
        document.setFieldValue(field, Arrays.asList(oldVal.split(separator)));
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static class Factory implements Processor.Factory<SplitProcessor> {
        @Override
        public SplitProcessor create(Map<String, Object> config) throws Exception {
            String field = ConfigurationUtils.readStringProperty(config, "field");
            return new SplitProcessor(field, ConfigurationUtils.readStringProperty(config, "separator"));
        }
    }
}
