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

package org.elasticsearch.ingest;

import org.elasticsearch.ingest.core.AbstractProcessor;
import org.elasticsearch.ingest.core.AbstractProcessorFactory;
import org.elasticsearch.ingest.core.ConfigurationUtils;
import org.elasticsearch.ingest.core.IngestDocument;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Processor that splits fields content into different items based on the occurrence of a specified separator.
 * New field value will be an array containing all of the different extracted items.
 * Throws exception if the field is null or a type other than string.
 */
public final class SplitProcessor extends AbstractProcessor {

    public static final String TYPE = "split";

    private final String field;
    private final String separator;

    SplitProcessor(String tag, String field, String separator) {
        super(tag);
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
        String[] strings = oldVal.split(separator);
        List<String> splitList = new ArrayList<>(strings.length);
        Collections.addAll(splitList, strings);
        document.setFieldValue(field, splitList);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static class Factory extends AbstractProcessorFactory<SplitProcessor> {
        @Override
        public SplitProcessor doCreate(String processorTag, Map<String, Object> config) throws Exception {
            String field = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "field");
            return new SplitProcessor(processorTag, field, ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "separator"));
        }
    }
}
