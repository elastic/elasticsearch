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
import org.elasticsearch.ingest.core.IngestDocument;
import org.elasticsearch.ingest.core.ConfigurationUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Processor that joins the different items of an array into a single string value using a separator between each item.
 * Throws exception is the specified field is not an array.
 */
public final class JoinProcessor extends AbstractProcessor {

    public static final String TYPE = "join";

    private final String field;
    private final String separator;

    JoinProcessor(String tag, String field, String separator) {
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
        List<?> list = document.getFieldValue(field, List.class);
        if (list == null) {
            throw new IllegalArgumentException("field [" + field + "] is null, cannot join.");
        }
        String joined = list.stream()
                .map(Object::toString)
                .collect(Collectors.joining(separator));
        document.setFieldValue(field, joined);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public final static class Factory extends AbstractProcessorFactory<JoinProcessor> {
        @Override
        public JoinProcessor doCreate(String processorTag, Map<String, Object> config) throws Exception {
            String field = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "field");
            String separator = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "separator");
            return new JoinProcessor(processorTag, field, separator);
        }
    }
}

