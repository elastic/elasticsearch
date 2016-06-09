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

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.ingest.core.ConfigurationUtils.newConfigurationException;
import static org.elasticsearch.ingest.core.ConfigurationUtils.readStringProperty;

/**
 * Processor that allows to search for patterns in field content and replace them with corresponding string replacement.
 * Support fields of string type only, throws exception if a field is of a different type.
 */
public final class GsubProcessor extends AbstractProcessor {

    public static final String TYPE = "gsub";

    private final String field;
    private final Pattern pattern;
    private final String replacement;

    GsubProcessor(String tag, String field, Pattern pattern, String replacement) {
        super(tag);
        this.field = field;
        this.pattern = pattern;
        this.replacement = replacement;
    }

    String getField() {
        return field;
    }

    Pattern getPattern() {
        return pattern;
    }

    String getReplacement() {
        return replacement;
    }


    @Override
    public void execute(IngestDocument document) {
        String oldVal = document.getFieldValue(field, String.class);
        if (oldVal == null) {
            throw new IllegalArgumentException("field [" + field + "] is null, cannot match pattern.");
        }
        Matcher matcher = pattern.matcher(oldVal);
        String newVal = matcher.replaceAll(replacement);
        document.setFieldValue(field, newVal);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory extends AbstractProcessorFactory<GsubProcessor> {
        @Override
        public GsubProcessor doCreate(String processorTag, Map<String, Object> config) throws Exception {
            String field = readStringProperty(TYPE, processorTag, config, "field");
            String pattern = readStringProperty(TYPE, processorTag, config, "pattern");
            String replacement = readStringProperty(TYPE, processorTag, config, "replacement");
            Pattern searchPattern;
            try {
                searchPattern = Pattern.compile(pattern);
            } catch (Exception e) {
                throw newConfigurationException(TYPE, processorTag, "pattern", "Invalid regex pattern. " + e.getMessage());
            }
            return new GsubProcessor(processorTag, field, searchPattern, replacement);
        }
    }
}
