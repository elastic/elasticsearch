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

package org.elasticsearch.ingest.common;

import java.util.Map;
import java.util.regex.Pattern;

import static org.elasticsearch.ingest.ConfigurationUtils.newConfigurationException;
import static org.elasticsearch.ingest.ConfigurationUtils.readStringProperty;

/**
 * Processor that allows to search for patterns in field content and replace them with corresponding string replacement.
 * Support fields of string type only, throws exception if a field is of a different type.
 */
public final class GsubProcessor extends AbstractStringProcessor {

    public static final String TYPE = "gsub";

    private final Pattern pattern;
    private final String replacement;

    GsubProcessor(String tag, String field, Pattern pattern, String replacement, boolean ignoreMissing, String targetField) {
        super(tag, field, ignoreMissing, targetField);
        this.pattern = pattern;
        this.replacement = replacement;
    }

    Pattern getPattern() {
        return pattern;
    }

    String getReplacement() {
        return replacement;
    }

    @Override
    protected String process(String value) {
        return pattern.matcher(value).replaceAll(replacement);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory extends AbstractStringProcessor.Factory {

        public Factory() {
            super(TYPE);
        }

        @Override
        protected AbstractStringProcessor newProcessor(String processorTag, Map<String, Object> config, String field,
                                                       boolean ignoreMissing, String targetField) {
            String pattern = readStringProperty(TYPE, processorTag, config, "pattern");
            String replacement = readStringProperty(TYPE, processorTag, config, "replacement");
            Pattern searchPattern;
            try {
                searchPattern = Pattern.compile(pattern);
            } catch (Exception e) {
                throw newConfigurationException(TYPE, processorTag, "pattern", "Invalid regex pattern. " + e.getMessage());
            }
            return new GsubProcessor(processorTag, field, searchPattern, replacement, ignoreMissing, targetField);
        }
    }
}
