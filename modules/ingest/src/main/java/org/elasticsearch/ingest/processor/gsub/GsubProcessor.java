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

package org.elasticsearch.ingest.processor.gsub;

import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.processor.ConfigurationUtils;
import org.elasticsearch.ingest.processor.Processor;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Processor that allows to search for patterns in field content and replace them with corresponding string replacement.
 * Support fields of string type only, throws exception if a field is of a different type.
 */
public class GsubProcessor implements Processor {

    public static final String TYPE = "gsub";

    private final String field;
    private final Pattern pattern;
    private final String replacement;

    GsubProcessor(String field, Pattern pattern, String replacement) {
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

    public static class Factory implements Processor.Factory<GsubProcessor> {
        @Override
        public GsubProcessor create(Map<String, Object> config) throws Exception {
            String field = ConfigurationUtils.readStringProperty(config, "field");
            String pattern = ConfigurationUtils.readStringProperty(config, "pattern");
            String replacement = ConfigurationUtils.readStringProperty(config, "replacement");
            Pattern searchPattern = Pattern.compile(pattern);
            return new GsubProcessor(field, searchPattern, replacement);
        }
    }
}
