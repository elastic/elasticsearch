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

package org.elasticsearch.ingest.grok;

import org.elasticsearch.ingest.core.AbstractProcessor;
import org.elasticsearch.ingest.core.AbstractProcessorFactory;
import org.elasticsearch.ingest.core.ConfigurationUtils;
import org.elasticsearch.ingest.core.IngestDocument;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.ingest.core.ConfigurationUtils.newConfigurationException;

public final class GrokProcessor extends AbstractProcessor {

    public static final String TYPE = "grok";

    private final String matchField;
    private final Grok grok;

    public GrokProcessor(String tag, Grok grok, String matchField) {
        super(tag);
        this.matchField = matchField;
        this.grok = grok;
    }

    @Override
    public void execute(IngestDocument ingestDocument) throws Exception {
        String fieldValue = ingestDocument.getFieldValue(matchField, String.class);
        Map<String, Object> matches = grok.captures(fieldValue);
        if (matches != null) {
            matches.forEach((k, v) -> ingestDocument.setFieldValue(k, v));
        } else {
            throw new IllegalArgumentException("Grok expression does not match field value: [" + fieldValue + "]");
        }
    }

    @Override
    public String getType() {
        return TYPE;
    }

    String getMatchField() {
        return matchField;
    }

    Grok getGrok() {
        return grok;
    }

    public final static class Factory extends AbstractProcessorFactory<GrokProcessor> {

        private final Map<String, String> builtinPatterns;

        public Factory(Map<String, String> builtinPatterns) {
            this.builtinPatterns = builtinPatterns;
        }

        @Override
        public GrokProcessor doCreate(String processorTag, Map<String, Object> config) throws Exception {
            String matchField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "field");
            String matchPattern = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "pattern");
            Map<String, String> customPatternBank = ConfigurationUtils.readOptionalMap(TYPE, processorTag, config, "pattern_definitions");
            Map<String, String> patternBank = new HashMap<>(builtinPatterns);
            if (customPatternBank != null) {
                patternBank.putAll(customPatternBank);
            }

            Grok grok;
            try {
                grok = new Grok(patternBank, matchPattern);
            } catch (Exception e) {
                throw newConfigurationException(TYPE, processorTag, "pattern", "Invalid regex pattern. " + e.getMessage());
            }
            return new GrokProcessor(processorTag, grok, matchField);
        }

    }

}
