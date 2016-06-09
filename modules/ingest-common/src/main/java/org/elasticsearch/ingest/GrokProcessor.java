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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.ingest.core.ConfigurationUtils.newConfigurationException;

public final class GrokProcessor extends AbstractProcessor {

    public static final String TYPE = "grok";
    private static final String PATTERN_MATCH_KEY = "_ingest._grok_match_index";

    private final String matchField;
    private final Grok grok;
    private final boolean traceMatch;

    public GrokProcessor(String tag, Map<String, String> patternBank, List<String> matchPatterns, String matchField) {
        this(tag, patternBank, matchPatterns, matchField, false);
    }

    public GrokProcessor(String tag, Map<String, String> patternBank, List<String> matchPatterns, String matchField, boolean traceMatch) {
        super(tag);
        this.matchField = matchField;
        this.grok = new Grok(patternBank, combinePatterns(matchPatterns, traceMatch));
        this.traceMatch = traceMatch;
    }

    @Override
    public void execute(IngestDocument ingestDocument) throws Exception {
        String fieldValue = ingestDocument.getFieldValue(matchField, String.class);
        Map<String, Object> matches = grok.captures(fieldValue);
        if (matches == null) {
            throw new IllegalArgumentException("Provided Grok expressions do not match field value: [" + fieldValue + "]");
        }

        matches.entrySet().stream()
            .filter((e) -> Objects.nonNull(e.getValue()))
            .forEach((e) -> ingestDocument.setFieldValue(e.getKey(), e.getValue()));

        if (traceMatch) {
            @SuppressWarnings("unchecked")
            HashMap<String, String> matchMap = (HashMap<String, String>) ingestDocument.getFieldValue(PATTERN_MATCH_KEY, Object.class);
            matchMap.keySet().stream().findFirst().ifPresent((index) -> {
                ingestDocument.setFieldValue(PATTERN_MATCH_KEY, index);
            });
        }
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public Grok getGrok() {
        return grok;
    }

    String getMatchField() {
        return matchField;
    }

    static String combinePatterns(List<String> patterns, boolean traceMatch) {
        String combinedPattern;
        if (patterns.size() > 1) {
            if (traceMatch) {
                combinedPattern = "";
                for (int i = 0; i < patterns.size(); i++) {
                    String valueWrap = "(?<" + PATTERN_MATCH_KEY + "." + i + ">" + patterns.get(i) + ")";
                    if (combinedPattern.equals("")) {
                        combinedPattern = valueWrap;
                    } else {
                        combinedPattern = combinedPattern + "|" + valueWrap;
                    }
                }
            } else {
                combinedPattern = patterns.stream().reduce("", (prefix, value) -> {
                    if (prefix.equals("")) {
                        return "(?:" + value + ")";
                    } else {
                        return prefix + "|" + "(?:" + value + ")";
                    }
                });
            }
        }  else {
            combinedPattern = patterns.get(0);
        }

        return combinedPattern;
    }

    public final static class Factory extends AbstractProcessorFactory<GrokProcessor> {

        private final Map<String, String> builtinPatterns;

        public Factory(Map<String, String> builtinPatterns) {
            this.builtinPatterns = builtinPatterns;
        }

        @Override
        public GrokProcessor doCreate(String processorTag, Map<String, Object> config) throws Exception {
            String matchField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "field");
            List<String> matchPatterns = ConfigurationUtils.readList(TYPE, processorTag, config, "patterns");
            boolean traceMatch = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "trace_match", false);

            if (matchPatterns.isEmpty()) {
                throw newConfigurationException(TYPE, processorTag, "patterns", "List of patterns must not be empty");
            }
            Map<String, String> customPatternBank = ConfigurationUtils.readOptionalMap(TYPE, processorTag, config, "pattern_definitions");
            Map<String, String> patternBank = new HashMap<>(builtinPatterns);
            if (customPatternBank != null) {
                patternBank.putAll(customPatternBank);
            }

            try {
                return new GrokProcessor(processorTag, patternBank, matchPatterns, matchField, traceMatch);
            } catch (Exception e) {
                throw newConfigurationException(TYPE, processorTag, "patterns",
                    "Invalid regex pattern found in: " + matchPatterns + ". " + e.getMessage());
            }

        }
    }
}
