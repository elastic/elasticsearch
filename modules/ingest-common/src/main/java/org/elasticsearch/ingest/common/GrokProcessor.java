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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.grok.Grok;
import org.elasticsearch.grok.MatcherWatchdog;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.ingest.ConfigurationUtils.newConfigurationException;

public final class GrokProcessor extends AbstractProcessor {

    public static final String TYPE = "grok";
    private static final String PATTERN_MATCH_KEY = "_ingest._grok_match_index";
    private static final Logger logger = LogManager.getLogger(GrokProcessor.class);

    private final String matchField;
    private final List<String> matchPatterns;
    private final Grok grok;
    private final boolean traceMatch;
    private final boolean ignoreMissing;

    GrokProcessor(String tag, String description, Map<String, String> patternBank, List<String> matchPatterns, String matchField,
                  boolean traceMatch, boolean ignoreMissing, MatcherWatchdog matcherWatchdog) {
        super(tag, description);
        this.matchField = matchField;
        this.matchPatterns = matchPatterns;
        this.grok = new Grok(patternBank, combinePatterns(matchPatterns, traceMatch), matcherWatchdog, logger::debug);
        this.traceMatch = traceMatch;
        this.ignoreMissing = ignoreMissing;
        // Joni warnings are only emitted on an attempt to match, and the warning emitted for every call to match which is too verbose
        // so here we emit a warning (if there is one) to the logfile at warn level on construction / processor creation.
        new Grok(patternBank, combinePatterns(matchPatterns, traceMatch), matcherWatchdog, logger::warn).match("___nomatch___");
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        String fieldValue = ingestDocument.getFieldValue(matchField, String.class, ignoreMissing);

        if (fieldValue == null && ignoreMissing) {
            return ingestDocument;
        } else if (fieldValue == null) {
            throw new IllegalArgumentException("field [" + matchField + "] is null, cannot process it.");
        }

        Map<String, Object> matches = grok.captures(fieldValue);
        if (matches == null) {
            throw new IllegalArgumentException("Provided Grok expressions do not match field value: [" + fieldValue + "]");
        }

        matches.forEach(ingestDocument::setFieldValue);

        if (traceMatch) {
            if (matchPatterns.size() > 1) {
                @SuppressWarnings("unchecked")
                HashMap<String, String> matchMap = (HashMap<String, String>) ingestDocument.getFieldValue(PATTERN_MATCH_KEY, Object.class);
                matchMap.keySet().stream().findFirst().ifPresent((index) -> {
                    ingestDocument.setFieldValue(PATTERN_MATCH_KEY, index);
                });
            } else {
                ingestDocument.setFieldValue(PATTERN_MATCH_KEY, "0");
            }
        }
        return ingestDocument;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    Grok getGrok() {
        return grok;
    }

    boolean isIgnoreMissing() {
        return ignoreMissing;
    }

    String getMatchField() {
        return matchField;
    }

    List<String> getMatchPatterns() {
        return matchPatterns;
    }

    static String combinePatterns(List<String> patterns, boolean traceMatch) {
        String combinedPattern;
        if (patterns.size() > 1) {
            combinedPattern = "";
            for (int i = 0; i < patterns.size(); i++) {
                String pattern = patterns.get(i);
                String valueWrap;
                if (traceMatch) {
                    valueWrap = "(?<" + PATTERN_MATCH_KEY + "." + i + ">" + pattern + ")";
                } else {
                    valueWrap = "(?:" + patterns.get(i) + ")";
                }
                if (combinedPattern.equals("")) {
                    combinedPattern = valueWrap;
                } else {
                    combinedPattern = combinedPattern + "|" + valueWrap;
                }
            }
        }  else {
            combinedPattern = patterns.get(0);
        }

        return combinedPattern;
    }

    public static final class Factory implements Processor.Factory {

        private final Map<String, String> builtinPatterns;
        private final MatcherWatchdog matcherWatchdog;

        public Factory(Map<String, String> builtinPatterns, MatcherWatchdog matcherWatchdog) {
            this.builtinPatterns = builtinPatterns;
            this.matcherWatchdog = matcherWatchdog;
        }

        @Override
        public GrokProcessor create(Map<String, Processor.Factory> registry, String processorTag,
                                    String description, Map<String, Object> config) throws Exception {
            String matchField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "field");
            List<String> matchPatterns = ConfigurationUtils.readList(TYPE, processorTag, config, "patterns");
            boolean traceMatch = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "trace_match", false);
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "ignore_missing", false);

            if (matchPatterns.isEmpty()) {
                throw newConfigurationException(TYPE, processorTag, "patterns", "List of patterns must not be empty");
            }
            Map<String, String> customPatternBank = ConfigurationUtils.readOptionalMap(TYPE, processorTag, config, "pattern_definitions");
            Map<String, String> patternBank = new HashMap<>(builtinPatterns);
            if (customPatternBank != null) {
                patternBank.putAll(customPatternBank);
            }

            try {
                return new GrokProcessor(processorTag, description, patternBank, matchPatterns, matchField, traceMatch, ignoreMissing,
                    matcherWatchdog);
            } catch (Exception e) {
                throw newConfigurationException(TYPE, processorTag, "patterns",
                    "Invalid regex pattern found in: " + matchPatterns + ". " + e.getMessage());
            }

        }
    }
}
