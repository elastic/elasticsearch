/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.ingest.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.grok.Grok;
import org.elasticsearch.grok.MatcherWatchdog;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.ingest.ConfigurationUtils.newConfigurationException;

public class RedactProcessor extends AbstractProcessor {

    public static final String TYPE = "redact";

    private static final Logger logger = LogManager.getLogger(RedactProcessor.class);

    private static final char REDACTED_START = '<';
    private static final char REDACTED_END = '>';

    private final String redactField;
    private final List<String> matchPatterns;
    private final List<Grok> groks;
    private final boolean ignoreMissing;

    RedactProcessor(
        String tag,
        String description,
        Map<String, String> patternBank,
        List<String> matchPatterns,
        String redactField,
        boolean ignoreMissing,
        MatcherWatchdog matcherWatchdog
    ) {
        super(tag, description);
        this.redactField = redactField;
        this.matchPatterns = matchPatterns;
        this.groks = new ArrayList<>(matchPatterns.size());
        for (var matchPattern : matchPatterns) {
            this.groks.add(new Grok(patternBank, matchPattern, matcherWatchdog, logger::debug));
        }
        this.ignoreMissing = ignoreMissing;
        // Joni warnings are only emitted on an attempt to match, and the warning emitted for every call to match which is too verbose
        // so here we emit a warning (if there is one) to the logfile at warn level on construction / processor creation.
        if (matchPatterns.isEmpty() == false) {
            new Grok(patternBank, matchPatterns.get(0), matcherWatchdog, logger::warn).match("___nomatch___");
        }
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) {
        // Call with ignoreMissing = true so getFieldValue does not throw
        final String fieldValue = ingestDocument.getFieldValue(redactField, String.class, true);

        if (fieldValue == null && ignoreMissing) {
            return ingestDocument;
        } else if (fieldValue == null) {
            throw new IllegalArgumentException("field [" + redactField + "] is null or missing");
        }

        try {
            String redacted = redactGroks(fieldValue, groks);
            ingestDocument.setFieldValue(redactField, redacted);
            return ingestDocument;
        } catch (RuntimeException e) {
            // grok throws a RuntimeException when the watchdog interrupts the match
            throw new ElasticsearchTimeoutException("Grok pattern matching timed out", e);
        }
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public List<String> getMatchPatterns() {
        return matchPatterns;
    }

    public List<Grok> getGroks() {
        return groks;
    }

    static String redactGroks(String fieldValue, List<Grok> groks) {
        for (var grok : groks) {
            Map<String, Object> matches = grok.captures(fieldValue);
            if (matches != null) {
                for (var entry : matches.entrySet()) {
                    fieldValue = fieldValue.replace((String) entry.getValue(), '<' + entry.getKey() + '>');
                }
            }
        }
        return fieldValue;
    }

    public static final class Factory implements Processor.Factory {

        private final MatcherWatchdog matcherWatchdog;

        public Factory(MatcherWatchdog matcherWatchdog) {
            this.matcherWatchdog = matcherWatchdog;
        }

        @Override
        public RedactProcessor create(
            Map<String, Processor.Factory> registry,
            String processorTag,
            String description,
            Map<String, Object> config
        ) throws Exception {
            String matchField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "field");
            List<String> matchPatterns = ConfigurationUtils.readList(TYPE, processorTag, config, "patterns");
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "ignore_missing", true);

            if (matchPatterns == null || matchPatterns.isEmpty()) {
                throw newConfigurationException(TYPE, processorTag, "patterns", "List of patterns must not be empty");
            }
            Map<String, String> customPatternBank = ConfigurationUtils.readOptionalMap(TYPE, processorTag, config, "pattern_definitions");
            Map<String, String> patternBank = new HashMap<>(Grok.getBuiltinPatterns(true));
            if (customPatternBank != null) {
                patternBank.putAll(customPatternBank);
            }

            try {
                return new RedactProcessor(
                    processorTag,
                    description,
                    patternBank,
                    matchPatterns,
                    matchField,
                    ignoreMissing,
                    matcherWatchdog
                );
            } catch (Exception e) {
                throw newConfigurationException(
                    TYPE,
                    processorTag,
                    "patterns",
                    "Invalid regex pattern found in: " + matchPatterns + ". " + e.getMessage()
                );
            }
        }
    }
}
