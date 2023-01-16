/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.ingest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.grok.Grok;
import org.elasticsearch.grok.MatcherWatchdog;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.xpack.core.ml.action.InferModelAction;
import org.elasticsearch.xpack.core.ml.inference.results.NerResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.NerConfigUpdate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.elasticsearch.ingest.ConfigurationUtils.newConfigurationException;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class RedactProcessor extends AbstractProcessor {

    public static final String TYPE = "redact";

    private static final Logger logger = LogManager.getLogger(RedactProcessor.class);

    private static final char REDACTED_START = '<';
    private static final char REDACTED_END = '>';

    private final Client client;
    private final String nerModelId;
    private final String redactField;
    private final List<String> matchPatterns;
    private final List<Grok> groks;
    private final boolean ignoreMissing;

    RedactProcessor(
        String tag,
        String description,
        Client client,
        String nerModelId,
        Map<String, String> patternBank,
        List<String> matchPatterns,
        String redactField,
        boolean ignoreMissing,
        MatcherWatchdog matcherWatchdog
    ) {
        super(tag, description);
        this.nerModelId = nerModelId;
        this.redactField = redactField;
        this.client = client;
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
    public void execute(IngestDocument ingestDocument, BiConsumer<IngestDocument, Exception> handler) {
        // Call with ignoreMissing = true so getFieldValue does not throw
        final String fieldValue = ingestDocument.getFieldValue(redactField, String.class, true);

        if (fieldValue == null && ignoreMissing) {
            handler.accept(ingestDocument, null);
            return;
        } else if (fieldValue == null) {
            handler.accept(ingestDocument, new IllegalArgumentException("field [" + redactField + "] is null or missing"));
            return;
        }

        if (nerModelId == null) {
            try {
                String redacted = redactGroks(fieldValue, groks);
                ingestDocument.setFieldValue(redactField, redacted);
                handler.accept(ingestDocument, null);
            } catch (RuntimeException e) {
                // grok throws a RuntimeException when the watchdog interrupts the match
                handleGrokTimeout(ingestDocument, handler, e);
            }
            return;
        }

        executeAsyncWithOrigin(
            client,
            ML_ORIGIN,
            InferModelAction.INSTANCE,
            this.buildRequest(nerModelId, fieldValue),
            ActionListener.wrap(r -> {
                if (r.getInferenceResults().isEmpty()) {
                    // If the model ID is for a Java classification or regression model
                    // not a PyTorch model then 0 results may be returned as those models
                    // expect a document input not a single text field
                    handler.accept(
                        ingestDocument,
                        new IllegalStateException(
                            "Inference against model ["
                                + nerModelId
                                + "] returned 0 results. "
                                + "Is the model  a Named Entity Recognition model?"
                        )
                    );
                    return;
                }
                if (r.getInferenceResults().get(0)instanceof NerResults nerResult) {
                    String redactedEnts = redactEntities(fieldValue, nerResult.getEntityGroups());
                    try {
                        String redacted = redactGroks(redactedEnts, groks);
                        ingestDocument.setFieldValue(redactField, redacted);
                        handler.accept(ingestDocument, null);
                    } catch (RuntimeException e) {
                        // grok throws a RuntimeException when the watchdog interrupts the match
                        handleGrokTimeout(ingestDocument, handler, e);
                    }
                } else {
                    handler.accept(
                        ingestDocument,
                        new IllegalStateException(
                            "Unexpected result type ["
                                + r.getInferenceResults().get(0).getWriteableName()
                                + "]. Is the model ["
                                + nerModelId
                                + "] a Named Entity Recognition model?"
                        )
                    );
                }
            }, e -> handler.accept(ingestDocument, e))
        );
    }

    @Override
    public boolean isAsync() {
        return true;
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

    private InferModelAction.Request buildRequest(String modelId, String fieldValue) {
        return InferModelAction.Request.forTextInput(modelId, NerConfigUpdate.EMPTY_NER_CONFIG_UPDATE, List.of(fieldValue));
    }

    private void handleGrokTimeout(
        IngestDocument ingestDocument,
        BiConsumer<IngestDocument, Exception> handler,
        RuntimeException grokException
    ) {
        handler.accept(ingestDocument, new ElasticsearchTimeoutException("", grokException));
    }

    static String redactEntities(String field, List<NerResults.EntityGroup> entities) {
        for (var ent : entities) {
            field = field.replaceAll(ent.getEntity(), REDACTED_START + ent.getClassName() + REDACTED_END);
        }
        return field;
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
        private final Client client;

        public Factory(Client client, MatcherWatchdog matcherWatchdog) {
            this.client = client;
            this.matcherWatchdog = matcherWatchdog;
        }

        @Override
        public RedactProcessor create(
            Map<String, Processor.Factory> registry,
            String processorTag,
            String description,
            Map<String, Object> config
        ) throws Exception {
            String modelId = ConfigurationUtils.readOptionalStringProperty(TYPE, processorTag, config, "model_id");
            String matchField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "field");
            List<String> matchPatterns = ConfigurationUtils.readOptionalList(TYPE, processorTag, config, "patterns");
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "ignore_missing", true);

            if (matchPatterns == null) {
                matchPatterns = List.of();
            }
            if (matchPatterns.isEmpty() && modelId == null) {
                throw newConfigurationException(
                    TYPE,
                    processorTag,
                    "patterns",
                    "If [model_id] is not set the " + "list of patterns must not be empty"
                );
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
                    client,
                    modelId,
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
