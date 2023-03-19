/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.common.Strings;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.TemplateScript;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static org.elasticsearch.ingest.ConfigurationUtils.newConfigurationException;

public final class RerouteProcessor extends AbstractProcessor {
    public static final String TYPE = "reroute";

    private static final String DATA_STREAM_PREFIX = "data_stream.";
    private static final String DATA_STREAM_TYPE = DATA_STREAM_PREFIX + "type";
    private static final String DATA_STREAM_DATASET = DATA_STREAM_PREFIX + "dataset";
    private static final String DATA_STREAM_NAMESPACE = DATA_STREAM_PREFIX + "namespace";
    private static final char[] DISALLOWED_IN_DATASET = new char[] { '\\', '/', '*', '?', '\"', '<', '>', '|', ' ', ',', '#', ':', '-' };
    private static final char[] DISALLOWED_IN_NAMESPACE = new char[] { '\\', '/', '*', '?', '\"', '<', '>', '|', ' ', ',', '#', ':' };
    private static final int MAX_LENGTH = 100;
    private static final char REPLACEMENT_CHAR = '_';
    private final List<TemplateScript.Factory> dataset;
    private final List<TemplateScript.Factory> namespace;
    private final TemplateScript.Factory destination;
    private final boolean skipIfTargetUnchanged;

    RerouteProcessor(List<TemplateScript.Factory> dataset, List<TemplateScript.Factory> namespace) {
        this(null, null, dataset, namespace, null, false);
    }

    RerouteProcessor(TemplateScript.Factory destination) {
        this(null, null, null, null, destination, false);
    }

    RerouteProcessor(
        String tag,
        String description,
        List<TemplateScript.Factory> dataset,
        List<TemplateScript.Factory> namespace,
        TemplateScript.Factory destination,
        boolean skipIfTargetUnchanged
    ) {
        super(tag, description);
        this.dataset = dataset;
        this.namespace = namespace;
        this.destination = destination;
        this.skipIfTargetUnchanged = skipIfTargetUnchanged;
    }

    private static String sanitizeDataStreamField(String s, char[] disallowedInDataset) {
        if (s == null) {
            return null;
        }
        s = s.toLowerCase(Locale.ROOT);
        s = s.substring(0, Math.min(s.length(), MAX_LENGTH));
        for (char c : disallowedInDataset) {
            s = s.replace(c, REPLACEMENT_CHAR);
        }
        return s;
    }

    private static String sanitizeDataset(String dataset) {
        return sanitizeDataStreamField(dataset, DISALLOWED_IN_DATASET);
    }

    private static String sanitizeNamespace(String namespace) {
        return sanitizeDataStreamField(namespace, DISALLOWED_IN_NAMESPACE);
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        if (destination != null) {
            ingestDocument.reroute(ingestDocument.renderTemplate(destination));
            return ingestDocument;
        }
        final String currentTarget = ingestDocument.getFieldValue(IngestDocument.Metadata.INDEX.getFieldName(), String.class);
        final String type;
        final String currentDataset;
        final String currentNamespace;
        int indexOfFirstDash = currentTarget.indexOf('-');
        if (indexOfFirstDash < 0) {
            throw createInvalidDataStreamNameException(currentTarget);
        }
        int indexOfSecondDash = currentTarget.indexOf('-', indexOfFirstDash + 1);
        if (indexOfSecondDash < 0) {
            throw createInvalidDataStreamNameException(currentTarget);
        }
        type = parseDataStreamType(currentTarget, indexOfFirstDash);
        currentDataset = parseDataStreamDataset(currentTarget, indexOfFirstDash, indexOfSecondDash);
        currentNamespace = parseDataStreamNamespace(currentTarget, indexOfSecondDash);

        String dataset = determineDataset(ingestDocument, currentDataset);
        String namespace = determineNamespace(ingestDocument, currentNamespace);
        if (dataset == null || namespace == null) {
            return ingestDocument;
        }
        String newTarget = type + "-" + dataset + "-" + namespace;
        if (newTarget.equals(currentTarget) && skipIfTargetUnchanged) {
            return ingestDocument;
        }
        ingestDocument.reroute(newTarget);
        ingestDocument.setFieldValue(DATA_STREAM_TYPE, type);
        ingestDocument.setFieldValue(DATA_STREAM_DATASET, dataset);
        ingestDocument.setFieldValue(DATA_STREAM_NAMESPACE, namespace);
        return ingestDocument;
    }

    private static IllegalArgumentException createInvalidDataStreamNameException(String indexName) {
        return new IllegalArgumentException(
            "invalid data stream name: [" + indexName + "]; must follow naming scheme <type>-<dataset>-<namespace>"
        );
    }

    private static String parseDataStreamType(String dataStreamName, int indexOfFirstDash) {
        return dataStreamName.substring(0, indexOfFirstDash);
    }

    private static String parseDataStreamDataset(String dataStreamName, int indexOfFirstDash, int indexOfSecondDash) {
        return dataStreamName.substring(indexOfFirstDash + 1, indexOfSecondDash);
    }

    private static String parseDataStreamNamespace(String dataStreamName, int indexOfSecondDash) {
        return dataStreamName.substring(indexOfSecondDash + 1);
    }

    private String determineDataset(IngestDocument ingestDocument, String currentDataset) {
        return determineDataStreamField(ingestDocument, dataset, currentDataset, RerouteProcessor::sanitizeDataset, DATA_STREAM_DATASET);
    }

    private String determineNamespace(IngestDocument ingestDocument, String currentNamespace) {
        return determineDataStreamField(
            ingestDocument,
            namespace,
            currentNamespace,
            RerouteProcessor::sanitizeNamespace,
            DATA_STREAM_NAMESPACE
        );
    }

    private String determineDataStreamField(
        IngestDocument ingestDocument,
        List<TemplateScript.Factory> valueSources,
        String fromCurrentTarget,
        Function<String, String> sanitization,
        String dataStreamFieldName
    ) {
        String result = "";
        for (TemplateScript.Factory value : valueSources) {
            result = ingestDocument.renderTemplate(value);
            if (Strings.isNullOrEmpty(result) == false) {
                break;
            }
        }
        if (Strings.isNullOrEmpty(result)) {
            result = ingestDocument.getFieldValue(dataStreamFieldName, String.class, true);
        }
        if (Strings.isNullOrEmpty(result)) {
            result = fromCurrentTarget;
        }
        return sanitization.apply(result);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public List<TemplateScript.Factory> getDataStreamDataset() {
        return dataset;
    }

    public List<TemplateScript.Factory> getDataStreamNamespace() {
        return namespace;
    }

    public TemplateScript.Factory getDestination() {
        return destination;
    }

    public static final class Factory implements Processor.Factory {

        private final ScriptService scriptService;

        public Factory(ScriptService scriptService) {
            this.scriptService = scriptService;
        }

        @Override
        public RerouteProcessor create(
            Map<String, Processor.Factory> processorFactories,
            String tag,
            String description,
            Map<String, Object> config
        ) throws Exception {
            List<String> dataset = ConfigurationUtils.readOptionalListOrString(TYPE, tag, config, "dataset");
            validateDataStreamValue(tag, dataset, "dataset", RerouteProcessor::sanitizeDataset);
            List<TemplateScript.Factory> datasetTemplate = dataset.stream()
                .map(ds -> ConfigurationUtils.compileTemplate(TYPE, tag, "dataset", ds, scriptService))
                .toList();
            List<String> namespace = ConfigurationUtils.readOptionalListOrString(TYPE, tag, config, "namespace");
            validateDataStreamValue(tag, namespace, "namespace", RerouteProcessor::sanitizeNamespace);
            List<TemplateScript.Factory> namespaceTemplate = namespace.stream()
                .map(nsp -> ConfigurationUtils.compileTemplate(TYPE, tag, "namespace", nsp, scriptService))
                .toList();
            String destination = ConfigurationUtils.readOptionalStringProperty(TYPE, tag, config, "destination");
            TemplateScript.Factory destinationTemplate = null;
            if (destination != null) {
                destinationTemplate = ConfigurationUtils.compileTemplate(
                    TYPE,
                    tag,
                    "destination",
                    destination,
                    scriptService
                );
            }

            if (destination != null && (dataset.isEmpty() == false || namespace.isEmpty() == false)) {
                throw newConfigurationException(TYPE, tag, "destination", "can only be set if dataset and namespace are not set");
            }
            boolean skipIfTargetUnchanged = ConfigurationUtils.readBooleanProperty(TYPE, tag, config, "skip_if_target_unchanged", false);

            return new RerouteProcessor(tag, description, datasetTemplate, namespaceTemplate, destinationTemplate, skipIfTargetUnchanged);
        }

        private static void validateDataStreamValue(
            String tag,
            List<String> dataset,
            String dataStreamComponent,
            Function<String, String> sanitizer
        ) {
            dataset.stream()
                .filter(ds -> ds.contains("{{") == false)
                .filter(ds -> Objects.equals(sanitizer.apply(ds), ds) == false)
                .findAny()
                .ifPresent(
                    ds -> {
                        throw newConfigurationException(TYPE, tag, dataStreamComponent, "'" + ds + "' contains disallowed characters");
                    }
                );
        }
    }
}
